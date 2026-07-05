//! openraft 3-node HA transport layer.
//!
//! Provides:
//! - `NovaTypeConfig` — openraft TypeConfig for nova coordinator
//! - `InMemoryLogStore` — RaftLogStorage backed by BTreeMap (dev/test)
//! - `InMemoryStateMachine` — RaftStateMachine that delegates to CoordinatorStateMachine
//! - `NovaRaftNetworkFactory` — RaftNetworkFactory that creates tonic gRPC connections
//! - `NovaRaftNetworkConnection` — per-target RaftNetwork impl over tonic
//! - `RaftServiceServerImpl` — tonic gRPC server for RaftService (coordinator-to-coordinator)
//! - `NovaRaftNode` — high-level wrapper: start(), write(), is_leader()
//!
//! ponytail: InMemoryStateMachine uses minimal snapshots; persist snapshots when recovery tests need it.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;

use openraft::RaftNetwork;
use openraft::network::RPCOption;
use openraft::storage::RaftLogStorage;
use openraft::storage::RaftStateMachine;
use openraft::{
    BasicNode, Config, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, LogState,
    RaftLogReader, RaftNetworkFactory, RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError,
    StoredMembership, TokioRuntime, Vote,
};
use tokio::sync::Mutex;
use tonic::transport::Channel;

use crate::raft::{RaftRequest, RaftResponse};

// ---------------------------------------------------------------------------
// TypeConfig
// ---------------------------------------------------------------------------

openraft::declare_raft_types!(
    /// Nova coordinator Raft type configuration.
    pub NovaTypeConfig:
        NodeId = u64,
        Node = BasicNode,
        D = RaftRequest,
        R = RaftResponse,
        Entry = Entry<NovaTypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = TokioRuntime,
);

// ---------------------------------------------------------------------------
// gRPC stubs (from build.rs generated code)
// ---------------------------------------------------------------------------

tonic::include_proto!("nova.rpc");
use raft_service_client::RaftServiceClient;
use raft_service_server::RaftService;

// ---------------------------------------------------------------------------
// InMemoryLogStore
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct InMemoryLogStore {
    inner: Arc<Mutex<LogStoreInner>>,
}

struct LogStoreInner {
    vote: Option<Vote<u64>>,
    log: BTreeMap<u64, Entry<NovaTypeConfig>>,
    committed: Option<LogId<u64>>,
    purged: Option<LogId<u64>>,
}

impl Default for InMemoryLogStore {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(LogStoreInner {
                vote: None,
                log: BTreeMap::new(),
                committed: None,
                purged: None,
            })),
        }
    }
}

impl RaftLogReader<NovaTypeConfig> for InMemoryLogStore {
    async fn try_get_log_entries<
        RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + Send,
    >(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<NovaTypeConfig>>, StorageError<u64>> {
        let inner = self.inner.lock().await;
        Ok(inner.log.range(range).map(|(_, v)| v.clone()).collect())
    }
}

impl RaftLogStorage<NovaTypeConfig> for InMemoryLogStore {
    type LogReader = InMemoryLogStore;

    async fn get_log_state(&mut self) -> Result<LogState<NovaTypeConfig>, StorageError<u64>> {
        let inner = self.inner.lock().await;
        let last = inner.log.values().last().map(|e| e.log_id);
        Ok(LogState {
            last_purged_log_id: inner.purged,
            last_log_id: last,
        })
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        self.inner.lock().await.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        Ok(self.inner.lock().await.committed)
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        self.inner.lock().await.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.inner.lock().await.vote)
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: openraft::storage::LogFlushed<NovaTypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<NovaTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut inner = self.inner.lock().await;
        for e in entries {
            inner.log.insert(e.log_id.index, e);
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.lock().await;
        inner.log.retain(|&idx, _| idx < log_id.index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.lock().await;
        inner.log.retain(|&idx, _| idx > log_id.index);
        inner.purged = Some(log_id);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// FdbLogStore (durable Raft log/vote/commit state)
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct FdbLogStore {
    db: Arc<foundationdb::Database>,
    _network: Arc<foundationdb::api::NetworkAutoStop>,
}

impl FdbLogStore {
    #[allow(clippy::result_large_err)]
    pub fn open(cluster_file: &str) -> Result<Self, StorageError<u64>> {
        let network = unsafe { foundationdb::boot() };
        let db = foundationdb::Database::new(Some(cluster_file)).map_err(Self::write_err)?;
        Ok(Self {
            db: Arc::new(db),
            _network: Arc::new(network),
        })
    }

    fn log_key(index: u64) -> Vec<u8> {
        let mut key = b"nova/raft/log/".to_vec();
        key.extend_from_slice(&index.to_be_bytes());
        key
    }

    fn read_err(e: impl std::error::Error + Send + Sync + 'static) -> StorageError<u64> {
        StorageError::from_io_error(
            ErrorSubject::Store,
            ErrorVerb::Read,
            std::io::Error::other(e),
        )
    }

    fn write_err(e: impl std::error::Error + Send + Sync + 'static) -> StorageError<u64> {
        StorageError::from_io_error(
            ErrorSubject::Store,
            ErrorVerb::Write,
            std::io::Error::other(e),
        )
    }

    #[allow(clippy::result_large_err)]
    fn de<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, StorageError<u64>> {
        serde_json::from_slice(bytes).map_err(Self::read_err)
    }

    #[allow(clippy::result_large_err)]
    fn ser<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, StorageError<u64>> {
        serde_json::to_vec(value).map_err(Self::write_err)
    }

    async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, StorageError<u64>> {
        self.db
            .run(|trx, _| {
                let key = key.clone();
                async move {
                    trx.get(&key, false)
                        .await
                        .map(|v| v.map(|s| s.to_vec()))
                        .map_err(foundationdb::FdbBindingError::from)
                }
            })
            .await
            .map_err(Self::read_err)
    }

    async fn set(&self, key: Vec<u8>, val: Vec<u8>) -> Result<(), StorageError<u64>> {
        self.db
            .run(|trx, _| {
                let key = key.clone();
                let val = val.clone();
                async move {
                    trx.set(&key, &val);
                    Ok::<_, foundationdb::FdbBindingError>(())
                }
            })
            .await
            .map_err(Self::write_err)
    }

    async fn clear_range(&self, start: Vec<u8>, end: Vec<u8>) -> Result<(), StorageError<u64>> {
        self.db
            .run(|trx, _| {
                let start = start.clone();
                let end = end.clone();
                async move {
                    trx.clear_range(&start, &end);
                    Ok::<_, foundationdb::FdbBindingError>(())
                }
            })
            .await
            .map_err(Self::write_err)
    }
}

impl RaftLogReader<NovaTypeConfig> for FdbLogStore {
    async fn try_get_log_entries<
        RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + Send,
    >(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<NovaTypeConfig>>, StorageError<u64>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(v) => *v,
            std::ops::Bound::Excluded(v) => v.saturating_add(1),
            std::ops::Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(v) => v.saturating_add(1),
            std::ops::Bound::Excluded(v) => *v,
            std::ops::Bound::Unbounded => u64::MAX,
        };
        let begin = Self::log_key(start);
        let finish = Self::log_key(end);
        let vals = self
            .db
            .run(|trx, _| {
                let begin = begin.clone();
                let finish = finish.clone();
                async move {
                    let opt =
                        foundationdb::RangeOption::from((begin.as_slice(), finish.as_slice()));
                    trx.get_range(&opt, 1_000, false)
                        .await
                        .map(|v| v.iter().map(|kv| kv.value().to_vec()).collect::<Vec<_>>())
                        .map_err(foundationdb::FdbBindingError::from)
                }
            })
            .await
            .map_err(Self::read_err)?;
        vals.iter().map(|v| Self::de(v)).collect()
    }
}

impl RaftLogStorage<NovaTypeConfig> for FdbLogStore {
    type LogReader = FdbLogStore;

    async fn get_log_state(&mut self) -> Result<LogState<NovaTypeConfig>, StorageError<u64>> {
        let entries = self.try_get_log_entries(..).await?;
        let last = entries.last().map(|e| e.log_id);
        let purged = self
            .get(b"nova/raft/purged".to_vec())
            .await?
            .map(|v| Self::de(&v))
            .transpose()?;
        Ok(LogState {
            last_purged_log_id: purged,
            last_log_id: last,
        })
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        self.set(b"nova/raft/committed".to_vec(), Self::ser(&committed)?)
            .await
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        self.get(b"nova/raft/committed".to_vec())
            .await?
            .map(|v| Self::de(&v))
            .transpose()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        self.set(b"nova/raft/vote".to_vec(), Self::ser(vote)?).await
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        self.get(b"nova/raft/vote".to_vec())
            .await?
            .map(|v| Self::de(&v))
            .transpose()
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: openraft::storage::LogFlushed<NovaTypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<NovaTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        for e in entries {
            self.set(Self::log_key(e.log_id.index), Self::ser(&e)?)
                .await?;
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let start = Self::log_key(log_id.index);
        let end = Self::log_key(u64::MAX);
        self.clear_range(start, end).await
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.clear_range(
            Self::log_key(0),
            Self::log_key(log_id.index.saturating_add(1)),
        )
        .await?;
        self.set(b"nova/raft/purged".to_vec(), Self::ser(&log_id)?)
            .await
    }
}

// ---------------------------------------------------------------------------
// InMemoryStateMachine
// ---------------------------------------------------------------------------

pub struct InMemoryStateMachine {
    store: Arc<nova_storage::FdbMetadataStore>,
    last_applied: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
}

impl InMemoryStateMachine {
    pub fn new(store: Arc<nova_storage::FdbMetadataStore>) -> Self {
        Self {
            store,
            last_applied: None,
            last_membership: StoredMembership::default(),
        }
    }
}

/// Minimal snapshot builder (in-memory, just returns empty snapshot).
pub struct NovaSnapshotBuilder {
    last_applied: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
}

impl RaftSnapshotBuilder<NovaTypeConfig> for NovaSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<NovaTypeConfig>, StorageError<u64>> {
        let meta = SnapshotMeta {
            last_log_id: self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot_id: format!(
                "snap-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
            ),
        };
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(Vec::new())),
        })
    }
}

impl RaftStateMachine<NovaTypeConfig> for InMemoryStateMachine {
    type SnapshotBuilder = NovaSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        Ok((self.last_applied, self.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<RaftResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<NovaTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let sm = crate::raft::CoordinatorStateMachine::new(self.store.clone());
        let mut results = Vec::new();
        for entry in entries {
            self.last_applied = Some(entry.log_id);
            match &entry.payload {
                EntryPayload::Blank => results.push(RaftResponse::default()),
                EntryPayload::Normal(req) => {
                    let resp = sm.apply(req).await;
                    results.push(resp);
                }
                EntryPayload::Membership(m) => {
                    self.last_membership = StoredMembership::new(Some(entry.log_id), m.clone());
                    results.push(RaftResponse::default());
                }
            }
        }
        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        NovaSnapshotBuilder {
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        _snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        self.last_applied = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<NovaTypeConfig>>, StorageError<u64>> {
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// Network: connection per target
// ---------------------------------------------------------------------------

pub struct NovaRaftNetworkConnection {
    target: u64,
    addr: String,
}

impl NovaRaftNetworkConnection {
    async fn client(&self) -> Result<RaftServiceClient<Channel>, openraft::error::Unreachable> {
        RaftServiceClient::connect(self.addr.clone())
            .await
            .map_err(|e| openraft::error::Unreachable::new(&e))
    }
}

impl RaftNetwork<NovaTypeConfig> for NovaRaftNetworkConnection {
    async fn append_entries(
        &mut self,
        rpc: openraft::raft::AppendEntriesRequest<NovaTypeConfig>,
        _opt: RPCOption,
    ) -> Result<
        openraft::raft::AppendEntriesResponse<u64>,
        openraft::error::RPCError<u64, BasicNode, openraft::error::RaftError<u64>>,
    > {
        let payload = serde_json::to_vec(&rpc).map_err(|e| {
            openraft::error::RPCError::Unreachable(openraft::error::Unreachable::new(&e))
        })?;
        let req = RaftAppendEntriesRequest {
            target_node_id: self.target,
            payload,
        };
        let resp = self
            .client()
            .await?
            .append_entries(req)
            .await
            .map_err(|e| {
                openraft::error::RPCError::Unreachable(openraft::error::Unreachable::new(&e))
            })?
            .into_inner();
        if !resp.error.is_empty() {
            return Err(openraft::error::RPCError::Unreachable(
                openraft::error::Unreachable::new(&std::io::Error::other(resp.error)),
            ));
        }
        serde_json::from_slice(&resp.payload).map_err(|e| {
            openraft::error::RPCError::Unreachable(openraft::error::Unreachable::new(&e))
        })
    }

    async fn vote(
        &mut self,
        rpc: openraft::raft::VoteRequest<u64>,
        _opt: RPCOption,
    ) -> Result<
        openraft::raft::VoteResponse<u64>,
        openraft::error::RPCError<u64, BasicNode, openraft::error::RaftError<u64>>,
    > {
        let payload = serde_json::to_vec(&rpc).map_err(|e| {
            openraft::error::RPCError::Unreachable(openraft::error::Unreachable::new(&e))
        })?;
        let req = RaftVoteRequest {
            target_node_id: self.target,
            payload,
        };
        let resp = self
            .client()
            .await?
            .vote(req)
            .await
            .map_err(|e| {
                openraft::error::RPCError::Unreachable(openraft::error::Unreachable::new(&e))
            })?
            .into_inner();
        if !resp.error.is_empty() {
            return Err(openraft::error::RPCError::Unreachable(
                openraft::error::Unreachable::new(&std::io::Error::other(resp.error)),
            ));
        }
        serde_json::from_slice(&resp.payload).map_err(|e| {
            openraft::error::RPCError::Unreachable(openraft::error::Unreachable::new(&e))
        })
    }

    async fn install_snapshot(
        &mut self,
        rpc: openraft::raft::InstallSnapshotRequest<NovaTypeConfig>,
        _opt: RPCOption,
    ) -> Result<
        openraft::raft::InstallSnapshotResponse<u64>,
        openraft::error::RPCError<
            u64,
            BasicNode,
            openraft::error::RaftError<u64, openraft::error::InstallSnapshotError>,
        >,
    > {
        let payload = serde_json::to_vec(&rpc).map_err(|e| {
            openraft::error::RPCError::Unreachable(openraft::error::Unreachable::new(&e))
        })?;
        let req = RaftSnapshotRequest {
            target_node_id: self.target,
            payload,
        };
        let resp = self
            .client()
            .await?
            .install_snapshot(req)
            .await
            .map_err(|e| {
                openraft::error::RPCError::Unreachable(openraft::error::Unreachable::new(&e))
            })?
            .into_inner();
        if !resp.error.is_empty() {
            return Err(openraft::error::RPCError::Unreachable(
                openraft::error::Unreachable::new(&std::io::Error::other(resp.error)),
            ));
        }
        serde_json::from_slice(&resp.payload).map_err(|e| {
            openraft::error::RPCError::Unreachable(openraft::error::Unreachable::new(&e))
        })
    }
}

// ---------------------------------------------------------------------------
// Network factory
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct NovaRaftNetworkFactory {
    /// node_id → gRPC address e.g. "http://127.0.0.1:50052"
    pub addrs: std::collections::HashMap<u64, String>,
}

impl RaftNetworkFactory<NovaTypeConfig> for NovaRaftNetworkFactory {
    type Network = NovaRaftNetworkConnection;

    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Self::Network {
        let addr = self
            .addrs
            .get(&target)
            .cloned()
            .unwrap_or_else(|| format!("http://127.0.0.1:{}", 50050 + target));
        NovaRaftNetworkConnection { target, addr }
    }
}

// ---------------------------------------------------------------------------
// gRPC server — RaftService
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct RaftServiceServerImpl {
    raft: openraft::Raft<NovaTypeConfig>,
}

impl RaftServiceServerImpl {
    pub fn new(raft: openraft::Raft<NovaTypeConfig>) -> Self {
        Self { raft }
    }
}

#[tonic::async_trait]
impl RaftService for RaftServiceServerImpl {
    async fn append_entries(
        &self,
        request: tonic::Request<RaftAppendEntriesRequest>,
    ) -> Result<tonic::Response<RaftAppendEntriesResponse>, tonic::Status> {
        let req_bytes = request.into_inner().payload;
        let rpc: openraft::raft::AppendEntriesRequest<NovaTypeConfig> =
            serde_json::from_slice(&req_bytes)
                .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
        match self.raft.append_entries(rpc).await {
            Ok(resp) => {
                let payload = serde_json::to_vec(&resp)
                    .map_err(|e| tonic::Status::internal(e.to_string()))?;
                Ok(tonic::Response::new(RaftAppendEntriesResponse {
                    payload,
                    error: String::new(),
                }))
            }
            Err(e) => Ok(tonic::Response::new(RaftAppendEntriesResponse {
                payload: Vec::new(),
                error: e.to_string(),
            })),
        }
    }

    async fn vote(
        &self,
        request: tonic::Request<RaftVoteRequest>,
    ) -> Result<tonic::Response<RaftVoteResponse>, tonic::Status> {
        let req_bytes = request.into_inner().payload;
        let rpc: openraft::raft::VoteRequest<u64> = serde_json::from_slice(&req_bytes)
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
        match self.raft.vote(rpc).await {
            Ok(resp) => {
                let payload = serde_json::to_vec(&resp)
                    .map_err(|e| tonic::Status::internal(e.to_string()))?;
                Ok(tonic::Response::new(RaftVoteResponse {
                    payload,
                    error: String::new(),
                }))
            }
            Err(e) => Ok(tonic::Response::new(RaftVoteResponse {
                payload: Vec::new(),
                error: e.to_string(),
            })),
        }
    }

    async fn install_snapshot(
        &self,
        request: tonic::Request<RaftSnapshotRequest>,
    ) -> Result<tonic::Response<RaftSnapshotResponse>, tonic::Status> {
        let req_bytes = request.into_inner().payload;
        let rpc: openraft::raft::InstallSnapshotRequest<NovaTypeConfig> =
            serde_json::from_slice(&req_bytes)
                .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
        match self.raft.install_snapshot(rpc).await {
            Ok(resp) => {
                let payload = serde_json::to_vec(&resp)
                    .map_err(|e| tonic::Status::internal(e.to_string()))?;
                Ok(tonic::Response::new(RaftSnapshotResponse {
                    payload,
                    error: String::new(),
                }))
            }
            Err(e) => Ok(tonic::Response::new(RaftSnapshotResponse {
                payload: Vec::new(),
                error: e.to_string(),
            })),
        }
    }
}

// ---------------------------------------------------------------------------
// NovaRaftNode — public API
// ---------------------------------------------------------------------------

pub struct NovaRaftNode {
    raft: openraft::Raft<NovaTypeConfig>,
}

impl NovaRaftNode {
    /// Start a single Raft node.
    /// `peers` maps node_id → gRPC address for other coordinators.
    pub async fn start(
        node_id: u64,
        peers: std::collections::HashMap<u64, String>,
        store: Arc<nova_storage::FdbMetadataStore>,
    ) -> Result<Self, openraft::error::Fatal<u64>> {
        Self::start_with_log_store(node_id, peers, store, InMemoryLogStore::default()).await
    }

    /// Start a node with FoundationDB-backed Raft log/vote/commit state.
    pub async fn start_durable(
        node_id: u64,
        peers: std::collections::HashMap<u64, String>,
        store: Arc<nova_storage::FdbMetadataStore>,
        cluster_file: &str,
    ) -> Result<Self, openraft::error::Fatal<u64>> {
        let log_store =
            FdbLogStore::open(cluster_file).map_err(openraft::error::Fatal::StorageError)?;
        Self::start_with_log_store(node_id, peers, store, log_store).await
    }

    async fn start_with_log_store<L>(
        node_id: u64,
        peers: std::collections::HashMap<u64, String>,
        store: Arc<nova_storage::FdbMetadataStore>,
        log_store: L,
    ) -> Result<Self, openraft::error::Fatal<u64>>
    where
        L: RaftLogStorage<NovaTypeConfig>
            + RaftLogReader<NovaTypeConfig>
            + Clone
            + Send
            + Sync
            + 'static,
    {
        let config = Arc::new(
            Config::default()
                .validate()
                .expect("invalid openraft config"),
        );
        let sm = InMemoryStateMachine::new(store);
        let network = NovaRaftNetworkFactory { addrs: peers };
        let raft = openraft::Raft::new(node_id, config, network, log_store, sm).await?;
        Ok(Self { raft })
    }

    /// Initialize a single-node cluster (call once per fresh cluster).
    pub async fn initialize_single(&self) -> Result<(), Box<dyn std::error::Error>> {
        use std::collections::BTreeMap;
        let mut members = BTreeMap::new();
        let node_id = self.raft.metrics().borrow().id;
        members.insert(node_id, BasicNode::default());
        self.raft.initialize(members).await?;
        Ok(())
    }

    /// Initialize a multi-node cluster (call on leader candidate with all member addresses).
    pub async fn initialize_cluster(
        &self,
        members: std::collections::BTreeMap<u64, BasicNode>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.raft.initialize(members).await?;
        Ok(())
    }

    /// Submit a write to the cluster (must be called on leader).
    pub async fn write(
        &self,
        req: RaftRequest,
    ) -> Result<RaftResponse, Box<dyn std::error::Error>> {
        let resp = self.raft.client_write(req).await?;
        Ok(resp.data)
    }

    /// Return current leader id (None if unknown).
    pub fn leader_id(&self) -> Option<u64> {
        self.raft.metrics().borrow().current_leader
    }

    /// True if this node is the current leader.
    pub fn is_leader(&self) -> bool {
        let binding = self.raft.metrics();
        let m = binding.borrow();
        m.current_leader == Some(m.id)
    }

    /// Return a gRPC server impl to mount on tonic.
    pub fn grpc_server(&self) -> RaftServiceServerImpl {
        RaftServiceServerImpl::new(self.raft.clone())
    }

    /// Expose inner Raft handle (e.g. for metrics/shutdown).
    pub fn raft(&self) -> &openraft::Raft<NovaTypeConfig> {
        &self.raft
    }
}
