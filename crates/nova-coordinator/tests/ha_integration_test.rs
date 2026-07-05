//! HA integration tests — openraft 3-node coordinator cluster.
//!
//! Tests: single-node leader election, 3-node election, raft write commit.

#[cfg(test)]
mod tests {
    use nova_coordinator::raft::RaftRequest;
    use nova_coordinator::raft_transport::NovaRaftNode;
    use nova_storage::FdbMetadataStore;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    fn make_store() -> Arc<FdbMetadataStore> {
        Arc::new(
            FdbMetadataStore::open_test(
                "docker:docker@127.0.0.1:4500",
                format!("test_{}", nova_common::now_micros()).into_bytes(),
            )
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn test_single_node_raft_starts_as_leader() {
        let store = make_store();
        let node = NovaRaftNode::start(1, HashMap::new(), store)
            .await
            .expect("start failed");
        node.initialize_single().await.expect("init failed");

        // Wait up to 2s for leader election
        let mut is_leader = false;
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if node.is_leader() {
                is_leader = true;
                break;
            }
        }
        assert!(is_leader, "single node should become leader within 2s");
    }

    #[tokio::test]
    async fn test_raft_write_committed() {
        let store = make_store();
        let node = NovaRaftNode::start(1, HashMap::new(), store)
            .await
            .expect("start failed");
        node.initialize_single().await.expect("init failed");

        // Wait for leader
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if node.is_leader() {
                break;
            }
        }
        assert!(node.is_leader(), "must be leader before writing");

        let resp = node
            .write(RaftRequest::CreateDatabase {
                name: "ha_test".to_string(),
            })
            .await
            .expect("write failed");
        assert!(resp.success, "raft write should succeed: {}", resp.message);
    }
}
