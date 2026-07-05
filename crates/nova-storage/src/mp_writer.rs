// MpWriter — Write Arrow RecordBatches as immutable Parquet micro-partitions to S3.
//
// Flow: Arrow RecordBatch -> Parquet file -> S3 (temp path -> permanent path on commit)
// Each MP is immutable after write. Column stats (min, max, null_count) computed at write time.

use arrow::record_batch::RecordBatch;
use nova_common::types::ColumnStats;
use nova_common::{Compression, MicroPartitionMeta, MpId, Result, TableId, TxnId};
use object_store::{ObjectStore, PutPayload};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression as ParquetCompression;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::sync::Arc;

/// Writes micro-partitions as immutable Parquet files to object storage.
pub struct MpWriter {
    store: Arc<dyn ObjectStore>,
    bucket: String,
    target_size_bytes: usize,
    compression: Compression,
}

impl MpWriter {
    /// Create a new MpWriter.
    pub fn new(store: Arc<dyn ObjectStore>, bucket: String) -> Self {
        Self {
            store,
            bucket,
            target_size_bytes: 50 * 1024 * 1024,
            compression: Compression::Snappy,
        }
    }

    /// Return a clone of the object store handle used by this writer.
    pub fn store_arc(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }

    /// Return the configured object storage bucket/prefix name.
    pub fn bucket_name(&self) -> &str {
        &self.bucket
    }

    /// Set target MP size (default 50MB).
    pub fn with_target_size(mut self, size: usize) -> Self {
        self.target_size_bytes = size;
        self
    }

    /// Set compression (default Snappy).
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Write Arrow RecordBatches as an immutable Parquet micro-partition.
    pub async fn write(
        &self,
        table_id: TableId,
        mp_id: MpId,
        version: u64,
        batches: &[RecordBatch],
        txn_id: TxnId,
    ) -> Result<MicroPartitionMeta> {
        if batches.is_empty() {
            return Err(nova_common::NovaError::Internal {
                message: "cannot write empty MP".to_string(),
            });
        }

        let column_stats = compute_column_stats(batches);
        let row_count: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();

        let temp_path = format!(
            "{}/tmp/{}/mp-{}-v{}.parquet",
            self.bucket, txn_id, mp_id, version
        );
        let perm_path = format!(
            "{}/tables/{}/mp-{}-v{}.parquet",
            self.bucket, table_id, mp_id, version
        );

        let parquet_bytes = write_parquet_bytes(batches, self.compression)?;
        let byte_size = parquet_bytes.len() as u64;

        let object_path = object_store::path::Path::from(temp_path.clone());
        self.store
            .put(&object_path, parquet_bytes.into())
            .await
            .map_err(|e| nova_common::NovaError::ObjectStoreError {
                source: Box::new(e),
            })?;

        Ok(MicroPartitionMeta {
            mp_id,
            table_id,
            partition_id: None,
            version,
            s3_path: perm_path,
            s3_temp_path: Some(temp_path),
            row_count,
            byte_size,
            compression: self.compression,
            column_stats,
            commit_ts: 0,
            txn_id,
            supersedes: None,
            superseded_by: None,
            active: false,
        })
    }

    /// Move MP from temp path to permanent path (called on commit).
    pub async fn commit_mp(&self, mp: &MicroPartitionMeta) -> Result<()> {
        if let Some(ref temp_path) = mp.s3_temp_path {
            let temp = object_store::path::Path::from(temp_path.clone());
            let perm = object_store::path::Path::from(mp.s3_path.clone());

            let data = self
                .store
                .get(&temp)
                .await
                .map_err(|e| nova_common::NovaError::ObjectStoreError {
                    source: Box::new(e),
                })?
                .bytes()
                .await
                .map_err(|e| nova_common::NovaError::ObjectStoreError {
                    source: Box::new(e),
                })?;

            self.store
                .put(&perm, PutPayload::from(data))
                .await
                .map_err(|e| nova_common::NovaError::ObjectStoreError {
                    source: Box::new(e),
                })?;

            self.store.delete(&temp).await.map_err(|e| {
                nova_common::NovaError::ObjectStoreError {
                    source: Box::new(e),
                }
            })?;
        }
        Ok(())
    }

    /// Abort a write (delete temp file).
    pub async fn abort_mp(&self, mp: &MicroPartitionMeta) -> Result<()> {
        if let Some(ref temp_path) = mp.s3_temp_path {
            let temp = object_store::path::Path::from(temp_path.clone());
            let _ = self.store.delete(&temp).await;
        }
        Ok(())
    }
}

/// Write Arrow RecordBatches to Parquet bytes in memory.
fn write_parquet_bytes(batches: &[RecordBatch], compression: Compression) -> Result<Vec<u8>> {
    let parquet_compression = match compression {
        Compression::Snappy => ParquetCompression::SNAPPY,
        Compression::Zstd => ParquetCompression::ZSTD(Default::default()),
        Compression::Lz4 => ParquetCompression::LZ4,
    };

    let props = WriterProperties::builder()
        .set_compression(parquet_compression)
        .set_max_row_group_size(128 * 1024)
        .build();

    let mut buffer = Vec::new();
    {
        let schema = batches[0].schema();
        let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props)).map_err(|e| {
            nova_common::NovaError::ParquetError {
                source: Box::new(e),
            }
        })?;

        for batch in batches {
            writer
                .write(batch)
                .map_err(|e| nova_common::NovaError::ParquetError {
                    source: Box::new(e),
                })?;
        }

        writer
            .close()
            .map_err(|e| nova_common::NovaError::ParquetError {
                source: Box::new(e),
            })?;
    }

    Ok(buffer)
}

/// Compute column-level statistics from Arrow RecordBatches.
fn compute_column_stats(batches: &[RecordBatch]) -> HashMap<u32, ColumnStats> {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    let schema = batches[0].schema();
    let num_cols = schema.fields().len();
    let mut stats: HashMap<u32, ColumnStats> = HashMap::new();

    for col_idx in 0..num_cols {
        let dtype = schema.field(col_idx).data_type();

        let mut null_count: u64 = 0;
        let mut byte_size: u64 = 0;
        let mut min_val: Option<Vec<u8>> = None;
        let mut max_val: Option<Vec<u8>> = None;

        for batch in batches {
            let col = batch.column(col_idx);
            null_count += col.null_count() as u64;
            byte_size += col.get_array_memory_size() as u64;

            // Only compute min/max from first batch with data
            if min_val.is_none() && !col.is_empty() {
                match dtype {
                    DataType::Int8 => {
                        let arr = col.as_any().downcast_ref::<Int8Array>().unwrap();
                        let vals: Vec<i8> = arr.iter().flatten().collect();
                        if !vals.is_empty() {
                            min_val = Some(vec![*vals.iter().min().unwrap() as u8]);
                            max_val = Some(vec![*vals.iter().max().unwrap() as u8]);
                        }
                    }
                    DataType::Int16 => {
                        let arr = col.as_any().downcast_ref::<Int16Array>().unwrap();
                        let vals: Vec<i16> = arr.iter().flatten().collect();
                        if !vals.is_empty() {
                            min_val = Some(vals.iter().min().unwrap().to_le_bytes().to_vec());
                            max_val = Some(vals.iter().max().unwrap().to_le_bytes().to_vec());
                        }
                    }
                    DataType::Int32 => {
                        let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
                        let vals: Vec<i32> = arr.iter().flatten().collect();
                        if !vals.is_empty() {
                            min_val = Some(vals.iter().min().unwrap().to_le_bytes().to_vec());
                            max_val = Some(vals.iter().max().unwrap().to_le_bytes().to_vec());
                        }
                    }
                    DataType::Int64 => {
                        let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                        let vals: Vec<i64> = arr.iter().flatten().collect();
                        if !vals.is_empty() {
                            min_val = Some(vals.iter().min().unwrap().to_le_bytes().to_vec());
                            max_val = Some(vals.iter().max().unwrap().to_le_bytes().to_vec());
                        }
                    }
                    DataType::Float32 => {
                        let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
                        let vals: Vec<f32> = arr.iter().flatten().collect();
                        if !vals.is_empty() {
                            min_val = Some(
                                vals.iter()
                                    .min_by(|a, b| a.partial_cmp(b).unwrap())
                                    .unwrap()
                                    .to_le_bytes()
                                    .to_vec(),
                            );
                            max_val = Some(
                                vals.iter()
                                    .max_by(|a, b| a.partial_cmp(b).unwrap())
                                    .unwrap()
                                    .to_le_bytes()
                                    .to_vec(),
                            );
                        }
                    }
                    DataType::Float64 => {
                        let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
                        let vals: Vec<f64> = arr.iter().flatten().collect();
                        if !vals.is_empty() {
                            min_val = Some(
                                vals.iter()
                                    .min_by(|a, b| a.partial_cmp(b).unwrap())
                                    .unwrap()
                                    .to_le_bytes()
                                    .to_vec(),
                            );
                            max_val = Some(
                                vals.iter()
                                    .max_by(|a, b| a.partial_cmp(b).unwrap())
                                    .unwrap()
                                    .to_le_bytes()
                                    .to_vec(),
                            );
                        }
                    }
                    DataType::Utf8 => {
                        let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                        let vals: Vec<&str> = arr.iter().flatten().collect();
                        if !vals.is_empty() {
                            min_val = Some(vals.iter().min().unwrap().as_bytes().to_vec());
                            max_val = Some(vals.iter().max().unwrap().as_bytes().to_vec());
                        }
                    }
                    DataType::Date32 => {
                        let arr = col.as_any().downcast_ref::<Date32Array>().unwrap();
                        let vals: Vec<i32> = arr.iter().flatten().collect();
                        if !vals.is_empty() {
                            min_val = Some(vals.iter().min().unwrap().to_le_bytes().to_vec());
                            max_val = Some(vals.iter().max().unwrap().to_le_bytes().to_vec());
                        }
                    }
                    _ => {}
                }
            }
        }

        stats.insert(
            col_idx as u32,
            ColumnStats {
                min_value: min_val,
                max_value: max_val,
                null_count,
                distinct_count: 0,
                byte_size,
            },
        );
    }

    stats
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use object_store::local::LocalFileSystem;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec![
                    Some("alice"),
                    Some("bob"),
                    Some("charlie"),
                    Some("dave"),
                    None,
                ])),
            ],
        )
        .unwrap()
    }

    fn test_writer() -> (MpWriter, TempDir) {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let writer = MpWriter::new(store, "test".to_string());
        (writer, dir)
    }

    #[tokio::test]
    async fn test_write_mp() {
        let (writer, _dir) = test_writer();
        let batch = test_batch();

        let mp = writer.write(1, 1, 1, &[batch], 1).await.unwrap();

        assert_eq!(mp.table_id, 1);
        assert_eq!(mp.row_count, 5);
        assert!(mp.byte_size > 0);
        assert_eq!(mp.compression, Compression::Snappy);
        assert!(!mp.active);
        assert_eq!(mp.column_stats.len(), 2);
    }

    #[tokio::test]
    async fn test_column_stats_int64() {
        let (writer, _dir) = test_writer();
        let batch = test_batch();

        let mp = writer.write(1, 1, 1, &[batch], 1).await.unwrap();

        let id_stats = mp.column_stats.get(&0).unwrap();
        assert_eq!(id_stats.null_count, 0);
        let min_id = i64::from_le_bytes(
            id_stats
                .min_value
                .as_ref()
                .unwrap()
                .as_slice()
                .try_into()
                .unwrap(),
        );
        let max_id = i64::from_le_bytes(
            id_stats
                .max_value
                .as_ref()
                .unwrap()
                .as_slice()
                .try_into()
                .unwrap(),
        );
        assert_eq!(min_id, 1);
        assert_eq!(max_id, 5);
    }

    #[tokio::test]
    async fn test_column_stats_utf8() {
        let (writer, _dir) = test_writer();
        let batch = test_batch();

        let mp = writer.write(1, 1, 1, &[batch], 1).await.unwrap();

        let name_stats = mp.column_stats.get(&1).unwrap();
        assert_eq!(name_stats.null_count, 1);
        let min_name = String::from_utf8(name_stats.min_value.as_ref().unwrap().clone()).unwrap();
        let max_name = String::from_utf8(name_stats.max_value.as_ref().unwrap().clone()).unwrap();
        assert_eq!(min_name, "alice");
        assert_eq!(max_name, "dave");
    }

    #[tokio::test]
    async fn test_empty_batch_error() {
        let (writer, _dir) = test_writer();
        let result = writer.write(1, 1, 1, &[], 1).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_multiple_batches() {
        let (writer, _dir) = test_writer();
        let batch1 = test_batch();
        let batch2 = test_batch();

        let mp = writer.write(1, 1, 1, &[batch1, batch2], 1).await.unwrap();
        assert_eq!(mp.row_count, 10);
    }

    #[tokio::test]
    async fn test_commit_and_abort() {
        let (writer, _dir) = test_writer();
        let batch = test_batch();

        let mp = writer.write(1, 1, 1, &[batch], 1).await.unwrap();
        assert!(mp.s3_temp_path.is_some());

        writer.commit_mp(&mp).await.unwrap();

        let batch2 = test_batch();
        let mp2 = writer.write(1, 2, 2, &[batch2], 2).await.unwrap();
        writer.abort_mp(&mp2).await.unwrap();
    }
}
