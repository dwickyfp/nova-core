// MpReader — Read micro-partitions from S3 as Arrow RecordBatch streams.
//
// Flow: S3 -> Parquet -> Arrow RecordBatch stream
// Supports: column pruning (read only needed columns), predicate pushdown to row groups.

use arrow::record_batch::RecordBatch;
use nova_common::{MicroPartitionMeta, Result};
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::sync::Arc;

/// Reads micro-partitions from object storage as Arrow RecordBatch streams.
pub struct MpReader {
    store: Arc<dyn ObjectStore>,
    batch_size: usize,
}

impl MpReader {
    /// Create a new MpReader.
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            batch_size: 8192,
        }
    }

    /// Set batch size (default 8192 rows).
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Read a micro-partition from S3 into memory as RecordBatches.
    ///
    /// Downloads the Parquet file from S3 and deserializes into Arrow batches.
    /// Column pruning: only read specified column indices (None = all columns).
    pub async fn read(
        &self,
        mp: &MicroPartitionMeta,
        projection: Option<&[usize]>,
    ) -> Result<Vec<RecordBatch>> {
        let read_path = mp.s3_temp_path.as_ref().unwrap_or(&mp.s3_path);
        let object_path = object_store::path::Path::from(read_path.clone());

        let data = self
            .store
            .get(&object_path)
            .await
            .map_err(|e| nova_common::NovaError::ObjectStoreError {
                source: Box::new(e),
            })?
            .bytes()
            .await
            .map_err(|e| nova_common::NovaError::ObjectStoreError {
                source: Box::new(e),
            })?;

        let mut builder = ParquetRecordBatchReaderBuilder::try_new(data).map_err(|e| {
            nova_common::NovaError::ParquetError {
                source: Box::new(e),
            }
        })?;

        // Column pruning
        if let Some(cols) = projection {
            let mask = {
                let schema = builder.parquet_schema();
                parquet::arrow::ProjectionMask::roots(schema, cols.iter().copied())
            };
            builder = builder.with_projection(mask);
        }
        builder = builder.with_batch_size(self.batch_size);

        let reader = builder
            .build()
            .map_err(|e| nova_common::NovaError::ParquetError {
                source: Box::new(e),
            })?;

        let mut batches = Vec::new();
        for batch in reader {
            let batch = batch.map_err(|e| nova_common::NovaError::ArrowError {
                source: Box::new(e),
            })?;
            batches.push(batch);
        }

        Ok(batches)
    }

    /// Read all columns (no pruning).
    pub async fn read_all(&self, mp: &MicroPartitionMeta) -> Result<Vec<RecordBatch>> {
        self.read(mp, None).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mp_writer::MpWriter;
    use arrow::array::{Array, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use nova_common::Compression;
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("amount", DataType::Float64, false),
        ]))
    }

    fn test_batch() -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec![
                    Some("alice"),
                    Some("bob"),
                    Some("charlie"),
                    Some("dave"),
                    None,
                ])),
                Arc::new(Float64Array::from(vec![100.0, 200.0, 300.0, 400.0, 500.0])),
            ],
        )
        .unwrap()
    }

    fn setup() -> (MpWriter, MpReader, TempDir) {
        let dir = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let writer = MpWriter::new(store.clone(), "test".to_string());
        let reader = MpReader::new(store);
        (writer, reader, dir)
    }

    #[tokio::test]
    async fn test_read_all_columns() {
        let (writer, reader, _dir) = setup();
        let batch = test_batch();

        let mp = writer.write(1, 1, 1, &[batch], 1).await.unwrap();

        let batches = reader.read_all(&mp).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 5);
        assert_eq!(batches[0].num_columns(), 3);
    }

    #[tokio::test]
    async fn test_read_with_projection() {
        let (writer, reader, _dir) = setup();
        let batch = test_batch();

        let mp = writer.write(1, 1, 1, &[batch], 1).await.unwrap();

        // Read only columns 0 and 2 (id, amount)
        let batches = reader.read(&mp, Some(&[0, 2])).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 5);
        assert_eq!(batches[0].num_columns(), 2); // only id and amount

        let schema = batches[0].schema();
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "amount");
    }

    #[tokio::test]
    async fn test_read_data_correctness() {
        let (writer, reader, _dir) = setup();
        let batch = test_batch();

        let mp = writer.write(1, 1, 1, &[batch], 1).await.unwrap();

        let batches = reader.read_all(&mp).await.unwrap();
        let result = &batches[0];

        let ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(4), 5);

        let names = result
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "alice");
        assert!(names.is_null(4));
    }

    #[tokio::test]
    async fn test_roundtrip_multiple_batches() {
        let (writer, reader, _dir) = setup();
        let batch1 = test_batch();
        let batch2 = test_batch();

        // Write as single MP with 2 batches
        let mp = writer.write(1, 1, 1, &[batch1, batch2], 1).await.unwrap();
        assert_eq!(mp.row_count, 10);

        // Read back
        let batches = reader.read_all(&mp).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 10);
    }

    #[tokio::test]
    async fn test_read_with_small_batch_size() {
        let dir = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let writer = MpWriter::new(store.clone(), "test".to_string());
        let reader = MpReader::new(store).with_batch_size(2); // small batch

        let batch = test_batch(); // 5 rows
        let mp = writer.write(1, 1, 1, &[batch], 1).await.unwrap();

        let batches = reader.read_all(&mp).await.unwrap();
        // With batch_size=2, 5 rows -> 3 batches (2, 2, 1)
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[1].num_rows(), 2);
        assert_eq!(batches[2].num_rows(), 1);
    }
}
