use arrow::record_batch::RecordBatch;
use nova_common::{ChangePayloadRef, NovaError, Result, TableId, TxnId};
use object_store::{ObjectStore, PutPayload};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression as ParquetCompression;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;

pub struct CdcPayloadWriter {
    store: Arc<dyn ObjectStore>,
    bucket: String,
}

impl CdcPayloadWriter {
    pub fn new(store: Arc<dyn ObjectStore>, bucket: String) -> Self {
        Self { store, bucket }
    }

    pub async fn write_payload(
        &self,
        table_id: TableId,
        txn_id: TxnId,
        start_sequence: u64,
        batch: &RecordBatch,
    ) -> Result<ChangePayloadRef> {
        if batch.num_rows() == 0 {
            return Err(NovaError::Internal {
                message: "cannot write empty CDC payload".to_string(),
            });
        }

        let end_sequence = start_sequence + batch.num_rows() as u64 - 1;
        let path = cdc_payload_path(&self.bucket, table_id, txn_id, start_sequence, end_sequence);
        let bytes = write_parquet_bytes(batch)?;
        self.store
            .put(
                &object_store::path::Path::from(path.clone()),
                PutPayload::from(bytes),
            )
            .await
            .map_err(|e| NovaError::ObjectStoreError {
                source: Box::new(e),
            })?;

        Ok(ChangePayloadRef {
            path,
            row_start: 0,
            row_count: batch.num_rows() as u64,
        })
    }
}

pub struct CdcPayloadReader {
    store: Arc<dyn ObjectStore>,
}

impl CdcPayloadReader {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    pub async fn read_payload(&self, payload: &ChangePayloadRef) -> Result<Vec<RecordBatch>> {
        let data = self
            .store
            .get(&object_store::path::Path::from(payload.path.clone()))
            .await
            .map_err(|e| NovaError::ObjectStoreError {
                source: Box::new(e),
            })?
            .bytes()
            .await
            .map_err(|e| NovaError::ObjectStoreError {
                source: Box::new(e),
            })?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(data).map_err(|e| {
            NovaError::ParquetError {
                source: Box::new(e),
            }
        })?;
        let reader = builder.build().map_err(|e| NovaError::ParquetError {
            source: Box::new(e),
        })?;

        let mut batches = Vec::new();
        for batch in reader {
            batches.push(batch.map_err(|e| NovaError::ArrowError {
                source: Box::new(e),
            })?);
        }
        Ok(batches)
    }
}

pub fn cdc_payload_path(
    bucket: &str,
    table_id: TableId,
    txn_id: TxnId,
    start_sequence: u64,
    end_sequence: u64,
) -> String {
    format!(
        "{bucket}/cdc/tables/{table_id}/txn-{txn_id}/seq-{start_sequence}-{end_sequence}.parquet"
    )
}

fn write_parquet_bytes(batch: &RecordBatch) -> Result<Vec<u8>> {
    let props = WriterProperties::builder()
        .set_compression(ParquetCompression::SNAPPY)
        .set_max_row_group_size(128 * 1024)
        .build();
    let mut buffer = Vec::new();
    {
        let mut writer =
            ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).map_err(|e| {
                NovaError::ParquetError {
                    source: Box::new(e),
                }
            })?;
        writer.write(batch).map_err(|e| NovaError::ParquetError {
            source: Box::new(e),
        })?;
        writer.close().map_err(|e| NovaError::ParquetError {
            source: Box::new(e),
        })?;
    }
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, Int64Array, StringArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    fn cdc_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("METADATA$ACTION", DataType::Utf8, false),
            Field::new("METADATA$ISUPDATE", DataType::Boolean, false),
            Field::new("METADATA$ROW_ID", DataType::Utf8, false),
            Field::new("METADATA$TXN_ID", DataType::UInt64, false),
            Field::new("METADATA$COMMIT_TS", DataType::UInt64, false),
            Field::new("METADATA$SEQUENCE", DataType::UInt64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["INSERT", "INSERT", "INSERT"])),
                Arc::new(BooleanArray::from(vec![false, false, false])),
                Arc::new(StringArray::from(vec!["1:10:0:0", "1:10:1:0", "1:10:2:0"])),
                Arc::new(UInt64Array::from(vec![7, 7, 7])),
                Arc::new(UInt64Array::from(vec![100, 100, 100])),
                Arc::new(UInt64Array::from(vec![1, 2, 3])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn payload_round_trip_preserves_rows_and_metadata() {
        let dir = TempDir::new().unwrap();
        let store =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()) as Arc<dyn ObjectStore>;
        let writer = CdcPayloadWriter::new(store.clone(), "nova".to_string());
        let reader = CdcPayloadReader::new(store);
        let payload = writer.write_payload(42, 7, 1, &cdc_batch()).await.unwrap();
        assert_eq!(payload.path, "nova/cdc/tables/42/txn-7/seq-1-3.parquet");
        assert_eq!(payload.row_start, 0);
        assert_eq!(payload.row_count, 3);

        let batches = reader.read_payload(&payload).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
        assert_eq!(batches[0].schema().field(1).name(), "METADATA$ACTION");
    }
}
