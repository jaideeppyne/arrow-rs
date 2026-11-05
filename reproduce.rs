use std::sync::Arc;
use arrow_array::{ArrayRef, Float64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
use parquet::basic::{Compression, Encoding};
use parquet::errors::ParquetError;
use parquet::file::{properties::{WriterProperties, WriterVersion}, reader::{FileReader, SerializedFileReader}};

fn read(data: Bytes) -> Result<usize, ParquetError> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(data)?.build()?;
    let mut rows = 0;
    for batch in reader { rows += batch?.num_rows(); }
    Ok(rows)
}

fn main() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]));
    let values: ArrayRef = Arc::new(Float64Array::from_iter_values((0..8).map(|i| f64::from(i) + 0.5)));
    let batch = RecordBatch::try_new(schema.clone(), vec![values]).unwrap();
    let props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_dictionary_enabled(false)
        .set_compression(Compression::UNCOMPRESSED)
        .set_encoding(Encoding::BYTE_STREAM_SPLIT).build();
    let mut raw = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut raw, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let original = Bytes::from(raw);
    assert_eq!(read(original.clone()).unwrap(), 8);
    let file = SerializedFileReader::new(original.clone()).unwrap();
    let (offset, length) = file.metadata().row_group(0).column(0).byte_range();
    let offset = offset as usize;
    let field = original[offset..offset + length as usize].windows(2)
        .position(|pair| pair == [0x15, 16]).unwrap();
    let mut bad = original.to_vec();
    bad[offset + field + 1] = 126;
    std::fs::write("proof/valid-bss.parquet", &original).unwrap();
    std::fs::write("proof/malformed-bss.parquet", &bad).unwrap();
    println!("valid: 8 rows; changed byte {} from 16 to 126; file size {} unchanged", offset + field + 1, bad.len());
    let result = std::panic::catch_unwind(|| read(Bytes::from(bad)));
    if std::env::var_os("BSS_EXPECT_PANIC").is_some() {
        assert!(result.is_err(), "baseline did not panic: {result:?}");
        println!("baseline: observed the out-of-bounds panic through the public Parquet reader");
    } else {
        let error = result.expect("reader must not panic").unwrap_err();
        assert!(error.to_string().contains("insufficient values read from column"), "{error}");
        println!("fixed: {error}");
    }
}
