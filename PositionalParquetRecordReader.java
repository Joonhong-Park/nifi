package com.example.nifi.parquet;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.parquet.stream.NifiParquetInputFile;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * NiFi 1.23.2 ParquetRecordReader 기반.
 * Parquet 읽기에 PositionalParquetReaderBuilder(→ PositionalAvroReadSupport)를 사용한다.
 */
public class PositionalParquetRecordReader implements RecordReader {

    private GenericRecord lastParquetRecord;
    private final RecordSchema recordSchema;

    private final InputStream inputStream;
    private final InputFile inputFile;
    private final ParquetReader<GenericRecord> parquetReader;

    public PositionalParquetRecordReader(final InputStream inputStream, final long inputLength,
                                         final Configuration configuration) throws IOException {
        if (inputLength < 0) {
            throw new IllegalArgumentException("입력 길이가 올바르지 않습니다: '" + inputLength
                    + "'. 이 Reader는 InputStream의 길이를 알아야 하며, 길이를 알 수 없는 경우에는 사용할 수 없습니다.");
        }

        this.inputStream = inputStream;

        inputFile = new NifiParquetInputFile(inputStream, inputLength);
        parquetReader = new PositionalParquetReaderBuilder(inputFile).withConf(configuration).build();

        // 첫 레코드를 읽어 스키마를 얻음 (원본과 동일)
        lastParquetRecord = parquetReader.read();
        if (lastParquetRecord == null) {
            throw new EOFException("레코드가 없어 스키마를 가져올 수 없습니다 (0건 Parquet 파일)");
        }

        // Avro 스키마(사용자 이름 + 파일 타입) → NiFi RecordSchema
        recordSchema = AvroTypeUtil.createSchema(lastParquetRecord.getSchema());
    }

    @Override
    public Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields) throws IOException {
        if (lastParquetRecord == null) {
            return null;
        }

        final Map<String, Object> values = AvroTypeUtil.convertAvroRecordToMap(lastParquetRecord, recordSchema);
        final Record record = new MapRecord(recordSchema, values);

        lastParquetRecord = parquetReader.read();

        return record;
    }

    @Override
    public RecordSchema getSchema() {
        return recordSchema;
    }

    @Override
    public void close() throws IOException {
        try {
            parquetReader.close();
        } finally {
            inputStream.close();
        }
    }
}