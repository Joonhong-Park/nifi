package com.example.nifi.parquet;

import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.InputFile;

/**
 * AvroParquetReader.Builder 대신 사용하는 Builder.
 * AvroParquetReader.Builder는 ReadSupport를 바꿀 수 없으므로, ParquetReader.Builder를 상속해
 * PositionalAvroReadSupport를 사용하고 그 인스턴스를 보관한다 (decimal 컬럼 정보 조회용).
 */
public class PositionalParquetReaderBuilder extends ParquetReader.Builder<GenericRecord> {

    private final PositionalAvroReadSupport readSupport = new PositionalAvroReadSupport();

    public PositionalParquetReaderBuilder(final InputFile file) {
        super(file);
    }

    @Override
    protected ReadSupport<GenericRecord> getReadSupport() {
        return readSupport;
    }

    /** 첫 레코드를 읽은 뒤(= ReadSupport.init 이후) decimal 컬럼 정보를 꺼내기 위해 사용 */
    public PositionalAvroReadSupport getPositionalReadSupport() {
        return readSupport;
    }
}