package com.example.nifi.parquet;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.parquet.stream.NifiParquetInputFile;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * NiFi 1.23.2 ParquetRecordReader 기반 RecordReader.
 * <ul>
 *   <li>Parquet 읽기에 PositionalParquetReaderBuilder(→ PositionalAvroReadSupport) 사용</li>
 *   <li>INT32/INT64 decimal 필드는 DECIMAL(precision, scale) 타입과 BigDecimal 값으로 변환
 *       (NiFi 기본 변환은 Avro long을 LONG으로 바꿔 scale 손실)</li>
 *   <li>NiFi에 노출하는 스키마는 항상 새로 생성(원문 없음, alias 없음)
 *       → ExtractRecordSchema의 avro.schema가 파일과 관계없이 같은 형태, 필드명은 사용자 컬럼명만</li>
 * </ul>
 */
public class PositionalParquetRecordReader implements RecordReader {

    private GenericRecord lastParquetRecord;
    /** NiFi에 노출하는 스키마 (원문 없음, alias 없음, decimal 필드는 DECIMAL 타입) */
    private final RecordSchema recordSchema;
    /** Avro 스키마 그대로의 NiFi 스키마 (값 변환용) */
    private final RecordSchema avroRecordSchema;
    /** INT32/INT64 decimal 필드명 → scale */
    private final Map<String, Integer> decimalScales = new HashMap<>();

    private final InputStream inputStream;
    private final ParquetReader<GenericRecord> parquetReader;

    public PositionalParquetRecordReader(final InputStream inputStream, final long inputLength,
                                         final Configuration configuration) throws IOException {
        if (inputLength < 0) {
            throw new IllegalArgumentException("입력 길이가 올바르지 않습니다: '" + inputLength
                    + "'. 이 Reader는 InputStream의 길이를 알아야 하며, 길이를 알 수 없는 경우에는 사용할 수 없습니다.");
        }

        this.inputStream = inputStream;

        final InputFile inputFile = new NifiParquetInputFile(inputStream, inputLength);
        final PositionalParquetReaderBuilder builder = new PositionalParquetReaderBuilder(inputFile);
        parquetReader = builder.withConf(configuration).build();

        // 첫 레코드를 읽어 스키마를 얻음 (이 시점에 ReadSupport.init()이 실행됨)
        lastParquetRecord = parquetReader.read();
        if (lastParquetRecord == null) {
            throw new EOFException("레코드가 없어 스키마를 가져올 수 없습니다 (0건 Parquet 파일)");
        }

        // Avro 스키마(사용자 이름 + 파일 타입) → NiFi RecordSchema (값 변환용, 원문·alias 포함)
        avroRecordSchema = AvroTypeUtil.createSchema(lastParquetRecord.getSchema());

        // 노출 스키마: INT32/INT64 decimal은 DECIMAL로 교체, 원문·alias 없이 새로 생성
        final Map<String, LogicalTypeAnnotation.DecimalLogicalTypeAnnotation> decimals =
                builder.getPositionalReadSupport().getIntegerDecimalFields();
        decimals.forEach((name, d) -> decimalScales.put(name, d.getScale()));
        recordSchema = buildOutputSchema(avroRecordSchema, decimals);
    }

    @Override
    public Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields) throws IOException {
        if (lastParquetRecord == null) {
            return null;
        }

        Map<String, Object> values = AvroTypeUtil.convertAvroRecordToMap(lastParquetRecord, avroRecordSchema);

        // decimal 필드는 NiFi 변환 결과(Long) 대신 원본 값에서 BigDecimal 생성
        if (!decimalScales.isEmpty()) {
            values = new HashMap<>(values);
            for (Map.Entry<String, Integer> e : decimalScales.entrySet()) {
                values.put(e.getKey(), toDecimal(lastParquetRecord.get(e.getKey()), e.getValue(), e.getKey()));
            }
        }

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

    /** parquet-avro 원본 값 → BigDecimal (BigDecimal이면 그대로, 정수면 unscaled 값으로 보고 scale 적용) */
    private static BigDecimal toDecimal(final Object raw, final int scale, final String fieldName) {
        if (raw == null) {
            return null;
        }
        if (raw instanceof BigDecimal) {
            return (BigDecimal) raw;
        }
        if (raw instanceof Long || raw instanceof Integer) {
            return BigDecimal.valueOf(((Number) raw).longValue(), scale);
        }
        throw new IllegalStateException("decimal 필드 값 형식을 처리할 수 없습니다: 필드=" + fieldName
                + ", 타입=" + raw.getClass().getName());
    }

    /**
     * NiFi에 노출할 스키마 생성.
     * - 항상 새 SimpleRecordSchema 생성 (Avro 원문 없음 → avro.schema는 NiFi가 필드 타입으로 생성)
     * - 모든 필드의 alias 제거 (alias는 parquet 컬럼 연결용 read schema 내부에서만 필요)
     * - INT32/INT64 decimal 필드는 DECIMAL(precision, scale)로 교체
     */
    private static RecordSchema buildOutputSchema(final RecordSchema schema,
            final Map<String, LogicalTypeAnnotation.DecimalLogicalTypeAnnotation> decimals) {
        final List<RecordField> fields = new ArrayList<>(schema.getFieldCount());
        for (RecordField f : schema.getFields()) {
            final LogicalTypeAnnotation.DecimalLogicalTypeAnnotation d = decimals.get(f.getFieldName());
            final DataType type = (d == null)
                    ? f.getDataType()
                    : RecordFieldType.DECIMAL.getDecimalDataType(d.getPrecision(), d.getScale());
            fields.add(new RecordField(f.getFieldName(), type, f.getDefaultValue(), Collections.emptySet(), f.isNullable()));
        }
        return new SimpleRecordSchema(fields);
    }
}