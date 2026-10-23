package com.example.nifi.parquet;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.nifi.parquet.stream.NifiParquetInputFile;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 진단용 (jar에 포함하지 않음, 확인 후 삭제).
 * 기존 ParquetReader와 커스텀 Reader가 decimal 컬럼을 어떻게 읽는지 단계별로 출력한다.
 * 실행 인자: <Parquet 파일 경로> <문제 컬럼의 파일 원본 이름>
 */
public class Diag {

    public static void main(String[] args) throws Exception {
        final File f = new File(args[0]);       // Parquet 파일 경로
        final String col = args[1];             // 문제 컬럼의 파일 원본 이름

        final Configuration conf = new Configuration();
        conf.setBoolean("parquet.avro.compatible", true);
        conf.setBoolean("parquet.avro.readInt96AsFixed", true);

        // 1) footer: Parquet 스키마와 key-value 메타데이터
        MessageType fileSchema;
        try (ParquetFileReader r = ParquetFileReader.open(open(f))) {
            fileSchema = r.getFooter().getFileMetaData().getSchema();
            final Map<String, String> kv = r.getFooter().getFileMetaData().getKeyValueMetaData();
            System.out.println("[1] key-value keys = " + kv.keySet());
        }
        final Type t = fileSchema.getType(col);
        System.out.println("[2] parquet type   = " + t);

        // 2) 변환기 결과 (내장 Avro 스키마가 없을 때 기존 Reader가 쓰는 Avro 스키마)
        try {
            System.out.println("[3] converter      = "
                    + new AvroSchemaConverter(conf).convert(fileSchema).getField(col).schema());
        } catch (Exception e) {
            System.out.println("[3] converter 실패 = " + e);
        }

        // 3) AvroParquetReader 기본 방식으로 첫 레코드 읽기
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(open(f))
                .withConf(conf).build()) {
            final GenericRecord rec = reader.read();
            final Object v = rec.get(col);
            System.out.println("[4] record schema  = " + rec.getSchema().getField(col).schema());
            System.out.println("[5] value          = " + v + " (" + (v == null ? "null" : v.getClass().getName()) + ")");
        }

        // 4) 같은 Avro 스키마를 read schema(문자열)로 지정했을 때
        final Configuration conf2 = new Configuration(conf);
        org.apache.parquet.avro.AvroReadSupport.setAvroReadSchema(conf2,
                new AvroSchemaConverter(conf).convert(fileSchema));
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(open(f))
                .withConf(conf2).build()) {
            final Object v = reader.read().get(col);
            System.out.println("[6] with readSchema = " + v + " (" + (v == null ? "null" : v.getClass().getName()) + ")");
        }

        // 5) 호환 모드(구형 경로)를 강제로 켰을 때
        final Configuration conf3 = new Configuration(conf);
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(open(f))
                .withDataModel(org.apache.avro.generic.GenericData.get())
                .withCompatibility(true)
                .withConf(conf3).build()) {
            final Object v = reader.read().get(col);
            System.out.println("[7] compat=true     = " + v + " (" + (v == null ? "null" : v.getClass().getName()) + ")");
        }

        // 6) 커스텀 RecordReader 전체 경로 (사용자 스키마는 c0, c1, ... 로 자동 생성)
        final List<org.apache.avro.Schema.Field> genFields = new ArrayList<>();
        for (int i = 0; i < fileSchema.getFieldCount(); i++) {
            genFields.add(new org.apache.avro.Schema.Field("c" + i,
                    org.apache.avro.Schema.createUnion(
                            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL),
                            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING)),
                    null, org.apache.avro.Schema.Field.NULL_DEFAULT_VALUE));
        }
        final String userSchema = org.apache.avro.Schema.createRecord("diag", null, null, false, genFields).toString();
        final String userField = "c" + fileSchema.getFieldIndex(col);

        final Configuration conf4 = new Configuration(conf);
        conf4.set(PositionalAvroReadSupport.USER_SCHEMA_KEY, userSchema);
        try (PositionalParquetRecordReader rr = new PositionalParquetRecordReader(
                new BufferedInputStream(new FileInputStream(f), (int) f.length() + 1), f.length(), conf4)) {
            final org.apache.nifi.serialization.record.Record r = rr.nextRecord(true, false);
            final Object v = r.getValue(userField);
            System.out.println("[8] custom reader   = " + userField + " = " + v + " (" + (v == null ? "null" : v.getClass().getName())
                    + ") type=" + rr.getSchema().getField(userField).get().getDataType());
        }

        // 7) NiFi 기본 ParquetRecordReader (기존 ParquetReader가 내부에서 쓰는 클래스)
        try (org.apache.nifi.parquet.record.ParquetRecordReader rr = new org.apache.nifi.parquet.record.ParquetRecordReader(
                new BufferedInputStream(new FileInputStream(f), (int) f.length() + 1), f.length(), new Configuration(conf))) {
            final org.apache.nifi.serialization.record.Record r = rr.nextRecord(true, false);
            final Object v = r.getValue(col);
            System.out.println("[9] nifi reader     = " + v + " (" + (v == null ? "null" : v.getClass().getName())
                    + ") type=" + rr.getSchema().getField(col).get().getDataType());
        }
    }

    /** mark/reset 지원 스트림으로 Parquet 파일 열기 (테스트용, 파일 전체를 버퍼에 올림) */
    private static NifiParquetInputFile open(final File f) throws Exception {
        final InputStream in = new BufferedInputStream(new FileInputStream(f), (int) f.length() + 1);
        return new NifiParquetInputFile(in, f.length());
    }
}