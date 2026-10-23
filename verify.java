package com.example.nifi.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.Record;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * 진단용 (jar에 포함하지 않음, 확인 후 삭제).
 * INT64 DECIMAL(10,3) 파일을 만들어 기존 Reader / 커스텀 Reader로 읽고,
 * ParquetRecordSetWriter와 같은 변환 함수(AvroTypeUtil.createAvroRecord)로 double 변환 결과를 비교한다.
 */
public class Verify {

    /** 원본 값 (unscaled, scale 3). null은 값 없음 */
    private static final Long[] UNSCALED = {6000L, 6125L, -1500L, null};

    public static void main(String[] args) throws Exception {
        final File file = new File(args.length > 0 ? args[0] : "verify_int64_decimal.parquet");

        // ---------------------------------------------------------------
        // 1) 테스트 Parquet 파일 생성: ID INT32, AMT INT64 DECIMAL(10,3), 내장 Avro 스키마 없음
        // ---------------------------------------------------------------
        final MessageType parquetSchema = Types.buildMessage()
                .required(PrimitiveTypeName.INT32).named("ID")
                .optional(PrimitiveTypeName.INT64).as(LogicalTypeAnnotation.decimalType(3, 10)).named("AMT")
                .named("test");

        if (file.exists() && !file.delete()) {
            throw new IOException("기존 테스트 파일을 삭제할 수 없습니다: " + file.getAbsolutePath());
        }
        final SimpleGroupFactory factory = new SimpleGroupFactory(parquetSchema);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalFileOutput(file))
                .withType(parquetSchema)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()) {
            for (int i = 0; i < UNSCALED.length; i++) {
                final Group g = factory.newGroup().append("ID", i + 1);
                if (UNSCALED[i] != null) {
                    g.append("AMT", UNSCALED[i]);
                }
                writer.write(g);
            }
        }
        System.out.println("테스트 파일: " + file.getAbsolutePath());
        System.out.println("parquet 스키마: " + parquetSchema);

        // NiFi ParquetReader 공통 설정과 동일 (Avro Read Compatibility 기본값 true, INT96 → fixed)
        final Configuration conf = new Configuration();
        conf.setBoolean("parquet.avro.compatible", true);
        conf.setBoolean("parquet.avro.readInt96AsFixed", true);

        // ---------------------------------------------------------------
        // 2) 기존 Reader (NiFi ParquetRecordReader) + Writer 변환
        //    out_schema: 이름은 파일 원본(ID, AMT), AMT는 double
        // ---------------------------------------------------------------
        final Schema outSchemaOriginal = new Schema.Parser().parse(
                "{\"type\":\"record\",\"name\":\"out\",\"fields\":["
                        + "{\"name\":\"ID\",\"type\":[\"null\",\"int\"],\"default\":null},"
                        + "{\"name\":\"AMT\",\"type\":[\"null\",\"double\"],\"default\":null}]}");

        System.out.println();
        System.out.println("=== 기존 ParquetReader + Writer(double) ===");
        try (RecordReader reader = new org.apache.nifi.parquet.record.ParquetRecordReader(
                open(file), file.length(), new Configuration(conf))) {
            print(reader, "AMT", outSchemaOriginal);
        }

        // ---------------------------------------------------------------
        // 3) 커스텀 Reader (PositionalParquetRecordReader) + Writer 변환
        //    사용자 스키마: id, amt (decimal은 double로 선언 — 실제 사용 형태와 동일)
        // ---------------------------------------------------------------
        final String userSchema = "{\"type\":\"record\",\"name\":\"in\",\"fields\":["
                + "{\"name\":\"id\",\"type\":[\"null\",\"int\"],\"default\":null},"
                + "{\"name\":\"amt\",\"type\":[\"null\",\"double\"],\"default\":null}]}";
        final Schema outSchemaUser = new Schema.Parser().parse(
                "{\"type\":\"record\",\"name\":\"out\",\"fields\":["
                        + "{\"name\":\"id\",\"type\":[\"null\",\"int\"],\"default\":null},"
                        + "{\"name\":\"amt\",\"type\":[\"null\",\"double\"],\"default\":null}]}");

        final Configuration customConf = new Configuration(conf);
        customConf.set(PositionalAvroReadSupport.USER_SCHEMA_KEY, userSchema);

        System.out.println();
        System.out.println("=== 커스텀 PositionalParquetReader + Writer(double) ===");
        try (RecordReader reader = new PositionalParquetRecordReader(open(file), file.length(), customConf)) {
            print(reader, "amt", outSchemaUser);
        }
    }

    /** 레코드마다 원본 값 / Reader 값과 타입 / Writer 변환 후 값을 출력 */
    private static void print(final RecordReader reader, final String field, final Schema outSchema) throws Exception {
        System.out.println("Reader 필드 타입: " + reader.getSchema().getField(field).get().getDataType());
        System.out.println("행 | 원본        | Reader 값 (Java 타입)              | Writer 변환 후 double");
        Record r;
        int i = 0;
        while ((r = reader.nextRecord(true, false)) != null) {
            final Object readerValue = r.getValue(field);
            final GenericRecord avro = AvroTypeUtil.createAvroRecord(r, outSchema);   // ParquetRecordSetWriter와 같은 변환
            final Object written = avro.get(field);
            final String expected = UNSCALED[i] == null ? "null" : java.math.BigDecimal.valueOf(UNSCALED[i], 3).toPlainString();
            System.out.printf("%2d | %-11s | %-34s | %s%n", i + 1, expected,
                    readerValue + " (" + (readerValue == null ? "null" : readerValue.getClass().getSimpleName()) + ")",
                    written);
            i++;
        }
    }

    /** mark/reset 지원 스트림 (NifiParquetInputFile 요구사항) */
    private static InputStream open(final File f) throws IOException {
        return new BufferedInputStream(new FileInputStream(f), (int) f.length() + 1);
    }

    /** Hadoop 파일시스템 없이 로컬 파일에 쓰는 OutputFile (Windows winutils 불필요) */
    private static final class LocalFileOutput implements OutputFile {
        private final File file;

        LocalFileOutput(final File file) {
            this.file = file;
        }

        @Override
        public PositionOutputStream create(final long blockSizeHint) throws IOException {
            return createOrOverwrite(blockSizeHint);
        }

        @Override
        public PositionOutputStream createOrOverwrite(final long blockSizeHint) throws IOException {
            final FileOutputStream out = new FileOutputStream(file);
            return new PositionOutputStream() {
                private long pos = 0;

                @Override
                public long getPos() {
                    return pos;
                }

                @Override
                public void write(final int b) throws IOException {
                    out.write(b);
                    pos++;
                }

                @Override
                public void write(final byte[] b, final int off, final int len) throws IOException {
                    out.write(b, off, len);
                    pos += len;
                }

                @Override
                public void flush() throws IOException {
                    out.flush();
                }

                @Override
                public void close() throws IOException {
                    out.close();
                }
            };
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 0;
        }
    }
}