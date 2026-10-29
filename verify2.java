package com.example.nifi.parquet;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 진단용 (jar에 포함하지 않음, 확인 후 삭제).
 * 특수 컬럼명 파일을 만들어 현재 커스텀 Reader로 읽고, 필드별 avro.schema 타입과 1행 값만 출력한다.
 * - 파일 A: bytes/int = FIXED decimal, 'col_1', col/2, col$3, 1col
 * - 파일 B: 파일 A + AMT INT64 decimal
 * - 파일 C: bytes/int = BINARY decimal
 * 실행 인자(선택): 테스트 파일을 만들 폴더 (기본: 프로젝트 폴더)
 */
public class Verify2 {

    public static void main(String[] args) throws Exception {
        final File dir = new File(args.length > 0 ? args[0] : ".");
        final File fileA = new File(dir, "verify2_a.parquet");
        final File fileB = new File(dir, "verify2_b.parquet");
        final File fileC = new File(dir, "verify2_c.parquet");
        writeFile(fileA, false);
        writeFile(fileB, true);
        writeBinaryDecimalFile(fileC);

        // 운영과 같은 사용자 필드명
        run("S1", fileA, userSchema("bytes", "int", "col_1", "col_2", "col_3", "col1"));
        run("S2", fileB, userSchema("bytes", "int", "col_1", "col_2", "col_3", "col1", "amt"));
        run("S3", fileC, userSchema("bytes", "int"));

        // 참고: 현재 코드에서 bytes/int 외 동작을 미리 보기 위한 임시 이름 (운영 사용 아님)
        run("P1", fileA, userSchema("x_bytes", "x_int", "col_1", "col_2", "col_3", "col1"));
    }

    /** 시나리오별 최소 출력: 오류면 1줄, 성공이면 필드마다 "이름 | avro.schema 타입 | 1행 값(Java 타입)" */
    private static void run(final String title, final File file, final String userSchema) {
        // PositionalParquetReader가 만드는 설정과 동일 (Avro Read Compatibility 기본값 true, INT96 → fixed)
        final Configuration conf = new Configuration();
        conf.setBoolean("parquet.avro.compatible", true);
        conf.setBoolean("parquet.avro.readInt96AsFixed", true);
        conf.set(PositionalAvroReadSupport.USER_SCHEMA_KEY, userSchema);

        try (RecordReader reader = new PositionalParquetRecordReader(
                new BufferedInputStream(new FileInputStream(file), (int) file.length() + 1), file.length(), conf)) {

            final RecordSchema schema = reader.getSchema();
            final Schema avro = AvroTypeUtil.extractAvroSchema(schema);   // ExtractRecordSchema와 같은 방식
            final Record r = reader.nextRecord(true, false);

            System.out.println("[" + title + "] 성공");
            for (RecordField f : schema.getFields()) {
                final Object v = r.getValue(f.getFieldName());
                System.out.println("  " + f.getFieldName() + " | " + avro.getField(f.getFieldName()).schema()
                        + " | " + v + " (" + (v == null ? "null" : v.getClass().getSimpleName()) + ")");
            }
        } catch (Throwable t) {
            Throwable c = t;
            while (c.getCause() != null && c.getCause() != c) {
                c = c.getCause();
            }
            System.out.println("[" + title + "] 오류: " + c.getClass().getSimpleName() + ": " + c.getMessage());
        }
    }

    /** 사용자 스키마 JSON 생성 (기존 컬럼은 파일 타입으로 읽으므로 타입은 결과에 영향 없음 → 모두 nullable string) */
    private static String userSchema(final String... names) {
        final StringBuilder sb = new StringBuilder("{\"type\":\"record\",\"name\":\"in\",\"fields\":[");
        for (int i = 0; i < names.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append("{\"name\":\"").append(names[i]).append("\",\"type\":[\"null\",\"string\"],\"default\":null}");
        }
        return sb.append("]}").toString();
    }

    /** 파일 A/B: bytes/int = FIXED(16) decimal, 특수문자 컬럼명 포함 (내장 Avro 스키마 없음, 1행 값 / 2행 null) */
    private static void writeFile(final File file, final boolean withAmt) throws IOException {
        final List<Type> cols = new ArrayList<>();
        cols.add(Types.optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(16)
                .as(LogicalTypeAnnotation.decimalType(0, 38)).named("bytes"));
        cols.add(Types.optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(16)
                .as(LogicalTypeAnnotation.decimalType(15, 38)).named("int"));
        cols.add(Types.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("'col_1'"));
        cols.add(Types.optional(PrimitiveTypeName.INT32).named("col/2"));
        cols.add(Types.optional(PrimitiveTypeName.DOUBLE).named("col$3"));
        cols.add(Types.optional(PrimitiveTypeName.INT64).named("1col"));
        if (withAmt) {
            cols.add(Types.optional(PrimitiveTypeName.INT64).as(LogicalTypeAnnotation.decimalType(3, 10)).named("AMT"));
        }
        final MessageType schema = new MessageType("test", cols);

        if (file.exists() && !file.delete()) {
            throw new IOException("기존 테스트 파일을 삭제할 수 없습니다: " + file.getAbsolutePath());
        }
        final SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalFileOutput(file))
                .withType(schema)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()) {

            // 1행: 값 있음
            final Group g = factory.newGroup();
            g.append("bytes", Binary.fromConstantByteArray(fixed(new BigInteger("20261011220351"), 16)));
            g.append("int", Binary.fromConstantByteArray(fixed(new BigInteger("123456789012345678"), 16))); // 123.456789012345678
            g.append("'col_1'", "hello");
            g.append("col/2", 42);
            g.append("col$3", 1.5d);
            g.append("1col", 9000000000L);
            if (withAmt) {
                g.append("AMT", 6125L); // 6.125
            }
            writer.write(g);

            // 2행: 전부 null
            writer.write(factory.newGroup());
        }
    }

    /** 파일 C: 이름은 bytes/int, 타입은 BINARY DECIMAL (파일 A와 값·precision·scale 동일, 1행 값 / 2행 null) */
    private static void writeBinaryDecimalFile(final File file) throws IOException {
        final List<Type> cols = new ArrayList<>();
        cols.add(Types.optional(PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.decimalType(0, 38)).named("bytes"));
        cols.add(Types.optional(PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.decimalType(15, 38)).named("int"));
        final MessageType schema = new MessageType("test", cols);

        if (file.exists() && !file.delete()) {
            throw new IOException("기존 테스트 파일을 삭제할 수 없습니다: " + file.getAbsolutePath());
        }
        final SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalFileOutput(file))
                .withType(schema)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()) {

            // 1행: BINARY decimal은 unscaled 값의 big-endian 2의 보수 (길이 가변)
            final Group g = factory.newGroup();
            g.append("bytes", Binary.fromConstantByteArray(new BigInteger("20261011220351").toByteArray()));
            g.append("int", Binary.fromConstantByteArray(new BigInteger("123456789012345678").toByteArray())); // 123.456789012345678
            writer.write(g);

            // 2행: 전부 null
            writer.write(factory.newGroup());
        }
    }

    /** BigInteger(unscaled) → 고정 길이 big-endian 2의 보수 바이트 배열 (FIXED decimal용) */
    private static byte[] fixed(final BigInteger v, final int size) {
        final byte[] raw = v.toByteArray();
        final byte[] out = new byte[size];
        Arrays.fill(out, (byte) (v.signum() < 0 ? 0xFF : 0x00));
        System.arraycopy(raw, 0, out, size - raw.length, raw.length);
        return out;
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