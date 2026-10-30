package com.example.nifi.parquet;

import ch.qos.logback.classic.Level;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.DecimalDataType;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupWriter;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.parquet.schema.LogicalTypeAnnotation.decimalType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;

/**
 * 진단용 (jar에 포함하지 않음, 확인 후 C:\nifi-dev\diag\로 이동).
 * 요구사항 전체(CLAUDE.md 9번 A01~E25)를 검증하고 항목별 "테스트 내용 : 실제 결과"를 한 줄씩 출력한다.
 * 실행 인자(선택): 테스트 파일을 만들 폴더 (기본: 프로젝트 폴더)
 */
public class Verify3 {

    /** 항목 ID → 테스트 내용(입력 데이터), 출력 순서 */
    private static final Map<String, String> DESC = new LinkedHashMap<>();

    static {
        DESC.put("A01", "a$b BINARY문자열 → a_b");
        DESC.put("A02", "UPPER_COL INT32 → upper_col");
        DESC.put("A03", "'col_1' BINARY문자열 → col_1");
        DESC.put("A04", "col/2 INT64 → col_2");
        DESC.put("A05", "col$3 DOUBLE → col_3");
        DESC.put("A06", "1col BOOLEAN → col1");
        DESC.put("A07", "AMT INT64 DEC(10,3) 6000/6125/-1500 → amt");
        DESC.put("A08", "AMT32 INT32 DEC(9,2) 1234 → amt32");
        DESC.put("A09", "bytes FIXED16 DEC(38,0) → bytes");
        DESC.put("A10", "int FIXED16 DEC(38,15) → int");
        DESC.put("A11", "F3 bytes/int BINARY DEC(38,0)/(38,15)");
        DESC.put("A12", "F6 bytes INT64 7 / int 문자열 s");
        DESC.put("A13", "RAW BINARY {1,2,3} → raw");
        DESC.put("A14", "F1 4번째 행 전부 null");
        DESC.put("B15", "사용자 12개(+extra long) / 파일 11개");
        DESC.put("B16", "사용자 3개 / 파일 11개");
        DESC.put("C17", "내장스키마 NOTE=string, BINARY → note");
        DESC.put("C18", "내장스키마 파싱불가(N$OTE) → note");
        DESC.put("D19", "avro.schema·NiFi 필드명/순서");
        DESC.put("D20", "alias·pq_col_ 없음");
        DESC.put("D21", "avro decimal 표시");
        DESC.put("D22", "avro raw 타입");
        DESC.put("D23", "record 이름 F1/F3/F6");
        DESC.put("D24", "avro.schema 재파싱");
        DESC.put("E25", "INT96 ts {1..12} 기본 Reader 비교");
    }

    /** 검증 결과: ID → 성공 여부 (같은 ID가 여러 번 검사되면 모두 성공해야 PASS) */
    private static final Map<String, Boolean> RESULTS = new LinkedHashMap<>();
    /** 검증 결과: ID → 실제 결과 (여러 번 검사되면 ", "로 연결) */
    private static final Map<String, String> ACTUAL = new LinkedHashMap<>();

    /** F1 사용자 필드명 (파일 컬럼 순서와 같음) */
    private static final String[] U1 = {"a_b", "upper_col", "col_1", "col_2", "col_3", "col1",
            "amt", "amt32", "bytes", "int", "raw"};

    public static void main(String[] args) {
        // 라이브러리 로그 끄기 (logback 기본 설정이 DEBUG)
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)).setLevel(Level.OFF);

        final File dir = new File(args.length > 0 ? args[0] : ".");
        final File f1 = new File(dir, "v3_f1.parquet");

        final Schema avro1 = caseF1(f1);
        final Schema avro3 = caseF3(new File(dir, "v3_f3.parquet"));
        final Schema avro6 = caseF6(new File(dir, "v3_f6.parquet"));
        final String n1 = avro1 == null ? "-" : avro1.getFullName();
        final String n3 = avro3 == null ? "-" : avro3.getFullName();
        final String n6 = avro6 == null ? "-" : avro6.getFullName();
        check("D23", avro1 != null && n1.equals(n3) && n1.equals(n6), n1 + " / " + n3 + " / " + n6);

        caseB15(f1);
        caseB16(f1);
        caseC17(new File(dir, "v3_f4.parquet"));
        caseC18(new File(dir, "v3_f5.parquet"));
        caseE25(new File(dir, "v3_f7.parquet"));

        // 항목별 출력
        int pass = 0;
        for (Map.Entry<String, String> d : DESC.entrySet()) {
            final String id = d.getKey();
            final Boolean ok = RESULTS.get(id);
            final String status = ok == null ? "SKIP" : (ok ? "PASS" : "FAIL");
            if (Boolean.TRUE.equals(ok)) {
                pass++;
            }
            System.out.println(status + " " + id + " " + d.getValue() + " : "
                    + (ok == null ? "미실행(케이스 오류)" : ACTUAL.get(id)));
        }
        // 케이스 전체 오류 (F1, F3, F6 등)
        for (Map.Entry<String, Boolean> e : RESULTS.entrySet()) {
            if (!DESC.containsKey(e.getKey())) {
                System.out.println("FAIL " + e.getKey() + " : " + ACTUAL.get(e.getKey()));
            }
        }
        System.out.println("결과: PASS " + pass + " / FAIL " + (DESC.size() - pass) + " (기대 PASS 25)");
    }

    // ===================================================================== 케이스

    /** F1: 특수문자·대문자·숫자 시작 컬럼명, INT64/INT32/FIXED decimal, 표시 없는 BINARY, null 행 */
    private static Schema caseF1(final File f) {
        try {
            final MessageType s = new MessageType("test",
                    Types.optional(BINARY).as(stringType()).named("a$b"),
                    Types.optional(INT32).named("UPPER_COL"),
                    Types.optional(BINARY).as(stringType()).named("'col_1'"),
                    Types.optional(INT64).named("col/2"),
                    Types.optional(DOUBLE).named("col$3"),
                    Types.optional(BOOLEAN).named("1col"),
                    Types.optional(INT64).as(decimalType(3, 10)).named("AMT"),
                    Types.optional(INT32).as(decimalType(2, 9)).named("AMT32"),
                    Types.optional(FIXED_LEN_BYTE_ARRAY).length(16).as(decimalType(0, 38)).named("bytes"),
                    Types.optional(FIXED_LEN_BYTE_ARRAY).length(16).as(decimalType(15, 38)).named("int"),
                    Types.optional(BINARY).named("RAW"));
            final SimpleGroupFactory gf = new SimpleGroupFactory(s);
            final List<Group> rows = new ArrayList<>();
            for (long amt : new long[]{6000L, 6125L, -1500L}) {   // 6.000, 6.125, -1.500
                rows.add(gf.newGroup()
                        .append("a$b", "hello").append("UPPER_COL", 42).append("'col_1'", "x")
                        .append("col/2", 9000000000L).append("col$3", 1.5d).append("1col", true)
                        .append("AMT", amt).append("AMT32", 1234)
                        .append("bytes", fixed("20261011220351")).append("int", fixed("123456789012345678"))
                        .append("RAW", Binary.fromConstantByteArray(new byte[]{1, 2, 3})));
            }
            rows.add(gf.newGroup());   // 전부 null
            write(f, s, Collections.emptyMap(), rows);

            final ReadResult r = readCustom(f, recordJson(fields(U1)));
            final RecordSchema rs = r.schema;
            final Record r0 = r.rows.get(0);
            check("A01", isType(rs, "a_b", RecordFieldType.STRING) && strEq(r0.getValue("a_b"), "hello"),
                    tv(rs, r0, "a_b"));
            check("A02", isType(rs, "upper_col", RecordFieldType.INT) && Objects.equals(r0.getValue("upper_col"), 42),
                    tv(rs, r0, "upper_col"));
            check("A03", isType(rs, "col_1", RecordFieldType.STRING) && strEq(r0.getValue("col_1"), "x"),
                    tv(rs, r0, "col_1"));
            check("A04", isType(rs, "col_2", RecordFieldType.LONG) && Objects.equals(r0.getValue("col_2"), 9000000000L),
                    tv(rs, r0, "col_2"));
            check("A05", isType(rs, "col_3", RecordFieldType.DOUBLE) && Objects.equals(r0.getValue("col_3"), 1.5d),
                    tv(rs, r0, "col_3"));
            check("A06", isType(rs, "col1", RecordFieldType.BOOLEAN) && Objects.equals(r0.getValue("col1"), true),
                    tv(rs, r0, "col1"));
            check("A07", isDecimal(rs, "amt", 10, 3) && r.rows.size() == 4
                            && decEq(r.rows.get(0).getValue("amt"), "6.000")
                            && decEq(r.rows.get(1).getValue("amt"), "6.125")
                            && decEq(r.rows.get(2).getValue("amt"), "-1.500"),
                    typeOf(rs, "amt") + " " + v(r.rows.get(0).getValue("amt")) + ", "
                            + v(r.rows.get(1).getValue("amt")) + ", " + v(r.rows.get(2).getValue("amt")));
            check("A08", isDecimal(rs, "amt32", 9, 2) && decEq(r0.getValue("amt32"), "12.34"),
                    tv(rs, r0, "amt32"));
            check("A09", isDecimal(rs, "bytes", 38, 0) && decEq(r0.getValue("bytes"), "20261011220351"),
                    tv(rs, r0, "bytes"));
            check("A10", isDecimal(rs, "int", 38, 15) && decEq(r0.getValue("int"), "123.456789012345678"),
                    tv(rs, r0, "int"));
            check("A13", Arrays.equals(bytesOf(r0.getValue("raw")), new byte[]{1, 2, 3}),
                    tv(rs, r0, "raw"));
            check("A14", r.rows.size() == 4 && allNull(r.rows.get(3), U1),
                    "행 " + r.rows.size() + "개, 4번째 행 값 있는 필드 " + nonNullCount(r.rows.get(r.rows.size() - 1), U1) + "개");

            checkAvroCommon("F1", r, U1);
            check("D21", avroDecimal(r.avro, "amt", 10, 3) && avroDecimal(r.avro, "amt32", 9, 2)
                            && avroDecimal(r.avro, "bytes", 38, 0) && avroDecimal(r.avro, "int", 38, 15),
                    "F1 amt=" + at(r.avro, "amt") + " amt32=" + at(r.avro, "amt32")
                            + " bytes=" + at(r.avro, "bytes") + " int=" + at(r.avro, "int"));
            final Schema raw = nonNull(r.avro.getField("raw").schema());
            check("D22", raw.getType() == Schema.Type.BYTES && raw.getLogicalType() == null,
                    "raw=" + r.avro.getField("raw").schema());
            return r.avro;
        } catch (Throwable t) {
            error("F1", t);
            return null;
        }
    }

    /** F3: bytes/int 이름의 BINARY decimal (INT32/INT64 decimal 없는 파일) */
    private static Schema caseF3(final File f) {
        try {
            final MessageType s = new MessageType("test",
                    Types.optional(BINARY).as(decimalType(0, 38)).named("bytes"),
                    Types.optional(BINARY).as(decimalType(15, 38)).named("int"));
            final SimpleGroupFactory gf = new SimpleGroupFactory(s);
            write(f, s, Collections.emptyMap(), Arrays.asList(
                    gf.newGroup()
                            .append("bytes", Binary.fromConstantByteArray(new BigInteger("20261011220351").toByteArray()))
                            .append("int", Binary.fromConstantByteArray(new BigInteger("123456789012345678").toByteArray())),
                    gf.newGroup()));

            final String[] names = {"bytes", "int"};
            final ReadResult r = readCustom(f, recordJson(fields(names)));
            final Record r0 = r.rows.get(0);
            check("A11", isDecimal(r.schema, "bytes", 38, 0) && isDecimal(r.schema, "int", 38, 15)
                            && decEq(r0.getValue("bytes"), "20261011220351")
                            && decEq(r0.getValue("int"), "123.456789012345678")
                            && allNull(r.rows.get(1), names),
                    "bytes " + tv(r.schema, r0, "bytes") + ", int " + tv(r.schema, r0, "int")
                            + ", null행 " + (allNull(r.rows.get(1), names) ? "null" : "값있음"));
            checkAvroCommon("F3", r, names);
            check("D21", avroDecimal(r.avro, "bytes", 38, 0) && avroDecimal(r.avro, "int", 38, 15),
                    "F3 bytes=" + at(r.avro, "bytes") + " int=" + at(r.avro, "int"));
            return r.avro;
        } catch (Throwable t) {
            error("F3", t);
            return null;
        }
    }

    /** F6: bytes/int 이름의 INT64 / 문자열 컬럼 */
    private static Schema caseF6(final File f) {
        try {
            final MessageType s = new MessageType("test",
                    Types.optional(INT64).named("bytes"),
                    Types.optional(BINARY).as(stringType()).named("int"));
            final SimpleGroupFactory gf = new SimpleGroupFactory(s);
            write(f, s, Collections.emptyMap(), Arrays.asList(
                    gf.newGroup().append("bytes", 7L).append("int", "s"),
                    gf.newGroup()));

            final String[] names = {"bytes", "int"};
            final ReadResult r = readCustom(f, recordJson(fields(names)));
            final Record r0 = r.rows.get(0);
            check("A12", isType(r.schema, "bytes", RecordFieldType.LONG) && Objects.equals(r0.getValue("bytes"), 7L)
                            && isType(r.schema, "int", RecordFieldType.STRING) && strEq(r0.getValue("int"), "s"),
                    "bytes " + tv(r.schema, r0, "bytes") + ", int " + tv(r.schema, r0, "int"));
            checkAvroCommon("F6", r, names);
            return r.avro;
        } catch (Throwable t) {
            error("F6", t);
            return null;
        }
    }

    /** B15: 사용자 필드가 더 많음 → 초과 필드 null, 사용자 타입(long) */
    private static void caseB15(final File f1) {
        try {
            final List<String> fieldJsons = fields(U1);
            fieldJsons.add("{\"name\":\"extra\",\"type\":\"long\"}");
            final String[] names = Arrays.copyOf(U1, U1.length + 1);
            names[U1.length] = "extra";

            final ReadResult r = readCustom(f1, recordJson(fieldJsons));
            int extraNull = 0;
            for (Record rec : r.rows) {
                if (rec.getValue("extra") == null) {
                    extraNull++;
                }
            }
            final Record r0 = r.rows.get(0);
            check("B15", r.schema.getFieldCount() == 12 && extraNull == r.rows.size()
                            && isType(r.schema, "extra", RecordFieldType.LONG)
                            && nonNull(r.avro.getField("extra").schema()).getType() == Schema.Type.LONG
                            && decEq(r0.getValue("amt"), "6.000")
                            && Arrays.equals(bytesOf(r0.getValue("raw")), new byte[]{1, 2, 3}),
                    "필드 " + r.schema.getFieldCount() + "개, extra " + typeOf(r.schema, "extra")
                            + " null " + extraNull + "/" + r.rows.size()
                            + ", amt " + v(r0.getValue("amt")) + ", raw " + v(r0.getValue("raw")));
            checkAvroCommon("B15", r, names);
        } catch (Throwable t) {
            error("B15", t);
        }
    }

    /** B16: 사용자 필드가 더 적음 → 앞 3개만 */
    private static void caseB16(final File f1) {
        try {
            final String[] names = {"a_b", "upper_col", "col_1"};
            final ReadResult r = readCustom(f1, recordJson(fields(names)));
            final Record r0 = r.rows.get(0);
            check("B16", r.schema.getFieldCount() == 3 && r.rows.size() == 4
                            && strEq(r0.getValue("a_b"), "hello")
                            && Objects.equals(r0.getValue("upper_col"), 42)
                            && strEq(r0.getValue("col_1"), "x"),
                    "필드 " + r.schema.getFieldCount() + "개, 행 " + r.rows.size() + "개, "
                            + v(r0.getValue("a_b")) + ", " + v(r0.getValue("upper_col")) + ", " + v(r0.getValue("col_1")));
            checkAvroCommon("B16", r, names);
        } catch (Throwable t) {
            error("B16", t);
        }
    }

    /** C17: 내장 Avro 스키마 사용 (NOTE를 string으로 선언 → 변환기라면 bytes) */
    private static void caseC17(final File f) {
        try {
            final MessageType s = new MessageType("test",
                    Types.optional(INT32).named("ID"),
                    Types.optional(BINARY).named("NOTE"));
            final Map<String, String> meta = Collections.singletonMap("parquet.avro.schema",
                    "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
                            + "{\"name\":\"ID\",\"type\":[\"null\",\"int\"],\"default\":null},"
                            + "{\"name\":\"NOTE\",\"type\":[\"null\",\"string\"],\"default\":null}]}");
            final SimpleGroupFactory gf = new SimpleGroupFactory(s);
            write(f, s, meta, Collections.singletonList(gf.newGroup().append("ID", 1)
                    .append("NOTE", Binary.fromString("memo"))));

            final String[] names = {"id", "note"};
            final ReadResult r = readCustom(f, recordJson(fields(names)));
            final Record r0 = r.rows.get(0);
            check("C17", isType(r.schema, "note", RecordFieldType.STRING) && strEq(r0.getValue("note"), "memo"),
                    tv(r.schema, r0, "note"));
            checkAvroCommon("C17", r, names);
        } catch (Throwable t) {
            error("C17", t);
        }
    }

    /** C18: 내장 Avro 스키마 파싱 불가('$') → 변환기 경로 */
    private static void caseC18(final File f) {
        try {
            final MessageType s = new MessageType("test",
                    Types.optional(INT32).named("ID"),
                    Types.optional(BINARY).named("N$OTE"));
            final Map<String, String> meta = Collections.singletonMap("parquet.avro.schema",
                    "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
                            + "{\"name\":\"ID\",\"type\":[\"null\",\"int\"],\"default\":null},"
                            + "{\"name\":\"N$OTE\",\"type\":[\"null\",\"string\"],\"default\":null}]}");
            final SimpleGroupFactory gf = new SimpleGroupFactory(s);
            write(f, s, meta, Collections.singletonList(gf.newGroup().append("ID", 1)
                    .append("N$OTE", Binary.fromString("memo"))));

            final String[] names = {"id", "note"};
            final ReadResult r = readCustom(f, recordJson(fields(names)));
            final Record r0 = r.rows.get(0);
            check("C18", isType(r.schema, "note", RecordFieldType.ARRAY)
                            && Arrays.equals(bytesOf(r0.getValue("note")), "memo".getBytes(StandardCharsets.UTF_8)),
                    tv(r.schema, r0, "note"));
            checkAvroCommon("C18", r, names);
        } catch (Throwable t) {
            error("C18", t);
        }
    }

    /** E25: INT96 값이 기본 ParquetReader와 같은지 */
    private static void caseE25(final File f) {
        try {
            final MessageType s = new MessageType("test",
                    Types.optional(INT96).named("ts"),
                    Types.optional(INT32).named("id"));
            final byte[] ts = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
            final SimpleGroupFactory gf = new SimpleGroupFactory(s);
            write(f, s, Collections.emptyMap(), Arrays.asList(
                    gf.newGroup().append("ts", Binary.fromConstantByteArray(ts)).append("id", 5),
                    gf.newGroup()));

            final String[] names = {"ts", "id"};
            final ReadResult custom = readCustom(f, recordJson(fields(names)));
            final ReadResult base = readBase(f);
            final Record c0 = custom.rows.get(0);
            final Record b0 = base.rows.get(0);
            check("E25", Arrays.equals(bytesOf(c0.getValue("ts")), ts)
                            && Arrays.equals(bytesOf(b0.getValue("ts")), ts)
                            && Objects.equals(c0.getValue("id"), b0.getValue("id")),
                    "커스텀 ts " + v(c0.getValue("ts")) + " id " + v(c0.getValue("id"))
                            + " / 기본 ts " + v(b0.getValue("ts")) + " id " + v(b0.getValue("id")));
            checkAvroCommon("E25", custom, names);
        } catch (Throwable t) {
            error("E25", t);
        }
    }

    // ===================================================================== 공통 검사

    /** D19 필드명·순서, D20 alias/내부 이름 없음, D24 재파싱 */
    private static void checkAvroCommon(final String label, final ReadResult r, final String[] names) {
        final List<String> avroNames = new ArrayList<>();
        for (Schema.Field fld : r.avro.getFields()) {
            avroNames.add(fld.name());
        }
        final boolean namesOk = avroNames.equals(Arrays.asList(names)) && r.schema.getFieldNames().equals(Arrays.asList(names));
        check("D19", namesOk, label + (namesOk ? " " + names.length + "개 일치" : " 불일치 avro=" + avroNames
                + " nifi=" + r.schema.getFieldNames()));

        int nifiAlias = 0;
        for (RecordField fld : r.schema.getFields()) {
            nifiAlias += fld.getAliases().size();
        }
        final String text = r.avro.toString();
        final boolean hasAliases = text.contains("\"aliases\"");
        final boolean hasTemp = text.contains("pq_col_");
        check("D20", nifiAlias == 0 && !hasAliases && !hasTemp, label
                + (nifiAlias == 0 && !hasAliases && !hasTemp ? " 없음"
                : " 있음(NiFi alias " + nifiAlias + ", avro aliases " + hasAliases + ", pq_col_ " + hasTemp + ")"));

        String reparse;
        try {
            new Schema.Parser().parse(text);
            reparse = null;
        } catch (Exception e) {
            reparse = e.getClass().getSimpleName();
        }
        check("D24", reparse == null, label + (reparse == null ? " OK" : " 실패 " + reparse));
    }

    private static boolean isType(final RecordSchema s, final String name, final RecordFieldType type) {
        return s.getDataType(name).map(t -> t.getFieldType() == type).orElse(false);
    }

    private static boolean isDecimal(final RecordSchema s, final String name, final int precision, final int scale) {
        final DataType t = s.getDataType(name).orElse(null);
        if (!(t instanceof DecimalDataType)) {
            return false;
        }
        final DecimalDataType d = (DecimalDataType) t;
        return d.getPrecision() == precision && d.getScale() == scale;
    }

    private static boolean avroDecimal(final Schema avro, final String name, final int precision, final int scale) {
        final Schema t = nonNull(avro.getField(name).schema());
        if (t.getType() != Schema.Type.BYTES || !(t.getLogicalType() instanceof LogicalTypes.Decimal)) {
            return false;
        }
        final LogicalTypes.Decimal d = (LogicalTypes.Decimal) t.getLogicalType();
        return d.getPrecision() == precision && d.getScale() == scale;
    }

    private static Schema nonNull(final Schema s) {
        if (s.getType() == Schema.Type.UNION) {
            for (Schema t : s.getTypes()) {
                if (t.getType() != Schema.Type.NULL) {
                    return t;
                }
            }
        }
        return s;
    }

    private static boolean decEq(final Object v, final String expected) {
        return v instanceof BigDecimal && ((BigDecimal) v).compareTo(new BigDecimal(expected)) == 0;
    }

    private static boolean strEq(final Object v, final String expected) {
        return v != null && expected.equals(v.toString());
    }

    private static boolean allNull(final Record r, final String[] names) {
        return nonNullCount(r, names) == 0;
    }

    private static int nonNullCount(final Record r, final String[] names) {
        int n = 0;
        for (String name : names) {
            if (r.getValue(name) != null) {
                n++;
            }
        }
        return n;
    }

    /** NiFi 바이트 값(Object[] of Byte, byte[], ByteBuffer) → byte[] */
    private static byte[] bytesOf(final Object v) {
        if (v instanceof byte[]) {
            return (byte[]) v;
        }
        if (v instanceof Object[]) {
            final Object[] a = (Object[]) v;
            final byte[] b = new byte[a.length];
            for (int i = 0; i < a.length; i++) {
                b[i] = ((Number) a[i]).byteValue();
            }
            return b;
        }
        if (v instanceof ByteBuffer) {
            final ByteBuffer bb = ((ByteBuffer) v).duplicate();
            final byte[] b = new byte[bb.remaining()];
            bb.get(b);
            return b;
        }
        return null;
    }

    // ===================================================================== 출력 형식

    /** NiFi 타입 표시 (예: STRING, DECIMAL[10,3], ARRAY[BYTE]) */
    private static String typeOf(final RecordSchema s, final String name) {
        return s.getDataType(name).map(DataType::toString).orElse("필드없음");
    }

    /** "NiFi타입 값" */
    private static String tv(final RecordSchema s, final Record r, final String name) {
        return typeOf(s, name) + " " + v(r.getValue(name));
    }

    /** 값 표시: 문자열은 따옴표, decimal은 지수 표기 없이, 바이트는 [1, 2, 3] */
    private static String v(final Object o) {
        if (o == null) {
            return "null";
        }
        if (o instanceof BigDecimal) {
            return ((BigDecimal) o).toPlainString();
        }
        if (o instanceof CharSequence) {
            return "\"" + o + "\"";
        }
        final byte[] b = bytesOf(o);
        if (b != null) {
            return Arrays.toString(b);
        }
        return o + "(" + o.getClass().getSimpleName() + ")";
    }

    /** avro.schema 필드 타입 표시 (예: bytes:decimal(10,3), long) */
    private static String at(final Schema avro, final String name) {
        final Schema.Field fld = avro.getField(name);
        if (fld == null) {
            return "필드없음";
        }
        final Schema t = nonNull(fld.schema());
        if (t.getLogicalType() instanceof LogicalTypes.Decimal) {
            final LogicalTypes.Decimal d = (LogicalTypes.Decimal) t.getLogicalType();
            return t.getType().getName() + ":decimal(" + d.getPrecision() + "," + d.getScale() + ")";
        }
        return t.getType().getName();
    }

    private static void check(final String id, final boolean ok, final String actual) {
        RESULTS.merge(id, ok, Boolean::logicalAnd);
        ACTUAL.merge(id, actual, (a, b) -> a + ", " + b);
    }

    /** 케이스 전체 실패: 케이스 ID로 근본 원인을 짧게 기록 (하위 항목은 SKIP으로 표시됨) */
    private static void error(final String caseId, final Throwable t) {
        Throwable c = t;
        while (c.getCause() != null && c.getCause() != c) {
            c = c.getCause();
        }
        String msg = String.valueOf(c.getMessage());
        if (msg.length() > 60) {
            msg = msg.substring(0, 60) + "...";
        }
        check(caseId, false, c.getClass().getSimpleName() + ": " + msg);
    }

    // ===================================================================== 읽기

    private static final class ReadResult {
        final RecordSchema schema;
        final Schema avro;          // ExtractRecordSchema와 같은 방식으로 만든 avro.schema
        final List<Record> rows;

        ReadResult(final RecordSchema schema, final List<Record> rows) {
            this.schema = schema;
            this.avro = AvroTypeUtil.extractAvroSchema(schema);
            this.rows = rows;
        }
    }

    /** PositionalParquetReader가 만드는 설정과 동일 */
    private static Configuration baseConf() {
        final Configuration conf = new Configuration();
        conf.setBoolean("parquet.avro.compatible", true);
        conf.setBoolean("parquet.avro.readInt96AsFixed", true);
        return conf;
    }

    private static ReadResult readCustom(final File f, final String userSchema) throws Exception {
        final Configuration conf = baseConf();
        conf.set(PositionalAvroReadSupport.USER_SCHEMA_KEY, userSchema);
        try (RecordReader reader = new PositionalParquetRecordReader(open(f), f.length(), conf)) {
            return readAll(reader);
        }
    }

    private static ReadResult readBase(final File f) throws Exception {
        try (RecordReader reader = new org.apache.nifi.parquet.record.ParquetRecordReader(open(f), f.length(), baseConf())) {
            return readAll(reader);
        }
    }

    private static ReadResult readAll(final RecordReader reader) throws Exception {
        final List<Record> rows = new ArrayList<>();
        Record rec;
        while ((rec = reader.nextRecord(true, false)) != null) {
            rows.add(rec);
        }
        return new ReadResult(reader.getSchema(), rows);
    }

    /** mark/reset 지원 스트림 (NifiParquetInputFile 요구사항, 테스트 파일은 수 KB) */
    private static InputStream open(final File f) throws IOException {
        return new BufferedInputStream(new FileInputStream(f), (int) f.length() + 1);
    }

    // ===================================================================== 스키마 JSON

    private static List<String> fields(final String... names) {
        final List<String> list = new ArrayList<>();
        for (String n : names) {
            list.add("{\"name\":\"" + n + "\",\"type\":[\"null\",\"string\"],\"default\":null}");
        }
        return list;
    }

    private static String recordJson(final List<String> fieldJsons) {
        return "{\"type\":\"record\",\"name\":\"in\",\"fields\":[" + String.join(",", fieldJsons) + "]}";
    }

    // ===================================================================== 쓰기

    private static void write(final File f, final MessageType schema, final Map<String, String> meta,
                              final List<Group> rows) throws IOException {
        if (f.exists() && !f.delete()) {
            throw new IOException("기존 테스트 파일을 삭제할 수 없습니다: " + f.getAbsolutePath());
        }
        try (ParquetWriter<Group> w = new GroupBuilder(new LocalFileOutput(f), schema, meta)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()) {
            for (Group g : rows) {
                w.write(g);
            }
        }
    }

    /** unscaled 값 → FIXED(16) big-endian 2의 보수 */
    private static Binary fixed(final String unscaled) {
        final BigInteger v = new BigInteger(unscaled);
        final byte[] raw = v.toByteArray();
        final byte[] out = new byte[16];
        Arrays.fill(out, (byte) (v.signum() < 0 ? 0xFF : 0x00));
        System.arraycopy(raw, 0, out, 16 - raw.length, raw.length);
        return Binary.fromConstantByteArray(out);
    }

    /** Group 쓰기용 Builder (footer key-value 메타데이터 지정 가능) */
    private static final class GroupBuilder extends ParquetWriter.Builder<Group, GroupBuilder> {
        private final MessageType type;
        private final Map<String, String> meta;

        GroupBuilder(final OutputFile file, final MessageType type, final Map<String, String> meta) {
            super(file);
            this.type = type;
            this.meta = meta;
        }

        @Override
        protected GroupBuilder self() {
            return this;
        }

        @Override
        protected WriteSupport<Group> getWriteSupport(final Configuration conf) {
            return new MetaGroupWriteSupport(type, meta);
        }
    }

    /** GroupWriteSupport 대체 (해당 생성자가 package-private): 스키마 + footer 메타데이터 지정 */
    private static final class MetaGroupWriteSupport extends WriteSupport<Group> {
        private final MessageType type;
        private final Map<String, String> meta;
        private GroupWriter groupWriter;

        MetaGroupWriteSupport(final MessageType type, final Map<String, String> meta) {
            this.type = type;
            this.meta = meta;
        }

        @Override
        public WriteContext init(final Configuration configuration) {
            return new WriteContext(type, meta);
        }

        @Override
        public void prepareForWrite(final RecordConsumer recordConsumer) {
            this.groupWriter = new GroupWriter(recordConsumer, type);
        }

        @Override
        public void write(final Group record) {
            groupWriter.write(record);
        }
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