package com.example.nifi.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 사용자 스키마의 필드 순서대로 Parquet 컬럼을 매핑하는 ReadSupport (parquet 1.13.1 기준).
 * <ul>
 *   <li>이름: 사용자 스키마 i번째 필드명 ← 파일 i번째 컬럼 (원본 컬럼명은 alias로 부여, read schema 내부 전용)</li>
 *   <li>타입: 1순위 파일 내장 Avro 스키마, 2순위 AvroSchemaConverter 변환 결과 (필요한 컬럼만, 임시 이름으로 변환)</li>
 *   <li>사용자 필드가 더 많으면 초과 필드는 NULL, 더 적으면 초과 파일 컬럼은 읽지 않음</li>
 *   <li>INT32/INT64 decimal 컬럼은 precision/scale을 기록 → RecordReader가 DECIMAL로 변환</li>
 * </ul>
 */
public class PositionalAvroReadSupport extends AvroReadSupport<GenericRecord> {

    /** Controller Service가 사용자 스키마 JSON을 전달할 때 쓰는 설정 키 */
    public static final String USER_SCHEMA_KEY = "positional.parquet.user.schema";

    /** 파일 footer에 내장된 Avro 스키마 키 (AvroReadSupport와 같은 우선순위: 신규 → 구버전) */
    private static final String EMBEDDED_SCHEMA_KEY = "parquet.avro.schema";
    private static final String OLD_EMBEDDED_SCHEMA_KEY = "avro.schema";

    /**
     * 변환기에 넘길 임시 컬럼명 접두어.
     * 변환기는 FIXED/INT96/group 컬럼의 Avro 타입 이름에 컬럼명을 쓰는데, 'bytes', 'int' 같은
     * Avro 기본 타입 이름이나 '$' 등은 타입 이름으로 쓸 수 없으므로 위치 기반 임시 이름을 사용한다.
     */
    private static final String TEMP_COLUMN_PREFIX = "pq_col_";

    /** INT32/INT64 decimal 컬럼: 사용자 필드명 → decimal 정보 (init 시 수집) */
    private final Map<String, LogicalTypeAnnotation.DecimalLogicalTypeAnnotation> integerDecimalFields = new HashMap<>();

    /** RecordReader가 첫 레코드를 읽은 뒤(= init 이후) decimal 컬럼 정보를 가져갈 때 사용 */
    public Map<String, LogicalTypeAnnotation.DecimalLogicalTypeAnnotation> getIntegerDecimalFields() {
        return Collections.unmodifiableMap(integerDecimalFields);
    }

    @Override
    public ReadSupport.ReadContext init(final Configuration conf,
                                        final Map<String, String> keyValueMetaData,
                                        final MessageType fileSchema) {
        integerDecimalFields.clear();

        final Schema userSchema = parseUserSchema(conf.get(USER_SCHEMA_KEY));
        final List<Schema.Field> userFields = userSchema.getFields();
        final List<Type> fileColumns = fileSchema.getFields();
        final int userFieldCount = userFields.size();
        final int mappedCount = Math.min(userFieldCount, fileColumns.size());

        // 1) projection: 파일의 앞 mappedCount개 컬럼만 읽음 (원본 이름 유지, 초과 컬럼은 절삭)
        final MessageType projection = new MessageType(fileSchema.getName(),
                new ArrayList<>(fileColumns.subList(0, mappedCount)));

        // 2) 파일 컬럼 타입 결정: 내장 Avro 스키마 우선, 나머지만 변환기 실행
        final Schema[] columnTypes = resolveColumnTypes(conf, fileSchema.getName(), fileColumns, mappedCount,
                parseEmbeddedSchema(keyValueMetaData));

        // 3) read schema 조립
        final List<Schema.Field> readFields = new ArrayList<>(userFieldCount);
        for (int i = 0; i < userFieldCount; i++) {
            final String name = userFields.get(i).name();
            if (i < mappedCount) {
                final Type column = fileColumns.get(i);
                readFields.add(buildMappedField(name, column, columnTypes[i]));
                recordIntegerDecimal(name, column);
            } else {
                // 파일에 없는 필드: 사용자 타입을 nullable로 바꾸고 기본값 null
                readFields.add(new Schema.Field(name, nullable(userFields.get(i).schema()), null,
                        Schema.Field.NULL_DEFAULT_VALUE));
            }
        }
        final Schema readSchema = Schema.createRecord(
                userSchema.getName(), null, userSchema.getNamespace(), false, readFields);

        // 4) read schema 지정 후 부모 init 수행 → projection으로 ReadContext 재구성
        setAvroReadSchema(conf, readSchema);
        final ReadSupport.ReadContext base = super.init(conf, keyValueMetaData, projection);
        return new ReadSupport.ReadContext(projection, base.getReadSupportMetadata());
    }

    /** 사용자 스키마 파싱 (record 타입만 허용) */
    private static Schema parseUserSchema(final String text) {
        if (text == null || text.trim().isEmpty()) {
            throw new IllegalArgumentException("사용자 스키마가 설정되지 않았습니다: " + USER_SCHEMA_KEY);
        }
        final Schema schema = new Schema.Parser().parse(text);
        if (schema.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException("사용자 스키마는 record 타입이어야 합니다: " + schema.getType());
        }
        return schema;
    }

    /** 내장 Avro 스키마 파싱. 없거나 파싱할 수 없으면(예: '$' 컬럼명) null → 변환기 결과 사용 */
    private static Schema parseEmbeddedSchema(final Map<String, String> keyValueMetaData) {
        if (keyValueMetaData == null) {
            return null;
        }
        String text = keyValueMetaData.get(EMBEDDED_SCHEMA_KEY);
        if (text == null) {
            text = keyValueMetaData.get(OLD_EMBEDDED_SCHEMA_KEY);
        }
        if (text == null) {
            return null;
        }
        try {
            final Schema schema = new Schema.Parser().parse(text);
            return schema.getType() == Schema.Type.RECORD ? schema : null;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 앞 mappedCount개 컬럼의 Avro 타입 결정.
     * 1순위: 내장 Avro 스키마에서 원본 컬럼명으로 찾은 타입
     * 2순위: 1순위로 얻지 못한 컬럼만 임시 이름(pq_col_i)으로 바꿔 변환기로 변환 후 임시 이름으로 조회
     *        (모두 1순위로 해결되면 변환기를 실행하지 않음)
     */
    private static Schema[] resolveColumnTypes(final Configuration conf, final String messageName,
                                               final List<Type> fileColumns, final int mappedCount,
                                               final Schema embeddedSchema) {
        final Schema[] types = new Schema[mappedCount];
        final List<Type> toConvert = new ArrayList<>();

        for (int i = 0; i < mappedCount; i++) {
            final Type column = fileColumns.get(i);
            final Schema.Field embeddedField = (embeddedSchema == null) ? null : embeddedSchema.getField(column.getName());
            if (embeddedField != null) {
                types[i] = embeddedField.schema();
            } else {
                toConvert.add(rename(column, TEMP_COLUMN_PREFIX + i));
            }
        }

        if (!toConvert.isEmpty()) {
            final Schema converted = new AvroSchemaConverter(conf).convert(new MessageType(messageName, toConvert));
            for (int i = 0; i < mappedCount; i++) {
                if (types[i] == null) {
                    types[i] = converted.getField(TEMP_COLUMN_PREFIX + i).schema();
                }
            }
        }
        return types;
    }

    /** 파일에 있는 컬럼의 read schema 필드: 이름=사용자 이름, alias=원본 컬럼명(다를 때만, parquet 컬럼 연결용) */
    private static Schema.Field buildMappedField(final String name, final Type column, final Schema type) {
        final Schema.Field field = new Schema.Field(name, type, null, (Object) null);
        if (!name.equals(column.getName())) {
            field.addAlias(column.getName());
        }
        return field;
    }

    /** INT32/INT64 + DECIMAL 컬럼이면 decimal 정보 기록 (Avro 스키마에는 scale이 없으므로 Parquet 스키마에서 가져옴) */
    private void recordIntegerDecimal(final String name, final Type column) {
        if (!column.isPrimitive()
                || !(column.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation)) {
            return;
        }
        final PrimitiveType.PrimitiveTypeName pt = column.asPrimitiveType().getPrimitiveTypeName();
        if (pt == PrimitiveType.PrimitiveTypeName.INT32 || pt == PrimitiveType.PrimitiveTypeName.INT64) {
            integerDecimalFields.put(name,
                    (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) column.getLogicalTypeAnnotation());
        }
    }

    /** 이름만 바꾸고 repetition, 물리 타입, 길이, 논리 타입(decimal 등), id는 유지 */
    private static Type rename(final Type type, final String newName) {
        final LogicalTypeAnnotation annotation = type.getLogicalTypeAnnotation();

        if (type.isPrimitive()) {
            final PrimitiveType p = type.asPrimitiveType();
            Types.PrimitiveBuilder<PrimitiveType> b = Types.primitive(p.getPrimitiveTypeName(), p.getRepetition());
            if (p.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
                b = b.length(p.getTypeLength());
            }
            if (annotation != null) {
                b = b.as(annotation);
            }
            if (p.getId() != null) {
                b = b.id(p.getId().intValue());
            }
            return b.named(newName);
        }

        final GroupType g = type.asGroupType();
        Types.GroupBuilder<GroupType> gb = Types.buildGroup(g.getRepetition());
        if (annotation != null) {
            gb = gb.as(annotation);
        }
        if (g.getId() != null) {
            gb = gb.id(g.getId().intValue());
        }
        return gb.addFields(g.getFields().toArray(new Type[0])).named(newName);
    }

    /** null을 첫 번째로 허용하는 union으로 보정 (기본값 null을 쓰려면 null이 첫 번째여야 함) */
    private static Schema nullable(final Schema schema) {
        if (schema.getType() == Schema.Type.NULL) {
            return schema;
        }
        final List<Schema> types = new ArrayList<>();
        types.add(Schema.create(Schema.Type.NULL));
        if (schema.getType() == Schema.Type.UNION) {
            for (Schema t : schema.getTypes()) {
                if (t.getType() != Schema.Type.NULL) {
                    types.add(t);
                }
            }
        } else {
            types.add(schema);
        }
        return Schema.createUnion(types);
    }
}