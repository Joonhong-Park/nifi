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
 * - 이름: 사용자 스키마 i번째 필드명 (파일 컬럼명은 alias로 부여)
 * - 타입: 1순위 파일 내장 Avro 스키마의 필드 타입, 2순위 AvroSchemaConverter 변환 결과
 * - INT32/INT64 decimal 컬럼은 precision/scale을 기록 → RecordReader가 DECIMAL로 변환
 * - 사용자 필드가 더 많으면 초과 필드는 NULL, 더 적으면 초과 파일 컬럼은 읽지 않음
 */
public class PositionalAvroReadSupport extends AvroReadSupport<GenericRecord> {

    /** Reader가 사용자 스키마 JSON을 전달할 때 쓰는 설정 키 */
    public static final String USER_SCHEMA_KEY = "positional.parquet.user.schema";

    /** parquet-avro 내부의 read schema 설정 키 (AvroReadSupport.AVRO_READ_SCHEMA와 동일) */
    private static final String AVRO_READ_SCHEMA_KEY = "parquet.avro.read.schema";

    /** 파일 footer에 내장된 Avro 스키마 키 (신규 / 구버전, AvroReadSupport와 동일한 우선순위) */
    private static final String EMBEDDED_SCHEMA_KEY = "parquet.avro.schema";
    private static final String OLD_EMBEDDED_SCHEMA_KEY = "avro.schema";

    /** INT32/INT64 decimal 컬럼: 사용자 필드명 → decimal 정보 (init 시 수집) */
    private final Map<String, LogicalTypeAnnotation.DecimalLogicalTypeAnnotation> integerDecimalFields = new HashMap<>();

    public PositionalAvroReadSupport() {
        super();
    }

    /** RecordReader가 첫 레코드를 읽은 뒤 decimal 컬럼 정보를 가져갈 때 사용 */
    public Map<String, LogicalTypeAnnotation.DecimalLogicalTypeAnnotation> getIntegerDecimalFields() {
        return Collections.unmodifiableMap(integerDecimalFields);
    }

    @Override
    public ReadSupport.ReadContext init(final Configuration conf,
                                        final Map<String, String> keyValueMetaData,
                                        final MessageType fileSchema) {

        integerDecimalFields.clear();

        final String userSchemaText = conf.get(USER_SCHEMA_KEY);
        if (userSchemaText == null || userSchemaText.trim().isEmpty()) {
            throw new IllegalArgumentException("사용자 스키마가 설정되지 않았습니다: " + USER_SCHEMA_KEY);
        }

        final Schema userSchema = new Schema.Parser().parse(userSchemaText);
        if (userSchema.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException("사용자 스키마는 record 타입이어야 합니다: " + userSchema.getType());
        }

        final List<Schema.Field> userFields = userSchema.getFields();
        final List<Type> fileColumns = fileSchema.getFields();
        final int n = userFields.size();
        final int k = Math.min(n, fileColumns.size());

        // 0) 파일 내장 Avro 스키마
        final Schema embeddedSchema = parseEmbeddedSchema(keyValueMetaData);

        // 1) projection: 파일의 앞 k개 컬럼만 읽음 (원본 이름 유지)
        final MessageType projection = new MessageType(fileSchema.getName(), new ArrayList<>(fileColumns.subList(0, k)));

        // 2) 이름만 사용자 이름으로 바꾼 사본을 변환기로 Avro 타입 변환 (2순위용)
        Schema fileTyped = null;
        if (k > 0) {
            final List<Type> renamed = new ArrayList<>(k);
            for (int i = 0; i < k; i++) {
                renamed.add(rename(fileColumns.get(i), userFields.get(i).name()));
            }
            fileTyped = new AvroSchemaConverter(conf).convert(new MessageType(fileSchema.getName(), renamed));
        }

        // 3) read schema 조립
        final List<Schema.Field> readFields = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            final String name = userFields.get(i).name();
            if (i < k) {
                final Type column = fileColumns.get(i);
                final String originalName = column.getName();

                // 1순위: 내장 스키마의 타입, 2순위: 변환기 결과
                Schema typeFromFile = null;
                if (embeddedSchema != null) {
                    final Schema.Field embeddedField = embeddedSchema.getField(originalName);
                    if (embeddedField != null) {
                        typeFromFile = embeddedField.schema();
                    }
                }
                if (typeFromFile == null) {
                    typeFromFile = fileTyped.getField(name).schema();
                }

                final Schema.Field field = new Schema.Field(name, typeFromFile, null, (Object) null);
                if (!name.equals(originalName)) {
                    field.addAlias(originalName);
                }
                readFields.add(field);

                // INT32/INT64 decimal 컬럼 기록 (Avro 스키마에는 scale이 없으므로 Parquet 스키마에서 가져옴)
                if (column.isPrimitive()
                        && column.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
                    final PrimitiveType.PrimitiveTypeName pt = column.asPrimitiveType().getPrimitiveTypeName();
                    if (pt == PrimitiveType.PrimitiveTypeName.INT32 || pt == PrimitiveType.PrimitiveTypeName.INT64) {
                        integerDecimalFields.put(name,
                                (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) column.getLogicalTypeAnnotation());
                    }
                }
            } else {
                // 파일에 없는 필드: 사용자 타입을 nullable로 바꾸고 기본값 null
                readFields.add(new Schema.Field(name, nullable(userFields.get(i).schema()), null,
                        Schema.Field.NULL_DEFAULT_VALUE));
            }
        }

        final Schema readSchema = Schema.createRecord(
                userSchema.getName(), null, userSchema.getNamespace(), false, readFields);

        // 4) read schema 지정 후 기본 init 수행 → projection으로 ReadContext 재구성
        conf.set(AVRO_READ_SCHEMA_KEY, readSchema.toString());
        final ReadSupport.ReadContext base = super.init(conf, keyValueMetaData, projection);
        return new ReadSupport.ReadContext(projection, base.getReadSupportMetadata());
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

    /** 이름만 바꾸고 repetition, 물리 타입, 길이, 논리 타입(decimal 등)은 유지 */
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