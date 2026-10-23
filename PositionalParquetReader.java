package com.example.nifi.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.parquet.utils.ParquetConfig;
import org.apache.nifi.parquet.utils.ParquetUtils;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

@Tags({"parquet", "parse", "record", "row", "reader", "schema", "positional"})
@CapabilityDescription("Parquet 파일을 사용자 지정 Avro 스키마의 필드 순서대로 읽는다. "
        + "파일 컬럼명(대문자, 특수문자 등)과 관계없이 i번째 컬럼을 스키마의 i번째 필드명으로 읽는다. "
        + "스키마 필드가 더 많으면 초과 필드는 NULL, 더 적으면 초과 컬럼은 읽지 않는다. "
        + "타입은 파일 기준으로 읽으며, 사용자 타입은 Writer에서 적용한다.")
public class PositionalParquetReader extends AbstractControllerService implements RecordReaderFactory {

    static final PropertyDescriptor SCHEMA_TEXT = new PropertyDescriptor.Builder()
            .name("positional-schema-text")
            .displayName("Schema Text")
            .description("필드 순서와 이름을 지정하는 Avro 스키마(JSON). alias는 필요 없다. EL 사용 가능 (예: ${in_schema})")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES =
            List.of(SCHEMA_TEXT, ParquetUtils.AVRO_READ_COMPATIBILITY);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public RecordReader createRecordReader(final Map<String, String> variables, final InputStream in,
                                           final long inputLength, final ComponentLog logger) throws IOException {

        final String schemaText = getConfigurationContext().getProperty(SCHEMA_TEXT)
                .evaluateAttributeExpressions(variables).getValue();
        if (schemaText == null || schemaText.trim().isEmpty()) {
            throw new IllegalArgumentException("Schema Text 평가 결과가 비어 있습니다. FlowFile 속성 값을 확인하세요.");
        }

        // 기본 ParquetReader와 동일한 공통 설정 (Avro Read Compatibility, INT96 → fixed 등)
        final Configuration conf = new Configuration();
        final ParquetConfig parquetConfig = ParquetUtils.createParquetConfig(getConfigurationContext(), variables);
        ParquetUtils.applyCommonConfig(conf, parquetConfig);

        // 사용자 스키마 전달 → PositionalAvroReadSupport에서 사용
        conf.set(PositionalAvroReadSupport.USER_SCHEMA_KEY, schemaText);

        return new PositionalParquetRecordReader(in, inputLength, conf);
    }
}