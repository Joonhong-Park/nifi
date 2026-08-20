import org.apache.nifi.processor.io.StreamCallback
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.PrimitiveType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode

// out_schema attribute 중 Parquet 실제 binary 컬럼(annotation 무관)만 골라
// type 표기를 ["null", "bytes"]로 교정하는 스크립트
// - 매칭 안 되는 컬럼(양쪽 어느 한쪽에만 존재)은 무시, binary type 교정만 담당
// - 임시 파일은 Content Repository와 별도 지정 경로에 생성

def flowFile = session.get()
if (!flowFile) return

try {
    // 1. out_schema attribute 확인
    def outSchemaStr = flowFile.getAttribute('out_schema')
    if (outSchemaStr == null) {
        throw new IllegalStateException("out_schema attribute가 존재하지 않습니다")
    }

    // 2. FlowFile content를 지정 경로에 임시 파일로 write (디렉터리 없으면 생성)
    def tempDir = new File('/appdata/nifi/data/tmp-parquet-schema')
    if (!tempDir.exists()) {
        tempDir.mkdirs()
    }
    if (!tempDir.exists()) {
        throw new IllegalStateException("임시 디렉터리 생성 실패: ${tempDir.getAbsolutePath()}")
    }
    def tempFile = File.createTempFile('nifi-parquet-schema-', '.parquet', tempDir)

    try {
        session.exportTo(flowFile, tempFile.toPath())

        // 3. Parquet footer에서 binary 컬럼명 수집 (annotation 무관, physical type 기준)
        def conf = new Configuration()
        def hadoopPath = new Path(tempFile.getAbsolutePath())
        def inputFile = HadoopInputFile.fromPath(hadoopPath, conf)

        def binaryColumnNames = [] as Set

        ParquetFileReader.open(inputFile).withCloseable { reader ->
            def messageType = reader.getFooter().getFileMetaData().getSchema()
            messageType.getFields().each { field ->
                if (field.isPrimitive()) {
                    def primType = field.asPrimitiveType()
                    if (primType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BINARY) {
                        binaryColumnNames.add(field.getName())
                    }
                }
            }
        }

        // 4. out_schema JSON 파싱 및 binary 컬럼만 교체
        def mapper = new ObjectMapper()
        def rootNode = mapper.readTree(outSchemaStr) as ObjectNode
        def fieldsNode = rootNode.get('fields') as ArrayNode

        if (fieldsNode == null) {
            throw new IllegalStateException("out_schema에 fields 배열이 없습니다")
        }

        fieldsNode.each { fieldNode ->
            def fName = fieldNode.get('name')?.asText()
            if (fName != null && binaryColumnNames.contains(fName)) {
                def typeArray = mapper.createArrayNode()
                typeArray.addNull()
                typeArray.add('bytes')
                (fieldNode as ObjectNode).set('type', typeArray)
            }
        }

        // 5. attribute 덮어쓰기 (content는 변경하지 않음)
        def updatedSchemaStr = mapper.writeValueAsString(rootNode)
        flowFile = session.putAttribute(flowFile, 'out_schema', updatedSchemaStr)

        session.transfer(flowFile, REL_SUCCESS)

    } finally {
        tempFile.delete()
    }

} catch (Exception e) {
    log.error("out_schema binary 컬럼 처리 실패: {}", [e.getMessage()] as Object[], e)
    session.transfer(flowFile, REL_FAILURE)
}