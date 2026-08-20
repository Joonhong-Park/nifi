import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode

// avro.schema에서 bytes 타입으로 판별된 필드를 찾아
// out_schema의 동일 이름 필드를 avro.schema 필드 전체로 교체
// - 매칭 안 되는 필드는 무시

def flowFile = session.get()
if (!flowFile) return

try {
    def avroSchemaStr = flowFile.getAttribute('avro.schema')
    def outSchemaStr = flowFile.getAttribute('out_schema')
    if (avroSchemaStr == null) throw new IllegalStateException("avro.schema attribute가 없습니다")
    if (outSchemaStr == null) throw new IllegalStateException("out_schema attribute가 없습니다")

    def mapper = new ObjectMapper()
    def avroFields = (mapper.readTree(avroSchemaStr) as ObjectNode).get('fields') as ArrayNode
    def outRoot = mapper.readTree(outSchemaStr) as ObjectNode
    def outFields = outRoot.get('fields') as ArrayNode
    if (avroFields == null || outFields == null) {
        throw new IllegalStateException("fields 배열이 없습니다")
    }

    // avro.schema 필드 중 type이 ["null","bytes"]인 필드만 name 기준 맵으로 구성
    def bytesFieldByName = [:]
    avroFields.each { f ->
        def type = f.get('type')
        if (type?.isArray() && type.any { it.isTextual() && it.asText() == 'bytes' }) {
            bytesFieldByName[f.get('name').asText()] = f
        }
    }

    // out_schema 필드 중 이름이 일치하는 것만 avro.schema 필드로 통째 교체
    for (int i = 0; i < outFields.size(); i++) {
        def fName = outFields.get(i).get('name')?.asText()
        if (bytesFieldByName.containsKey(fName)) {
            outFields.set(i, bytesFieldByName[fName].deepCopy())
        }
    }

    flowFile = session.putAttribute(flowFile, 'out_schema', mapper.writeValueAsString(outRoot))
    session.transfer(flowFile, REL_SUCCESS)

} catch (Exception e) {
    log.error("out_schema binary 컬럼 처리 실패: {}", [e.getMessage()] as Object[], e)
    session.transfer(flowFile, REL_FAILURE)
}