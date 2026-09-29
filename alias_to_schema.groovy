import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def flowFile = session.get()
if (!flowFile) return

final SKIP = 'file_insert_time'
final slurper = new JsonSlurper()

def alias(schema, origNames) {
    def fields = schema.fields
    int start = (fields[0]?.name == SKIP) ? 1 : 0
    int n = Math.min(fields.size() - start, origNames.size())
    fields.eachWithIndex { f, i ->
        if (i - start in 0..<n) f.aliases = [origNames[i - start]]
    }
    schema
}

try {
    def origNames = slurper.parseText(flowFile.getAttribute('avro.schema')).fields*.name

    def inSchema  = alias(slurper.parseText(flowFile.getAttribute('in_schema')), origNames)
    def outSchema = alias(slurper.parseText(flowFile.getAttribute('out_schema')), origNames)

    flowFile = session.putAttribute(flowFile, 'in_schema', JsonOutput.toJson(inSchema))
    flowFile = session.putAttribute(flowFile, 'out_schema', JsonOutput.toJson(outSchema))
    session.transfer(flowFile, REL_SUCCESS)

} catch (Exception e) {
    log.error("스키마 alias 매핑 실패: ${e.message}", e)
    session.transfer(flowFile, REL_FAILURE)
}