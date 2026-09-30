import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def flowFile = session.get()
if (flowFile == null) {
    return
}

def parser = new JsonSlurper()
def skipName = 'file_insert_time'

def addAlias(schema, origNames, skipName) {
    def fields = schema.fields

    def start = 0
    if (fields.size() > 0 && fields[0].name == skipName) {
        start = 1
    }

    def count = Math.min(fields.size() - start, origNames.size())

    for (int i = 0; i < count; i++) {
        def field = fields[start + i]
        def origName = origNames[i]

        if (field.name == origName) {
            // 이름이 같으면 alias 자체를 넣지 않음 (있었다면 제거)
            field.remove('aliases')
        } else {
            field.aliases = [origName]
        }
    }

    return schema
}

try {
    def origSchema = parser.parseText(flowFile.getAttribute('avro.schema'))
    def origNames = []
    origSchema.fields.each { f -> origNames.add(f.name) }

    def inSchema = parser.parseText(flowFile.getAttribute('in_schema'))
    def outSchema = parser.parseText(flowFile.getAttribute('out_schema'))

    addAlias(inSchema, origNames, skipName)
    addAlias(outSchema, origNames, skipName)

    flowFile = session.putAttribute(flowFile, 'in_schema', JsonOutput.toJson(inSchema))
    flowFile = session.putAttribute(flowFile, 'out_schema', JsonOutput.toJson(outSchema))

    session.transfer(flowFile, REL_SUCCESS)

} catch (Exception e) {
    log.error("스키마 alias 매핑 오류: " + e.message, e)
    session.transfer(flowFile, REL_FAILURE)
}