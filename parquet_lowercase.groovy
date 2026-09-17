import org.apache.nifi.serialization.RecordReaderFactory
import org.apache.nifi.serialization.RecordSetWriterFactory
import org.apache.nifi.serialization.record.MapRecord
import org.apache.nifi.serialization.record.Record
import org.apache.nifi.serialization.record.RecordField
import org.apache.nifi.serialization.record.SimpleRecordSchema
import org.apache.nifi.processor.io.StreamCallback

def flowFile = session.get()
if (!flowFile) return

def readerFactory = context.getProperty('Record Reader').asControllerService(RecordReaderFactory.class)
def writerFactory = context.getProperty('Record Writer').asControllerService(RecordSetWriterFactory.class)

try {
    def originalAttributes = flowFile.getAttributes()

    flowFile = session.write(flowFile, { inputStream, outputStream ->
        def reader = readerFactory.createRecordReader(originalAttributes, inputStream, flowFile.getSize(), log)
        try {
            def originalSchema = reader.getSchema()

            // 1. 필드명만 소문자로 변환한 새 스키마 생성 (타입/순서 동일 유지)
            def originalFields = originalSchema.getFields()
            def lowerFields = originalFields.collect { RecordField f ->
                new RecordField(f.getFieldName().toLowerCase(Locale.ROOT), f.getDataType(), f.isNullable())
            }
            def lowerSchema = new SimpleRecordSchema(lowerFields)

            def writer = writerFactory.createWriter(log, lowerSchema, outputStream, originalAttributes)
            try {
                writer.beginRecordSet()

                Record record
                while ((record = reader.nextRecord()) != null) {
                    // 2. 원본 Record 값을 새 스키마(소문자 필드명) 순서에 맞춰 재구성
                    def values = [:]
                    lowerFields.eachWithIndex { lf, idx ->
                        def originalFieldName = originalFields[idx].getFieldName()
                        values[lf.getFieldName()] = record.getValue(originalFieldName)
                    }
                    def newRecord = new MapRecord(lowerSchema, values)
                    writer.write(newRecord)
                }

                writer.finishRecordSet()
            } finally {
                writer.close()
            }
        } finally {
            reader.close()
        }
    } as StreamCallback)

    session.transfer(flowFile, REL_SUCCESS)

} catch (Exception e) {
    log.error("Parquet 스키마 소문자 변환 실패: {}", [e.getMessage()] as Object[], e)
    session.transfer(flowFile, REL_FAILURE)
}