// ============================================================================
// Parquet 컬럼명을 소문자로 변환하는 NiFi 1.23.2 ExecuteScript(Groovy)
//
// [사전 준비 - ExecuteScript 프로세서 설정]
//   ExecuteScript는 프로퍼티를 Controller Service 타입(드롭다운)으로
//   선언하는 기능이 없어, 일반 텍스트 프로퍼티로 Controller Service의
//   UUID(식별자)를 넘기고 controllerServiceLookup으로 직접 조회합니다.
//
//   PROPERTIES 탭에 아래 두 개의 사용자 정의 프로퍼티를 텍스트로 추가:
//     - Record Reader  -> ParquetReader의 UUID 문자열
//                         (Controller Services 탭 -> 해당 서비스 -> View Details -> Id)
//     - Record Writer  -> ParquetRecordWriterLowercase의 UUID 문자열
//                         (Write Strategy: Do Not Write Schema - default 유지)
//
//   UUID는 Controller Service를 삭제 후 재생성하면 바뀌므로, 환경(개발/운영,
//   노드별)마다 실제 값을 다시 확인해서 넣어야 합니다.
//
// [동작]
//   1. ParquetReader로 FlowFile content를 스트리밍 방식으로 Record 단위 read
//   2. 원본 스키마의 필드명만 소문자로 변환한 새 RecordSchema 생성
//      (타입/순서는 원본 그대로 유지 -> 값 매핑 정합성 보장)
//   3. 각 Record를 새 스키마에 맞춰 재구성하며 즉시 write (스트리밍,
//      전체 Record를 메모리에 적재하지 않음 - GB급 파일 대응)
//   4. 원본 FlowFile content를 변환 결과로 교체
//
// [주의]
//   - nested 타입(struct/array/map) 컬럼이 있는 경우 최상위 필드명만
//     변환되며, nested 내부 필드명은 변환되지 않습니다. 필요 시 재귀
//     처리 로직 추가가 필요합니다.
//   - RecordField(fieldName, dataType, aliases, nullable) 생성자와
//     RecordSetWriterFactory.createWriter(logger, schema, out, variables)
//     시그니처는 NiFi 공식 API(1.9.0 이상 공통)를 확인 후 반영했습니다.
// ============================================================================

import org.apache.nifi.serialization.RecordReaderFactory
import org.apache.nifi.serialization.RecordSetWriterFactory
import org.apache.nifi.serialization.record.MapRecord
import org.apache.nifi.serialization.record.Record
import org.apache.nifi.serialization.record.RecordField
import org.apache.nifi.serialization.record.SimpleRecordSchema
import org.apache.nifi.processor.io.StreamCallback

def flowFile = session.get()
if (!flowFile) return

try {
    // Controller Service는 UUID(텍스트 프로퍼티)로 넘겨받아 lookup으로 직접 조회
    def readerServiceId = context.getProperty('Record Reader').getValue()
    def writerServiceId = context.getProperty('Record Writer').getValue()

    if (!readerServiceId) throw new IllegalStateException("Record Reader 프로퍼티(UUID)가 설정되지 않았습니다")
    if (!writerServiceId) throw new IllegalStateException("Record Writer 프로퍼티(UUID)가 설정되지 않았습니다")

    def readerFactory = context.controllerServiceLookup.getControllerService(readerServiceId) as RecordReaderFactory
    def writerFactory = context.controllerServiceLookup.getControllerService(writerServiceId) as RecordSetWriterFactory

    if (readerFactory == null) throw new IllegalStateException("Record Reader Controller Service를 찾을 수 없습니다: ${readerServiceId}")
    if (writerFactory == null) throw new IllegalStateException("Record Writer Controller Service를 찾을 수 없습니다: ${writerServiceId}")

    def originalAttributes = flowFile.getAttributes()
    def flowFileSize = flowFile.getSize()

    flowFile = session.write(flowFile, { inputStream, outputStream ->
        def reader = readerFactory.createRecordReader(originalAttributes, inputStream, flowFileSize, log)
        try {
            def originalSchema = reader.getSchema()

            // 1. 필드명만 소문자로 변환한 새 스키마 생성 (타입/순서 동일 유지)
            def originalFields = originalSchema.getFields()
            def lowerFields = originalFields.collect { RecordField f ->
                new RecordField(
                    f.getFieldName().toLowerCase(Locale.ROOT),
                    f.getDataType(),
                    f.getAliases(),
                    f.isNullable()
                )
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