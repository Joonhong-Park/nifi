import traceback

from org.apache.nifi.processor.io import StreamCallback

flowFile = session.get()
if flowFile is None:
    pass
else:
    try:
        avro_schema_str = flowFile.getAttribute('avro.schema')
        out_schema_str = flowFile.getAttribute('out_schema')

        if avro_schema_str is None:
            raise ValueError("avro.schema attribute가 없습니다")
        if out_schema_str is None:
            raise ValueError("out_schema attribute가 없습니다")

        avro_root = json.loads(avro_schema_str)
        out_root = json.loads(out_schema_str)

        avro_fields = avro_root.get('fields')
        out_fields = out_root.get('fields')
        if avro_fields is None or out_fields is None:
            raise ValueError("fields 배열이 없습니다")

        # avro.schema 필드 중 type이 ["null","bytes"]인 필드만 name 기준 맵으로 구성
        bytes_field_by_name = {}
        for f in avro_fields:
            field_type = f.get('type')
            if isinstance(field_type, list) and 'bytes' in field_type:
                bytes_field_by_name[f['name']] = f

        # out_schema 필드 중 이름이 일치하는 것만 avro.schema 필드로 통째 교체
        for i in range(len(out_fields)):
            f_name = out_fields[i].get('name')
            if f_name in bytes_field_by_name:
                out_fields[i] = dict(bytes_field_by_name[f_name])

        updated_out_schema_str = json.dumps(out_root)
        flowFile = session.putAttribute(flowFile, 'out_schema', updated_out_schema_str)

        session.transfer(flowFile, REL_SUCCESS)

    except Exception, e:
        log.error("out_schema binary 컬럼 처리 실패: {}".format(str(e)))
        session.transfer(flowFile, REL_FAILURE)