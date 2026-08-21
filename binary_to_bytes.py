import json
import sys


def main() -> None:
    avro_schema_str = sys.argv[1]
    out_schema_str = sys.argv[2]

    avro_root = json.loads(avro_schema_str)
    out_root = json.loads(out_schema_str)

    avro_fields = avro_root.get("fields")
    out_fields = out_root.get("fields")
    if avro_fields is None or out_fields is None:
        raise ValueError("fields 배열이 없습니다")

    # avro.schema 필드 중 type이 ["null","bytes"]인 필드만 name 기준 맵으로 구성
    bytes_field_by_name = {}
    for f in avro_fields:
        field_type = f.get("type")
        if isinstance(field_type, list) and "bytes" in field_type:
            bytes_field_by_name[f["name"]] = f

    # out_schema 필드 중 이름이 일치하는 것만 avro.schema 필드로 통째 교체
    for i, field_node in enumerate(out_fields):
        f_name = field_node.get("name")
        if f_name in bytes_field_by_name:
            out_fields[i] = dict(bytes_field_by_name[f_name])  # 얕은 복사로 참조 분리

    print(json.dumps(out_root))


if __name__ == "__main__":
    main()