from io import BytesIO

from openpyxl import Workbook

from src.plc.tag_importer import build_device_tag_import_template, parse_device_tag_mapping_content, parse_device_tag_mapping_file


def test_parse_device_tag_mapping_file_supports_csv_aliases():
    csv_payload = (
        "点位名称,PLC地址,数据类型,单位,资产ID,point_key,死区,防抖ms,说明\n"
        "fan_current,40001,float,A,ASSET_BAGHOUSE_01,fan_current_a,0.25,1500,main current\n"
        ",40002,float,C,ASSET_BAGHOUSE_01,temperature_c,0.1,500,missing name\n"
        "dust,40003,,mg/m3,ASSET_BAGHOUSE_01,dust_concentration_mg_m3,abc,oops,stack dust\n"
    ).encode("utf-8-sig")

    parsed = parse_device_tag_mapping_file("tag-mapping.csv", csv_payload)

    assert parsed["file_type"] == "csv"
    assert parsed["total_rows"] == 3
    assert parsed["parsed_rows"] == 2
    assert parsed["skipped_rows"] == 1

    first = parsed["tags"][0]
    assert first["name"] == "fan_current"
    assert first["point_key"] == "fan_current_a"
    assert first["deadband"] == 0.25
    assert first["debounce_ms"] == 1500

    second = parsed["tags"][1]
    assert second["data_type"] == "float"
    assert second["deadband"] is None
    assert second["debounce_ms"] == 0

    assert any("missing required name or address" in warning for warning in parsed["warnings"])
    assert any("invalid deadband" in warning for warning in parsed["warnings"])
    assert any("invalid debounce_ms" in warning for warning in parsed["warnings"])


def test_parse_device_tag_mapping_file_supports_excel():
    workbook = Workbook()
    sheet = workbook.active
    sheet.append(["name", "address", "data_type", "asset_id", "point_key", "deadband", "debounce_ms"])
    sheet.append(["pressure_diff", "DB1.DBD0", "FLOAT", "ASSET_DUST_COLLECTOR_01", "pressure_diff_kpa", 0.5, 1200])
    sheet.append(["running_state", "DB1.DBX8.0", "BOOL", "ASSET_DUST_COLLECTOR_01", "running_state", "", ""])

    buffer = BytesIO()
    workbook.save(buffer)

    parsed = parse_device_tag_mapping_file("points.xlsx", buffer.getvalue())

    assert parsed["file_type"] == "excel"
    assert parsed["parsed_rows"] == 2
    assert parsed["skipped_rows"] == 0
    assert parsed["detected_columns"][:3] == ["name", "address", "data_type"]

    first = parsed["tags"][0]
    assert first["name"] == "pressure_diff"
    assert first["address"] == "DB1.DBD0"
    assert first["data_type"] == "float"
    assert first["deadband"] == 0.5
    assert first["debounce_ms"] == 1200

    second = parsed["tags"][1]
    assert second["data_type"] == "bool"
    assert second["deadband"] is None
    assert second["debounce_ms"] == 0


def test_parse_device_tag_mapping_file_supports_manual_field_mapping():
    csv_payload = (
        "Signal,Register,SignalType,Machine,SemanticKey\n"
        "fan_current,40001,float,ASSET_BAGHOUSE_01,fan_current_a\n"
    ).encode("utf-8-sig")

    parsed = parse_device_tag_mapping_content(
        "custom-columns.csv",
        csv_payload,
        field_mapping={
            "name": "Signal",
            "address": "Register",
            "data_type": "SignalType",
            "asset_id": "Machine",
            "point_key": "SemanticKey",
        },
    )

    assert parsed["parsed_rows"] == 1
    assert parsed["field_mapping"]["name"] == "Signal"
    assert parsed["unmatched_columns"] == []
    assert parsed["tags"][0]["asset_id"] == "ASSET_BAGHOUSE_01"
    assert parsed["tags"][0]["point_key"] == "fan_current_a"


def test_build_device_tag_import_template_outputs_reimportable_files():
    csv_content, csv_name, csv_media_type = build_device_tag_import_template("csv")
    assert csv_name.endswith(".csv")
    assert csv_media_type.startswith("text/csv")
    csv_preview = parse_device_tag_mapping_file(csv_name, csv_content)
    assert csv_preview["parsed_rows"] >= 1

    xlsx_content, xlsx_name, xlsx_media_type = build_device_tag_import_template("xlsx")
    assert xlsx_name.endswith(".xlsx")
    assert "spreadsheetml" in xlsx_media_type
    xlsx_preview = parse_device_tag_mapping_file(xlsx_name, xlsx_content)
    assert xlsx_preview["parsed_rows"] >= 1
    assert xlsx_preview["tags"][0]["name"] == "pressure_diff"


def test_parse_device_tag_mapping_file_builds_validation_report():
    csv_payload = (
        "name,address,data_type,asset_id,point_key,unit\n"
        "pressure_diff,DB1.DBD0,bool,,,kPa\n"
        "dust_concentration,DB1.DBD0,float,,dust_concentration_mg_m3,mg/m3\n"
    ).encode("utf-8-sig")

    parsed = parse_device_tag_mapping_file("validation.csv", csv_payload)

    report = parsed["validation_report"]
    assert report["has_errors"] is True
    assert report["rows_with_errors"] == 2
    assert report["issue_counts"]["duplicate_address"] == 2
    assert report["issue_counts"]["missing_asset_id"] == 2
    assert report["issue_counts"]["missing_point_key"] == 1
    assert report["issue_counts"]["suspicious_type_mismatch"] == 1
    assert report["suggestion_count"] >= 3
    assert len(report["duplicate_clusters"]) == 1
    assert report["duplicate_clusters"][0]["cluster_key"] == "DB1"

    first_row = parsed["preview_rows"][0]
    assert first_row["status"] == "error"
    assert "address" in first_row["flagged_fields"]
    assert "asset_id" in first_row["flagged_fields"]
    assert "point_key" in first_row["flagged_fields"]
    assert "data_type" in first_row["flagged_fields"]
    assert any(issue["code"] == "suspicious_type_mismatch" for issue in first_row["issues"])
    assert any(suggestion["field"] == "point_key" and suggestion["value"] == "pressure_diff_kpa" for suggestion in first_row["suggestions"])
    assert any(suggestion["field"] == "asset_id" and suggestion["value"] == "ASSET_DUST_COLLECTOR_01" for suggestion in first_row["suggestions"])

    second_row = parsed["preview_rows"][1]
    assert "address" in second_row["flagged_fields"]
    assert "asset_id" in second_row["flagged_fields"]
    assert any(suggestion["field"] == "address" and suggestion["value"] == "DB1" for suggestion in second_row["suggestions"])


def test_parse_device_tag_mapping_file_revalidates_after_value_overrides():
    csv_payload = (
        "name,address,data_type,asset_id,point_key,unit\n"
        "pressure_diff,DB1.DBD0,float,,,kPa\n"
    ).encode("utf-8-sig")

    parsed = parse_device_tag_mapping_content(
        "override.csv",
        csv_payload,
        value_overrides={
            2: {
                "asset_id": "ASSET_DUST_COLLECTOR_01",
                "point_key": "pressure_diff_kpa",
            }
        },
    )

    report = parsed["validation_report"]
    assert report["has_errors"] is False
    assert report["clean_rows"] == 1
    assert report["error_count"] == 0

    preview_row = parsed["preview_rows"][0]
    assert preview_row["status"] == "ok"
    assert preview_row["tag"]["asset_id"] == "ASSET_DUST_COLLECTOR_01"
    assert preview_row["tag"]["point_key"] == "pressure_diff_kpa"
    assert preview_row["suggestions"] == []
