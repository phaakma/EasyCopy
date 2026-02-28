"""Feature class execution strategies."""

import json
from typing import Any

from easycopy.models import ChangeSet


class FeatureClassExecutor:
    """Executes copy operations against geodatabase feature classes."""

    @staticmethod
    def _resolve_dataset_reference(dataset: Any) -> str:
        """Resolve a dataset-like object to an ArcPy-compatible path/layer string."""
        if isinstance(dataset, str):
            return dataset

        path_value = getattr(dataset, "path", None)
        if isinstance(path_value, str) and path_value:
            return path_value

        get_output = getattr(dataset, "getOutput", None)
        if callable(get_output):
            output = get_output(0)
            if output is not None:
                return str(output)

        return str(dataset)

    @staticmethod
    def _is_object_id_field(field_name: str) -> bool:
        """Return whether a field name is an ObjectID-like system field."""
        return field_name.lower() in {"objectid", "oid", "fid"}

    @staticmethod
    def _format_where_value(value: Any) -> str:
        """Format a Python value for an ArcPy SQL where-clause literal."""
        if value is None:
            return "NULL"

        if isinstance(value, bool):
            return "1" if value else "0"

        if isinstance(value, str):
            escaped = value.replace("'", "''")
            return f"'{escaped}'"

        return str(value)

    @classmethod
    def _build_where_clause(
        cls,
        arcpy_module: Any,
        dataset_reference: str,
        id_field: str,
        id_value: Any,
    ) -> str:
        """Build a safe where clause for id_field equality."""
        delimiter_fn = getattr(arcpy_module, "AddFieldDelimiters", None)
        field_expr = id_field
        if callable(delimiter_fn):
            field_expr = str(delimiter_fn(dataset_reference, id_field))

        if id_value is None:
            return f"{field_expr} IS NULL"

        value_expr = cls._format_where_value(id_value)
        return f"{field_expr} = {value_expr}"

    @staticmethod
    def _prepare_row_values(field_names: list[str], row: dict[str, Any]) -> list[Any]:
        """Map a row dictionary to cursor values for selected field names."""
        values: list[Any] = []
        for field_name in field_names:
            if field_name == "SHAPE@JSON":
                geometry_value = row.get("geometry")
                if isinstance(geometry_value, dict):
                    values.append(json.dumps(geometry_value))
                else:
                    values.append(geometry_value)
                continue

            values.append(row.get(field_name))
        return values

    def _resolve_insert_fields(self, rows: list[dict[str, Any]]) -> list[str]:
        """Resolve editable cursor fields for inserts from change rows."""
        if not rows:
            return []

        fields = [
            field
            for field in rows[0]
            if not self._is_object_id_field(field)
            and field.lower() not in {"globalid", "shape_length", "shape_area"}
        ]

        if "geometry" in rows[0] and "SHAPE@JSON" not in fields:
            fields.append("SHAPE@JSON")

        return [field for field in fields if field != "geometry"]

    def _resolve_update_fields(self, row: dict[str, Any], id_field: str) -> list[str]:
        """Resolve editable cursor fields for updates from a change row."""
        fields = [
            field
            for field in row
            if field != id_field
            and not self._is_object_id_field(field)
            and field.lower() not in {"globalid", "shape_length", "shape_area"}
        ]

        if "geometry" in row and "SHAPE@JSON" not in fields:
            fields.append("SHAPE@JSON")

        return [field for field in fields if field != "geometry"]

    def truncate_append(self, source: Any, target: Any) -> dict[str, Any]:
        """Run truncate + append for feature classes via ArcPy."""
        try:
            import arcpy
        except Exception as exc:
            raise RuntimeError("arcpy is required for feature class execution") from exc

        source_reference = self._resolve_dataset_reference(source)
        target_reference = self._resolve_dataset_reference(target)

        try:
            arcpy.management.TruncateTable(target_reference)
        except Exception as exc:
            get_messages = getattr(arcpy, "GetMessages", None)
            messages = ""
            if callable(get_messages):
                try:
                    messages = str(get_messages(2))
                except Exception:
                    messages = ""

            error_text = f"{exc} {messages}".lower()
            can_fallback = (
                "160592" in error_text
                or "truncation not allowed while editing" in error_text
            )
            if not can_fallback:
                raise

            arcpy.management.DeleteRows(target_reference)

        arcpy.management.Append(
            inputs=source_reference,
            target=target_reference,
            schema_type="NO_TEST",
        )

        return {"ok": True, "method": "TRUNCATE_APPEND", "processed": "all"}

    def read_records(self, dataset: Any, id_field: str) -> list[dict[str, Any]]:
        """Read records from a feature class or table using ArcPy cursors."""
        try:
            import arcpy
        except Exception as exc:
            raise RuntimeError("arcpy is required for feature class execution") from exc

        dataset_reference = self._resolve_dataset_reference(dataset)

        list_fields = getattr(arcpy, "ListFields", None)
        if not callable(list_fields):
            raise RuntimeError("arcpy.ListFields is required for CHANGEDETECTION")

        describe = getattr(arcpy, "Describe", None)
        data_type = ""
        if callable(describe):
            try:
                data_type = str(getattr(describe(dataset_reference), "dataType", ""))
            except Exception:
                data_type = ""

        fields: list[str] = []
        has_id_field = False

        for field in list_fields(dataset_reference):
            field_name = str(getattr(field, "name", ""))
            field_type = str(getattr(field, "type", ""))
            if not field_name:
                continue

            lower_name = field_name.lower()
            if field_name == id_field:
                has_id_field = True

            if field_type in {"OID", "GlobalID"}:
                continue
            if lower_name in {"shape_length", "shape_area"}:
                continue

            fields.append(field_name)

        if not has_id_field:
            raise ValueError(f"id_field '{id_field}' not found in dataset fields")

        cursor_fields = list(fields)
        if data_type in {"FeatureClass", "FeatureLayer"} and "SHAPE@JSON" not in cursor_fields:
            cursor_fields.append("SHAPE@JSON")

        records: list[dict[str, Any]] = []
        with arcpy.da.SearchCursor(dataset_reference, cursor_fields) as cursor:
            for row in cursor:
                record: dict[str, Any] = {}
                for index, field_name in enumerate(cursor_fields):
                    value = row[index]
                    if field_name == "SHAPE@JSON":
                        if isinstance(value, str):
                            try:
                                record["geometry"] = json.loads(value)
                            except Exception:
                                record["geometry"] = value
                        else:
                            record["geometry"] = value
                        continue

                    record[field_name] = value
                records.append(record)

        return records

    def apply_changes(
        self,
        target: Any,
        changes: ChangeSet,
        id_field: str,
    ) -> dict[str, Any]:
        """Apply adds, updates, and deletes to a feature class target."""
        try:
            import arcpy
        except Exception as exc:
            raise RuntimeError("arcpy is required for feature class execution") from exc

        target_reference = self._resolve_dataset_reference(target)

        add_count = 0
        update_count = 0
        delete_count = 0

        insert_fields = self._resolve_insert_fields(changes.adds)
        if insert_fields:
            with arcpy.da.InsertCursor(target_reference, insert_fields) as insert_cursor:
                for row in changes.adds:
                    insert_cursor.insertRow(
                        self._prepare_row_values(insert_fields, row)
                    )
                    add_count += 1

        for row in changes.updates:
            update_fields = self._resolve_update_fields(row, id_field)
            if not update_fields:
                continue

            where_clause = self._build_where_clause(
                arcpy,
                target_reference,
                id_field,
                row.get(id_field),
            )
            with arcpy.da.UpdateCursor(
                target_reference,
                update_fields,
                where_clause=where_clause,
            ) as update_cursor:
                for _existing in update_cursor:
                    update_cursor.updateRow(
                        self._prepare_row_values(update_fields, row)
                    )
                    update_count += 1
                    break

        for row in changes.deletes:
            where_clause = self._build_where_clause(
                arcpy,
                target_reference,
                id_field,
                row.get(id_field),
            )
            with arcpy.da.UpdateCursor(
                target_reference,
                [id_field],
                where_clause=where_clause,
            ) as delete_cursor:
                for _existing in delete_cursor:
                    delete_cursor.deleteRow()
                    delete_count += 1
                    break

        return {
            "ok": True,
            "method": "CHANGEDETECTION",
            "adds": add_count,
            "updates": update_count,
            "deletes": delete_count,
        }
