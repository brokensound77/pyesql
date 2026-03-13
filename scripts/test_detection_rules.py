"""Validate all ES|QL detection rules and hunts from the elastic/detection-rules repo.

Validates both parse correctness and schema correctness (field existence + type
compatibility). Schema is built per-rule by merging, in priority order:

    ECS (base) → beats/integration → endgame → non-ecs-schema.json

Usage:
    python scripts/test_detection_rules.py [path-to-detection-rules-repo]

The repo path defaults to ./detection-rules. Clone it next to this project if
not specified.
"""

import gzip
import json
import sys
import tomllib
import warnings
from fnmatch import fnmatch
from pathlib import Path

from pyesql import parse
from pyesql.ast import FromCommand, IndexPattern
from pyesql.schema import ESQL_TYPES, Schema
from pyesql.validator import SchemaValidationError, SchemaValidationWarning, SchemaValidator


def _read_gz_json(path: Path) -> dict:
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return json.load(fh)


def _latest_dir(parent: Path) -> Path | None:
    dirs = sorted(d for d in parent.iterdir() if d.is_dir())
    return dirs[-1] if dirs else None


def _latest_gz(parent: Path) -> Path | None:
    gz_files = sorted(parent.glob("*.gz"))
    return gz_files[-1] if gz_files else None


def load_ecs_schema(repo: Path) -> dict[str, str]:
    """Load latest ECS flat schema → {field: type}.

    ecs_flat.json.gz format: {field_name: {"type": "keyword", ...}}
    """
    latest = _latest_dir(repo / "detection_rules" / "etc" / "ecs_schemas")
    if latest is None:
        return {}
    raw = _read_gz_json(latest / "ecs_flat.json.gz")
    return {k: v["type"] for k, v in raw.items() if isinstance(v, dict) and "type" in v}


def load_non_ecs_schema(repo: Path) -> dict:
    """Load non-ecs-schema.json → {index_pattern: flat-or-nested field dict}."""
    path = repo / "detection_rules" / "etc" / "non-ecs-schema.json"
    return json.loads(path.read_text()) if path.exists() else {}


def load_integration_schemas(repo: Path) -> dict:
    """Load integration-schemas.json.gz → {package: {version: {integration: {field: type}}}}."""
    path = repo / "detection_rules" / "etc" / "integration-schemas.json.gz"
    return _read_gz_json(path) if path.exists() else {}


def integration_type_map(
    integration_schemas: dict,
    package: str,
    integration: str | None = None,
) -> dict[str, str]:
    """Return a flat {field: type} map for a package, optionally scoped to one integration.

    Uses the latest available version of the package.
    """
    versions = integration_schemas.get(package, {})
    if not versions:
        return {}
    latest = sorted(versions)[-1]
    package_data = versions[latest]

    if integration and integration in package_data:
        return {k: v for k, v in package_data[integration].items() if isinstance(v, str)}

    # No specific integration — merge all (skip the "jobs" ML key)
    merged: dict[str, str] = {}
    for key, fields in package_data.items():
        if key != "jobs" and isinstance(fields, dict):
            merged.update({k: v for k, v in fields.items() if isinstance(v, str)})
    return merged


def load_beats_schema(repo: Path) -> dict:
    """Load latest beats schema (raw folder tree keyed by beat name)."""
    gz = _latest_gz(repo / "detection_rules" / "etc" / "beats_schemas")
    return _read_gz_json(gz) if gz else {}


def load_endgame_schema(repo: Path) -> dict[str, str]:
    """Load latest endgame schema → {field: type}."""
    latest = _latest_dir(repo / "detection_rules" / "etc" / "endgame_schemas")
    if latest is None:
        return {}
    gz = _latest_gz(latest)
    if gz is None:
        return {}
    raw = _read_gz_json(gz)
    # ECS-flat style: {field: {type: ...}}
    if raw and isinstance(next(iter(raw.values())), dict):
        return {k: v["type"] for k, v in raw.items() if isinstance(v, dict) and "type" in v}
    # Plain {field: type}
    return {k: v for k, v in raw.items() if isinstance(v, str)}


# Beats schema JSON has this shape:
#   {beat_name: {folders: {_meta: {...}, module: {folders: {mod: {folders: {ds: ...}}}}}}}
#
# Each folder node has "files" (dict of filename → parsed YAML field-def list)
# and "folders" (dict of child folder name → folder node).
# Field defs are dicts with "name", "type", and optionally "fields"/"field" for nesting.


def _field_defs_to_type_map(defs: list, prefix: str = "") -> dict[str, str]:
    """Recursively flatten beats field definitions to a {dotted.path: type} map.

    Container types (group, nested, object) are expanded into their children
    rather than emitted directly. Type defaults to "keyword" when absent.
    """
    result = {}
    for entry in defs:
        if not isinstance(entry, dict) or "name" not in entry:
            continue
        path = f"{prefix}.{entry['name']}" if prefix else entry["name"]
        kind = entry.get("type", "keyword")
        children = entry.get("fields") or entry.get("field") or []
        if isinstance(children, dict):
            children = [children]
        if kind in ("group", "nested", "object") and children:
            result |= _field_defs_to_type_map(children, path)
        else:
            result[path] = kind
    return result


def _folder_to_type_map(folder: dict) -> dict[str, str]:
    """Walk a beats folder node, collecting and flattening all field definitions."""
    result = {}
    for file_content in folder.get("files", {}).values():
        if isinstance(file_content, list):
            result |= _field_defs_to_type_map(file_content)
    for child_folder in folder.get("folders", {}).values():
        result |= _folder_to_type_map(child_folder)
    return result


def beat_type_map(
    beats_schema: dict,
    beat: str,
    module: str | None = None,
    dataset: str | None = None,
) -> dict[str, str]:
    """Return a flat {field: type} map for a beat, scoped to module/dataset if given."""
    top = beats_schema.get(beat, {}).get("folders", {})

    result = _folder_to_type_map(top.get("_meta", {}))

    if module:
        module_node = top.get("module", {}).get("folders", {}).get(module, {})
        scope = (
            module_node.get("folders", {}).get(dataset, {})
            if dataset and dataset in module_node.get("folders", {})
            else module_node
        )
        result |= _folder_to_type_map(scope)

    return result


# ── Index pattern → beat/module/dataset ───────────────────────────────────────
#
# Index pattern conventions:
#   {beat}-*                        e.g. winlogbeat-*, auditbeat-*
#   logs-{module}.{dataset}-*       e.g. logs-aws.s3access-default
#   metrics-{module}.{dataset}-*    e.g. metrics-aws.billing-default
#
# The prefix before the first "-" either IS the beat name or signals a
# data-stream type that maps to a canonical beat.

_DATA_STREAM_BEAT = {"logs": "filebeat", "metrics": "metricbeat", "traces": "apm"}


def index_to_beat_scope(index: str) -> tuple[str, str | None, str | None]:
    """Derive (beat, module, dataset) from an index pattern.

    Examples:
        "winlogbeat-*"          → ("winlogbeat", None,  None)
        "logs-aws.s3access-*"   → ("filebeat",   "aws", "s3access")
        "metrics-aws.billing-*" → ("metricbeat",  "aws", "billing")
    """
    prefix = index.split("-", 1)[0]
    beat = _DATA_STREAM_BEAT.get(prefix)
    if beat:
        # Strip the prefix and any trailing version component
        remainder = index[len(prefix) + 1 :].split("-")[0]
        parts = remainder.split(".", 1)
        return (beat, parts[0] or None, parts[1] if len(parts) > 1 else None)
    # Prefix is the beat name directly (winlogbeat, auditbeat, packetbeat, …)
    return (prefix, None, None)


def _is_endpoint_index(index: str) -> bool:
    return fnmatch(index, "endgame-*") or fnmatch(index, "logs-endpoint.*")


def _flatten_to_dotted(d: dict, prefix: str, out: dict[str, str]) -> None:
    """Recursively flatten a mixed flat/nested dict into dotted-path keys."""
    for key, value in d.items():
        path = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            _flatten_to_dotted(value, path, out)
        elif isinstance(value, str):
            out[path] = value


def _merge_known(target: dict[str, str], source: dict[str, str]) -> None:
    """Merge source into target, skipping any type strings not valid in ESQL (later wins)."""
    for field, ftype in source.items():
        if ftype in ESQL_TYPES:
            target[field] = ftype


# Elasticsearch internal metadata fields — always present, never in any schema file.
_ES_METADATA_FIELDS: dict[str, str] = {
    "_id": "keyword",
    "_index": "keyword",
    "_version": "long",
    "_seq_no": "long",
    "_primary_term": "long",
    "_routing": "keyword",
    "_source": "keyword",
    "_score": "double",
}


def build_rule_schema(
    indexes: list[str],
    ecs_fields: dict[str, str],
    beats_schema: dict,
    endgame_fields: dict[str, str],
    integration_schemas: dict,
    non_ecs: dict,
    integrations: list[str] | None = None,
) -> Schema:
    """Build a merged Schema for a rule: ECS → beats → integrations → endgame → non-ecs.

    ``integrations`` (from rule/hunt metadata) supplements index-pattern routing
    for cases where the index pattern alone is ambiguous.
    """
    merged: dict[str, str] = {k: v for k, v in ecs_fields.items() if v in ESQL_TYPES}
    merged.update(_ES_METADATA_FIELDS)
    integrations = integrations or []

    _endpoint = {"endpoint", "endgame"}
    if any(_is_endpoint_index(i) for i in indexes) or (_endpoint & set(integrations)):
        _merge_known(merged, endgame_fields)

    for index in indexes:
        beat, module, dataset = index_to_beat_scope(index)
        _merge_known(merged, beat_type_map(beats_schema, beat, module, dataset))

        # Integration package schema: logs-{package}.{dataset}-* → package + dataset
        if module:
            _merge_known(merged, integration_type_map(integration_schemas, module, dataset))

        for pattern, fields in non_ecs.items():
            if fnmatch(index, pattern) or fnmatch(pattern, index):
                flat: dict[str, str] = {}
                _flatten_to_dotted(fields, "", flat)
                _merge_known(merged, flat)

    # Any integration not already covered by index patterns: probe beats + integration schemas
    covered_modules = {index_to_beat_scope(i)[1] for i in indexes}
    for integration in integrations:
        if integration in _endpoint or integration in covered_modules:
            continue
        _merge_known(merged, beat_type_map(beats_schema, "filebeat", integration))
        _merge_known(merged, beat_type_map(beats_schema, "metricbeat", integration))
        _merge_known(merged, integration_type_map(integration_schemas, integration))

    return Schema(merged)


def infer_missing_integration(field: str, integration_schemas: dict) -> str | None:
    """Return 'module.dataset' if the field appears to come from a missing integration dataset.

    Checks whether the field's module is a known integration package but its dataset
    sub-key is absent from the bundled schema. Returns None if the module is unknown
    (genuinely unexpected field) or if the dataset IS present (schema gap not applicable).
    """
    parts = field.split(".")
    if len(parts) < 2:
        return None
    module, dataset = parts[0], parts[1]
    pkg_versions = integration_schemas.get(module)
    if not pkg_versions:
        return None  # module not a known integration — leave as unknown field warning
    latest = sorted(pkg_versions)[-1]
    if dataset not in pkg_versions[latest]:
        return f"{module}.{dataset}"
    return None


def load_elastic_rules(repo_path: Path, esql_only: bool = True) -> list[dict]:
    rules = [tomllib.loads(r.read_text()) for r in repo_path.rglob("rules/**/*.toml")]
    bb_rules = repo_path.rglob("rules_building_block/**/*.toml")
    rules += [tomllib.loads(r.read_text()) for r in bb_rules]
    if esql_only:
        return [r for r in rules if r["rule"].get("language") == "esql"]
    return rules


def load_elastic_hunts(repo_path: Path, esql_only: bool = True) -> list[dict]:
    hunts = [tomllib.loads(r.read_text()) for r in repo_path.rglob("hunting/**/*.toml")]
    if esql_only:
        return [h for h in hunts if h["hunt"].get("language") == ["ES|QL"]]
    return hunts


def validate(
    loaded_rules: list[dict],
    loaded_hunts: list[dict],
    ecs_fields: dict[str, str],
    beats_schema: dict,
    endgame_fields: dict[str, str],
    integration_schemas: dict,
    non_ecs: dict,
) -> None:
    # pyesql functional failures — unexpected crashes that should never happen
    pyesql_failures: list[str] = []
    # Rule-quality findings — validator working correctly on imperfect rules
    rule_parse_issues: list[str] = []
    rule_schema_issues: list[str] = []
    rule_schema_warnings: list[str] = []
    # Schema bundle gaps — unique module.dataset pairs known but not in bundle
    missing_schema_notices: set[str] = set()

    def _check_query(
        name: str,
        kind: str,
        query_text: str,
        indexes: list[str],
        integrations: list[str] | None = None,
    ) -> None:
        try:
            ast = parse(query_text)
        except Exception as exc:
            # Parse failures on production rules likely indicate a pyesql gap, but
            # could also be genuinely malformed rules — report separately, don't fail CI.
            rule_parse_issues.append(f"[{kind}] {name}: {exc}")
            return

        try:
            # Supplement explicit index list with FROM sources extracted from the AST
            if isinstance(ast.source, FromCommand):
                from_indexes = [p.index for p in ast.source.indices if isinstance(p, IndexPattern)]
                all_indexes = list(dict.fromkeys(indexes + from_indexes))
            else:
                all_indexes = indexes

            schema = build_rule_schema(
                all_indexes,
                ecs_fields,
                beats_schema,
                endgame_fields,
                integration_schemas,
                non_ecs,
                integrations,
            )
            validator = SchemaValidator(schema, on_unknown="warn", on_type_mismatch="error")
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always", SchemaValidationWarning)
                validator.validate(ast)
            for w in caught:
                msg = str(w.message)
                if msg.startswith("Unknown field '"):
                    field_name = msg[len("Unknown field '") :].split("'")[0]
                    missing = infer_missing_integration(field_name, integration_schemas)
                    if missing:
                        missing_schema_notices.add(missing)
                        continue
                rule_schema_warnings.append(f"[{kind}] {name}: {w.message}")
        except SchemaValidationError as exc:
            # Validator correctly identified a type issue in the rule — not a pyesql bug.
            rule_schema_issues.append(f"[{kind}] {name}: {exc}")
        except Exception as exc:
            # Unexpected crash inside pyesql — this is a functional failure.
            pyesql_failures.append(f"[{kind}] {name}: {exc}")

    def _parse_integrations(meta: dict) -> list[str]:
        value = meta.get("integration") or meta.get("integrations") or []
        return [value] if isinstance(value, str) else value

    for rule in loaded_rules:
        _check_query(
            rule["rule"]["name"],
            "rule",
            rule["rule"]["query"],
            rule["rule"].get("index") or [],
            _parse_integrations(rule.get("metadata", {})),
        )

    for hunt in loaded_hunts:
        hunt_data = hunt["hunt"]
        integrations = _parse_integrations(hunt_data)
        for query_text in hunt_data["query"]:
            _check_query(hunt_data["name"], "hunt", query_text, [], integrations)

    n_hunts = sum(len(h["hunt"]["query"]) for h in loaded_hunts)
    total = len(loaded_rules) + n_hunts
    print(f"\nValidated {total} queries ({len(loaded_rules)} rules, {n_hunts} hunts)")

    if missing_schema_notices:
        n = len(missing_schema_notices)
        print(f"\n{n} missing integration schema(s) (package known, dataset not in bundle):")
        for schema_id in sorted(missing_schema_notices):
            print(f"  {schema_id}")

    if rule_schema_warnings:
        n = len(rule_schema_warnings)
        print(f"\n{n} unknown field warning(s) (not found in any loaded schema):")
        for msg in rule_schema_warnings:
            print(f"  {msg}")

    if rule_parse_issues:
        print(f"\n{len(rule_parse_issues)} parse issue(s) (rule syntax or unsupported feature):")
        for msg in rule_parse_issues:
            print(f"  {msg}")

    if rule_schema_issues:
        print(f"\n{len(rule_schema_issues)} schema issue(s) (type mismatches detected in rules):")
        for msg in rule_schema_issues:
            print(f"  {msg}")

    if pyesql_failures:
        print(f"\n{len(pyesql_failures)} pyesql functional failure(s):")
        for msg in pyesql_failures:
            print(f"  {msg}")
        sys.exit(1)
    else:
        print("\npyesql functional validation passed.")


if __name__ == "__main__":
    dr_repo = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("detection-rules")

    print("Loading schemas...")
    _ecs = load_ecs_schema(dr_repo)
    _beats = load_beats_schema(dr_repo)
    _endgame = load_endgame_schema(dr_repo)
    _integrations = load_integration_schemas(dr_repo)
    _non_ecs = load_non_ecs_schema(dr_repo)
    print(
        f"  ECS: {len(_ecs)} fields | {len(_integrations)} integrations | {len(_non_ecs)} non-ECS"
    )

    print("Loading rules and hunts...")
    _rules = load_elastic_rules(dr_repo)
    _hunts = load_elastic_hunts(dr_repo)
    print(f"  {len(_rules)} rules | {len(_hunts)} hunts")

    validate(_rules, _hunts, _ecs, _beats, _endgame, _integrations, _non_ecs)
