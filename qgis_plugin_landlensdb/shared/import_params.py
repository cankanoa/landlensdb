def normalize_import_parameter_row(query_from='', import_type='', search_re=''):
    return (
        str(query_from or ''),
        str(import_type or ''),
        str(search_re or ''),
    )


def unique_import_parameter_rows(records):
    unique_rows = []
    seen = set()

    for record in records or []:
        metadata = record.get('metadata') if isinstance(record, dict) else None
        input_params = metadata.get('input_params', {}) if isinstance(metadata, dict) else {}
        row = normalize_import_parameter_row(
            input_params.get('query_from', ''),
            input_params.get('import_type', ''),
            input_params.get('search_re', ''),
        )
        if row in seen or not any(row):
            continue
        seen.add(row)
        unique_rows.append(row)

    return unique_rows


def import_parameter_label(row):
    query_from, import_type, search_re = normalize_import_parameter_row(*row)
    return ' | '.join([import_type or '(blank)', query_from or '(blank)', search_re or '(blank)'])
