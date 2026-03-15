from qgis.PyQt import QtCore

try:
    import psycopg2
    from psycopg2 import sql
except ImportError:  # pragma: no cover
    psycopg2 = None
    sql = None


SETTINGS_PREFIX = 'Landlensdb/connection'


def load_connection_settings():
    settings = QtCore.QSettings()
    return {
        'name': settings.value('{}/name'.format(SETTINGS_PREFIX), ''),
        'service': settings.value('{}/service'.format(SETTINGS_PREFIX), ''),
        'host': settings.value('{}/host'.format(SETTINGS_PREFIX), 'localhost'),
        'port': settings.value('{}/port'.format(SETTINGS_PREFIX), '5432'),
        'database': settings.value('{}/database'.format(SETTINGS_PREFIX), 'landlensdb'),
        'schema': settings.value('{}/schema'.format(SETTINGS_PREFIX), 'public'),
    }


def save_connection_settings(values):
    settings = QtCore.QSettings()
    for key, value in values.items():
        settings.setValue('{}/{}'.format(SETTINGS_PREFIX, key), value)


def validate_connection_values(values):
    required = {'Database': values.get('database', '').strip()}
    if not values.get('service', '').strip():
        required['Host'] = values.get('host', '').strip()
    missing = [label for label, value in required.items() if not value]
    if missing:
        return False, 'Missing required fields: {}'.format(', '.join(missing))
    if psycopg2 is None:
        return False, 'psycopg2 is not available in this QGIS Python environment.'
    return True, ''


def connection_kwargs(values):
    kwargs = {'dbname': values.get('database', '').strip()}
    service = values.get('service', '').strip()
    if service:
        kwargs['service'] = service
    else:
        kwargs['host'] = values.get('host', '').strip()
        kwargs['port'] = values.get('port', '').strip() or '5432'
    return kwargs


def test_connection_values(values):
    valid, message = validate_connection_values(values)
    if not valid:
        return False, message

    try:
        with psycopg2.connect(**connection_kwargs(values)) as connection:
            with connection.cursor() as cursor:
                schema_name = values.get('schema', '').strip()
                if schema_name:
                    cursor.execute(
                        sql.SQL('SET search_path TO {}, public').format(
                            sql.Identifier(schema_name),
                        )
                    )
                cursor.execute('SELECT 1')
    except Exception as exc:  # pragma: no cover
        return False, 'Connection failed: {}'.format(exc)

    return True, 'Connection successful'
