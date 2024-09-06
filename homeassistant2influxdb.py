# -*- coding: utf-8 -*-

import argparse
from datetime import datetime
from homeassistant.core import Event, State
from homeassistant.components.influxdb import get_influx_connection, INFLUX_SCHEMA
from homeassistant.exceptions import InvalidEntityFormatError
import json
import sys
from tqdm import tqdm
import voluptuous as vol
import yaml
import requests

sys.path.append("home-assistant-core")

# MySQL / MariaDB
try:
    from MySQLdb import connect as mysql_connect, cursors, Error
except:
    print("Warning: Could not load Mysql driver, might not be a problem if you intend to use sqlite")

# SQLite (not tested)
try:
    import sqlite3
except:
    print("Warning: Could not load sqlite3 driver, might not be a problem if you intend to use Mysql")


def rename_entity_id(old_name: str) -> str:
    rename_table = {
        "sensor.old_entity_name": "sensor.new_entity_name",
    }

    if old_name in rename_table:
        return rename_table[old_name]

    return old_name


def rename_friendly_name(attributes: dict) -> dict:
    rename_table = {
        "Old Sensor Name": "New Sensor Name",
    }

    if "friendly_name" in attributes and attributes["friendly_name"] in rename_table:
        attributes["friendly_name"] = rename_table[attributes["friendly_name"]]

    return attributes


def create_statistics_attributes(mean: float, min: float, max: float, attributes: dict) -> dict:
    attributes = rename_friendly_name(attributes)
    attributes['mean'] = mean
    attributes['min'] = min
    attributes['max'] = max
    return attributes


def convert_to_line_protocol(event):
    """Converts Home Assistant event to InfluxDB Line Protocol format"""
    state = event.data.get("new_state")
    measurement = state.entity_id
    tags = []
    fields = []
    timestamp = int(event.time_fired.timestamp() * 1e9)

    def process_dict(prefix, dictionary):
        """Helper function to process nested dictionaries."""
        for key, value in dictionary.items():
            if isinstance(value, str):
                value = value.replace(" ", "_")
            elif isinstance(value, dict):
                process_dict(key, value)
            else:
                fields.append(f"{key}={value}")

    for key, value in state.attributes.items():
        # Заменяем пробелы на подчеркивания в именах тегов
        if key not in ('device_class', 'state_class', 'icon'):
            key = key.replace(" ", "_")
            '''
            if isinstance(value, dict):
                process_dict(key, value)
            else:
                if isinstance(value, str):                
                    value = value.replace(" ", "_")
                    '''
            #if isinstance(value, str) and " " in value:
            #    value = f'""{value}""'
            if isinstance(value, str):
                value = value.replace(" ", "_")
            tags.append(f"{key}={value}")

    # Добавляем основное значение состояния в поля
    fields.append(f"value={state.state}")

    # Собираем финальную строку
    line = f"{measurement},{','.join(tags)} {','.join(fields)} {timestamp}"

    return line


def main():

    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('--type', '-t',
                        dest='type', action='store', required=True,
                        help='Database type: MySQL, MariaDB or SQLite')
    parser.add_argument('--db', '-b',
                        dest='db', action='store', required=True, choices=['influx', 'victoria'],
                        help='Target database: influx or victoria')
    parser.add_argument('--user', '-u',
                        dest='user', action='store', required=False,
                        help='MySQL/MariaDB username')
    parser.add_argument('--password', "-p",
                        dest='password', action='store', required=False,
                        help='MySQL/MariaDB password')
    parser.add_argument('--host', '-s',
                        dest='host', action='store', required=False,
                        help='MySQL/MariaDB host')
    parser.add_argument('--database', '-d',
                        dest='database', action='store', required=True,
                        help='MySQL/MariaDB database or SQLite databasefile')
    parser.add_argument('--count', '-c',
                        dest='row_count', action='store', required=False, type=int, default=0,
                        help='If 0 (default), determine upper bound of number of rows by querying database, '
                             'otherwise use this number (used for progress bar only)')
    parser.add_argument('--table', '-x',
                        dest='table', action='store', required=False, default='both',
                        help='Source Table is either states or statistics or both'
                             'Home Assistant keeps 10 days of states by default and keeps statistics forever for some entities')
    parser.add_argument('--dry-run', '-y',
                        dest='dry', action='store_true', required=False,
                        help='do all work except writing to InfluxDB or VictoriaMetrics')
    parser.add_argument('--victoria-url', '-v',
                        dest='victoria_url', action='store', required=True,
                        help='VictoriaMetrics write URL, e.g., http://localhost:8428/write')

    args = parser.parse_args()

    '''
    args = argparse.Namespace(
        type = 'MariaDB',
        db = 'victoria',
        user = 'hass',
        password = 'hass',
        table = 'states',
        host = '127.0.0.1',
        database = 'homeassistant',
        row_count = 0,
        dry = None,
        victoria_url = 'http://127.0.0.1:8428'
    )


    if args.dry:
        print("option --dry-run was given, nothing will be written to InfluxDB or VictoriaMetrics")

    with open("influxdb.yaml") as config_file:
        influx_config = yaml.load(config_file, Loader=yaml.FullLoader)

    schema = vol.Schema(INFLUX_SCHEMA, extra=vol.ALLOW_EXTRA)
    influx_config = schema(influx_config)

    if influx_config.get('token') == "TOKEN_HERE":
        print('oopsie woopsie we made a fucky wucky, we need to add our influx credentials to influxdb.yaml')
        exit()

    if args.db == 'victoria':
        victoria_url = args.victoria_url
    else:
        influx = get_influx_connection(influx_config, test_write=True, test_read=True)

    if args.type == "MySQL" or args.type == "MariaDB":
        connection = mysql_connect(host=args.host,
                                   user=args.user,
                                   password=args.password,
                                   database=args.database,
                                   cursorclass=cursors.SSCursor,
                                   charset="utf8")
    else:
        connection = sqlite3.connect(args.database)

    tables = get_tables(args.table)
    print(f"Migrating home assistant database (tables {', '.join(tables)}) to {args.db} database.")

    influx_batch_size_max = 1024
    influx_batch_size_cur = 0
    influx_batch_lines = []

    if 'statistics' in tables:
        cursor = connection.cursor()
        tmp_table_query = formulate_tmp_table_sql()
        cursor.execute(tmp_table_query)
        #print(tmp_table_query)
        cursor.close()

    for table in tables:
        print(f"Running SQL query on database table {table}." +
              "This may take longer than a few minutes, depending on how many rows there are in the database.")
        if args.row_count == 0:
            cursor = connection.cursor()
            cursor.execute(f"select COUNT(*) from {table}")
            total = cursor.fetchone()[0]
            cursor.close()
        else:
            total = args.row_count

        statistics = {}
        sql_query = formulate_sql_query(table, args.table, total)
        cursor = connection.cursor()
        #print(sql_query)
        cursor.execute(sql_query)

        print(
            f"    Processing max. {total} rows from table {table} and writing to {args.db} database.")
        with tqdm(total=total, mininterval=1, maxinterval=5, unit=" rows", unit_scale=True,
                  leave=True) as progress_bar:
            try:
                row_counter = 0
                for row in cursor:

                    if row_counter >= total:
                        print(f"Лимит обработанных строк ({total}) достигнут.")
                        #break


                    progress_bar.update(1)
                    row_counter += 1
                    try:
                        if table == "states":
                            _entity_id = rename_entity_id(row[0])
                            _state = row[1]
                            _attributes_raw = row[2]
                            _attributes = rename_friendly_name(json.loads(_attributes_raw))
                            _event_type = row[3]
                            _time_fired = datetime.fromtimestamp(row[4])
                        elif table == "statistics":
                            _entity_id = rename_entity_id(row[0])
                            _state = row[1]
                            _mean = row[1]
                            _min = row[2]
                            _max = row[3]
                            _attributes_raw = row[4]
                            _attributes = create_statistics_attributes(_mean, _min, _max, json.loads(_attributes_raw))
                            _event_type = row[5]
                            _time_fired = datetime.fromtimestamp(row[6])
                    except Exception as e:
                        print(f"Failed extracting data from {row}: {e}.\nAttributes: {_attributes_raw}")
                        continue

                    try:
                        state = State(
                            entity_id=_entity_id,
                            state=_state,
                            attributes=_attributes)
                        event = Event(
                            _event_type,
                            data={"new_state": state},
                            time_fired=_time_fired
                        )
                    except InvalidEntityFormatError:
                        pass
                    else:
                        line_protocol = convert_to_line_protocol(event)
                        #metric1 = f"{_entity_id} state={_state},mean={_attributes.get('mean')},min={_attributes.get('min')},max={_attributes.get('max')} {int(_time_fired.timestamp() * 1e9)}"
                        #metric2 = f'{_entity_id}{{mean="{_mean}",min="{_min}",max="{_max}"}} {int(_time_fired.timestamp())}'
                        #metric3 = f"{_entity_id} state={_state} mean={_mean} min={_min} max={_max} {int(_time_fired.timestamp())}000000000"

                        if _state not in (
                                "unavailable", "unknown", "off", "on", "home",
                                "not_home") \
                                and _entity_id not in ('plant.pepper'): # skipping these states is ok
                            influx_batch_lines.append(line_protocol)
                            influx_batch_size_cur += 1
                        #  else:
                        #    print(f"skipping {_entity_id} with state {_state} at {_time_fired}.")

                        if influx_batch_size_cur >= influx_batch_size_max:
                            if not args.dry:
                                if args.db == 'victoria':
                                    requests.post(f"{victoria_url}/write", data = "\n".join(influx_batch_lines).encode('utf-8'))
                                else:
                                        influx.write("\n".join(influx_batch_lines))
                            influx_batch_size_cur = 0
                            influx_batch_lines = []

            except Error as mysql_error:
                print(f"MySQL error on row {row_counter}: {mysql_error}")
                continue
            finally:
                progress_bar.close()
                cursor.close()

    if not args.dry and influx_batch_lines:
        if args.db == 'victoria':
            requests.post(f"{victoria_url}/write", data="\n".join(influx_batch_lines).encode('utf-8'))
        else:
            influx.write("\n".join(influx_batch_lines))

    print("Finished writing data to the database, closing connections.")
    connection.close()


def formulate_tmp_table_sql():
    """
    Create temporary LUT to map attributes_id to entity_id to improve performance.

    TODO Not perfect solution, some entities have multiple attributes that change over time.
    TODO Here we select the most recent
    """
    return """CREATE TABLE IF NOT EXISTS state_tmp AS
    SELECT max(states.attributes_id) as attributes_id, states_meta.entity_id
    FROM states, states_meta
    WHERE states.metadata_id = states_meta.metadata_id
      AND states.attributes_id IS NOT NULL
    GROUP BY states_meta.entity_id;
    """


def formulate_sql_query(table: str, arg_tables: str, total: int):
    """
    Retrieves data from the HA databse
    """
    sql_query = ""
    if table == "states":
        # Using two different SQL queries in a Union to support data made with older HA db schema:
        # https://github.com/home-assistant/core/pull/71165
        sql_query = """select states_meta.entity_id,
                              states.state,
                              state_attributes.shared_attrs as attributes,
                              'state_changed',
                              states.last_updated_ts as time_fired
                       from states, state_attributes, states_meta
                       where event_id is null
                        and states.attributes_id = state_attributes.attributes_id
                        and states.metadata_id = states_meta.metadata_id
                        """
        sql_query += f" LIMIT {total};"

    elif table == "statistics":
        if arg_tables == 'both':
            # If we're adding both, we should not add statistics for the same time period we're adding events
            inset_query = f"{sql_query}" + \
                f"\n         AND statistics.start_ts < (select min(events.time_fired_ts) as datetetime_start from events)"
        else:
            # inset_query = "\n         AND statistics.start_ts < 1708837680"
            inset_query = "\n         AND statistics.start_ts > 1630894088"
            # start 25.2. 6:10 MEZ = 1708837680
        sql_query = f"""
        SELECT statistics_meta.statistic_id,
               statistics.mean,
               statistics.min,
               statistics.max,
               state_attributes.shared_attrs,
               'state_changed',
               statistics.start_ts
        FROM statistics_meta,
             statistics,
             state_attributes
        WHERE statistics.metadata_id = statistics_meta.id
        """ + \
         f""" AND mean != 0 {inset_query}
         AND state_attributes.attributes_id = (
            SELECT state_tmp.attributes_id
            FROM state_tmp
            WHERE state_tmp.entity_id = statistics_meta.statistic_id)
        """
    return sql_query


def get_tables(table_type: str) -> list:
    if table_type == "both":
        return ["states", "statistics"]
    else:
        return [table_type]


if __name__ == "__main__":
    main()
