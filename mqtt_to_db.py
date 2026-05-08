#!/usr/bin/env python3
import os
import sys
import ssl
import json
import logging
import signal
import paho.mqtt.client as mqtt
from paho.mqtt.client import MQTTv5
import pymysql
from pymysql import Error as MySQLError
import re
from datetime import datetime, timedelta

# ==================== CONFIGURATION ====================
MQTT_BROKER = os.getenv("MQTT_BROKER", "raspi00")
MQTT_PORT = int(os.getenv("MQTT_PORT", "8883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "IOT_DB/#")
MQTT_TLS_ENABLED = os.getenv("MQTT_TLS_ENABLED", "true").lower() == "true"
MQTT_TLS_CA_CERTS = os.getenv("MQTT_TLS_CA_CERTS", "/etc/mosquitto/certs/ca.crt")
MQTT_USERNAME = os.getenv("MQTT_USER", "your_mqtt_username")
MQTT_PASSWORD = os.getenv("MQTT_PASS", "your_mqtt_password")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER", "your_db_user")
DB_PASSWORD = os.getenv("DB_PASS", "your_db_password")
DB_NAME = os.getenv("DB_NAME", "IOT_DB")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Reserved JSON keys for data and diagnostic messages
RESERVED_KEYS_DAT = {"dNM", "dPJ", "dS"}
RESERVED_KEYS_DIAG = {"dNM", "dDGT"}

# Dynamic field prefixes – only keys starting with these are stored (prefix stripped)
DYNAMIC_FIELD_PREFIXES = ["d_"]

# Name of the array field that contains additional diagnostic blocks
DIAG_ARRAY_FIELD = "diagnostics"

# Session timeout (minutes) – only used as fallback if disconnect not seen
SESSION_TIMEOUT_MINUTES = 30

# Retention limits for E_LOG categories
MAX_MESSAGE_LOGS = 200   # keep at most 200 JSON/parsing errors
MAX_DB_LOGS = 1000       # keep at most 1000 database errors

# Global database connection and column cache
db_conn = None
column_cache = set()

# Logging
logging.basicConfig(level=getattr(logging, LOG_LEVEL.upper()),
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ---------- Helper functions ----------
def sanitize_name(raw, prefix):
    clean = re.sub(r'[^A-Za-z0-9_]', '', raw)
    if not clean:
        raise ValueError(f"Name '{raw}' sanitized to empty string")
    return f"{prefix}_{clean}"

def get_sql_type(value):
    if isinstance(value, bool):
        return "TINYINT(1)"
    elif isinstance(value, int):
        return "INT"
    elif isinstance(value, float):
        return "DECIMAL(20,10)"
    elif isinstance(value, str):
        return "VARCHAR(255)"
    else:
        return "VARCHAR(255)"

def add_column_if_not_exists(conn, table_name, column_name, sql_type):
    key = (table_name, column_name)
    if key in column_cache:
        return
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
            """, (DB_NAME, table_name, column_name))
            exists = cursor.fetchone()[0] > 0
            if not exists:
                alter_sql = f"ALTER TABLE `{table_name}` ADD COLUMN `{column_name}` {sql_type}"
                cursor.execute(alter_sql)
                conn.commit()
                logger.info(f"Added column {column_name} to {table_name}")
        column_cache.add(key)
    except MySQLError as e:
        logger.error(f"Failed to add column {column_name}: {e}")
        raise

def ensure_data_table(conn, table_name):
    """Create parent diagnostic or data table (without parent_id)"""
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS `{table_name}` (
                    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
                    deviceID SMALLINT UNSIGNED NOT NULL,
                    ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
                    PRIMARY KEY (id),
                    INDEX (deviceID, ts)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            conn.commit()
            logger.info(f"Table {table_name} ready")
    except MySQLError as e:
        logger.error(f"Failed to create table {table_name}: {e}")
        raise

def ensure_child_table(conn, table_name):
    """Create child diagnostic table (with parent_id column)"""
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS `{table_name}` (
                    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
                    deviceID SMALLINT UNSIGNED NOT NULL,
                    parent_id INT UNSIGNED NOT NULL,
                    ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
                    PRIMARY KEY (id),
                    INDEX (deviceID, ts)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            conn.commit()
            logger.info(f"Child table {table_name} ready")
    except MySQLError as e:
        logger.error(f"Failed to create child table {table_name}: {e}")
        raise

def log_unified_error(conn, dev_id, ts, message, category='D', topic=None, payload=None):
    """
    Insert an error into E_LOG with category and optional topic/payload.
    After insertion, prune old rows for the same category.
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO E_LOG (devID, category, topic, payload, ts, message)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (dev_id, category, topic, payload, ts, message[:1000]))
            conn.commit()

            limit = MAX_MESSAGE_LOGS if category == 'M' else MAX_DB_LOGS
            cursor.execute("SELECT COUNT(*) FROM E_LOG WHERE category = %s", (category,))
            count = cursor.fetchone()[0]
            if count > limit:
                delete_count = count - limit
                cursor.execute("""
                    DELETE FROM E_LOG
                    WHERE id IN (
                        SELECT id FROM (
                            SELECT id FROM E_LOG
                            WHERE category = %s
                            ORDER BY ts ASC
                            LIMIT %s
                        ) AS t
                    )
                """, (category, delete_count))
                conn.commit()
                logger.debug(f"Pruned {delete_count} old rows from category {category}")
    except Exception as e:
        logger.error(f"Failed to log unified error: {e}")
        conn.rollback()

def insert_data_row(conn, table_name, dev_id, ts, data_fields, parent_id=None):
    columns = ['deviceID', 'ts']
    values = [dev_id, ts]
    if parent_id is not None:
        columns.insert(1, 'parent_id')
        values.insert(1, parent_id)
    for key, value in data_fields.items():
        columns.append(f"`{key}`")
        values.append(value)
    placeholders = ','.join(['%s'] * len(values))
    sql = f"INSERT INTO `{table_name}` ({','.join(columns)}) VALUES ({placeholders})"
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql, values)
            conn.commit()
            return cursor.lastrowid
    except MySQLError as e:
        if e.args[0] in (1406, 1264):
            log_unified_error(conn, dev_id, ts, f"Insert failed: {e} - field values: {data_fields}", category='D')
            logger.warning(f"Logged data error for device {dev_id}")
            return None
        else:
            logger.error(f"Insert error: {e}")
            raise

# ---------- Connection tracking functions (unchanged) ----------
def close_device_connection(conn, dev_name):
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT devID FROM DEVICES WHERE devName = %s", (dev_name,))
            row = cursor.fetchone()
            if not row:
                logger.warning(f"Device '{dev_name}' not found, cannot close connection")
                return
            dev_id = row[0]
            cursor.execute("""
                UPDATE DEVICE_CONN
                SET disconnected_ts = NOW()
                WHERE devID = %s AND disconnected_ts IS NULL
            """, (dev_id,))
            conn.commit()
            if cursor.rowcount > 0:
                logger.info(f"Closed connection for device {dev_name}")
    except Exception as e:
        logger.error(f"Error closing connection for {dev_name}: {e}")
        conn.rollback()

def create_device_connection(conn, dev_name):
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT devID, devPROJID FROM DEVICES WHERE devName = %s", (dev_name,))
            row = cursor.fetchone()
            if not row:
                logger.warning(f"Device '{dev_name}' not found, cannot create connection")
                return
            dev_id, proj_id = row
            cursor.execute("""
                INSERT INTO DEVICE_CONN (devID, projID, connected_ts, last_msg_ts, disconnected_ts)
                VALUES (%s, %s, NOW(), NOW(), NULL)
            """, (dev_id, proj_id))
            conn.commit()
            logger.info(f"New connection created for device {dev_name}")
    except Exception as e:
        logger.error(f"Error creating connection for {dev_name}: {e}")
        conn.rollback()

def get_db_connection():
    global db_conn
    try:
        if db_conn is None or not db_conn.open:
            db_conn = pymysql.connect(host=DB_HOST, user=DB_USER,
                                      password=DB_PASSWORD, database=DB_NAME,
                                      autocommit=False, connect_timeout=5)
            logger.info("Database connected")
            # Ensure E_LOG has extended columns
            with db_conn.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(*) FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'E_LOG' AND COLUMN_NAME = 'category'
                """, (DB_NAME,))
                if cursor.fetchone()[0] == 0:
                    cursor.execute("ALTER TABLE E_LOG ADD COLUMN category CHAR(1) NOT NULL DEFAULT 'D'")
                    cursor.execute("ALTER TABLE E_LOG ADD COLUMN topic VARCHAR(255) NULL")
                    cursor.execute("ALTER TABLE E_LOG ADD COLUMN payload TEXT NULL")
                    cursor.execute("CREATE INDEX idx_category_ts ON E_LOG (category, ts)")
                    db_conn.commit()
                    logger.info("Extended E_LOG table with category, topic, payload columns")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise
    return db_conn

# ---------- Helper to get/create device from client ID (MQTT5 User Property) ----------
def get_devid_from_clientid(conn, client_id):
    """Call the GetDeviceID stored procedure to get (or create) a device and return its devID."""
    try:
        with conn.cursor() as cursor:
            cursor.execute("CALL GetDeviceID(%s, @devID)", (client_id,))
            cursor.execute("SELECT @devID")
            dev_id = cursor.fetchone()[0]
            conn.commit()
            return dev_id
    except Exception as e:
        logger.error(f"Failed to get/create device for client ID '{client_id}': {e}")
        conn.rollback()
        return None

# ---------- MQTT callbacks ----------
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        logger.info(f"Connected to MQTT broker, subscribing to {MQTT_TOPIC}")
        client.subscribe(MQTT_TOPIC)
        client.subscribe("$SYS/broker/log/N")
        logger.info("Subscribed to $SYS/broker/log/N for connection tracking")
    else:
        logger.error(f"MQTT connection failed with code {rc}")

def on_disconnect(client, userdata, rc, properties=None, reason_code=None):
    logger.warning(f"Disconnected (rc={rc}, reason={reason_code})")

def process_diagnostic_entry(conn, data_dict, dev_name, table_name, parent_id=None):
    """Save a single diagnostic dictionary into the specified table."""
    with conn.cursor() as cursor:
        cursor.execute("CALL GetDeviceID(%s, @devID)", (dev_name,))
        cursor.execute("SELECT @devID")
        dev_id = cursor.fetchone()[0]
        conn.commit()
        logger.debug(f"Device {dev_name} -> devID {dev_id}")

    try:
        with conn.cursor() as cursor2:
            cursor2.execute("""
                UPDATE DEVICE_CONN SET last_msg_ts = NOW()
                WHERE devID = %s AND disconnected_ts IS NULL
            """, (dev_id,))
            conn.commit()
    except Exception as e:
        logger.debug(f"Could not update last_msg_ts: {e}")

    if parent_id is not None:
        ensure_child_table(conn, table_name)
    else:
        ensure_data_table(conn, table_name)

    dynamic_fields = {}
    for key, value in data_dict.items():
        if key in RESERVED_KEYS_DIAG or key == 'dS':
            continue
        matched_prefix = None
        for prefix in DYNAMIC_FIELD_PREFIXES:
            if key.startswith(prefix):
                matched_prefix = prefix
                break
        if not matched_prefix:
            continue
        base_name = key[len(matched_prefix):]
        col_name = re.sub(r'[^A-Za-z0-9_]', '_', base_name)
        dynamic_fields[col_name] = value

    for col_name, value in dynamic_fields.items():
        sql_type = get_sql_type(value)
        add_column_if_not_exists(conn, table_name, col_name, sql_type)

    now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    row_id = insert_data_row(conn, table_name, dev_id, now_ts, dynamic_fields, parent_id)
    logger.debug(f"Data saved into {table_name} (parent_id={parent_id}), id={row_id}")
    return row_id

def process_data_message(conn, data, topic, dev_name):
    save_flag = data.get('dS')
    if save_flag not in ('S', 's', 'Y', 'y', '1'):
        logger.debug(f"Ignoring DAT message: dS={save_flag}")
        return

    proj_id = data.get('dPJ')
    if proj_id is None:
        proj_id = ""
    else:
        proj_id = str(proj_id).strip()

    now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    with conn.cursor() as cursor:
        cursor.execute("CALL GetOrCreateDevice(%s, %s, @devID)", (dev_name, proj_id))
        cursor.execute("SELECT @devID")
        dev_id = cursor.fetchone()[0]
        conn.commit()
        logger.debug(f"Device {dev_name} -> devID {dev_id}")

    try:
        with conn.cursor() as cursor2:
            cursor2.execute("""
                UPDATE DEVICE_CONN SET last_msg_ts = NOW()
                WHERE devID = %s AND disconnected_ts IS NULL
            """, (dev_id,))
            conn.commit()
    except Exception as e:
        logger.debug(f"Could not update last_msg_ts: {e}")

    if proj_id:
        table_name = sanitize_name(proj_id, "dt")
        ensure_data_table(conn, table_name)

        dynamic_fields = {}
        for key, value in data.items():
            if key in RESERVED_KEYS_DAT:
                continue
            matched_prefix = None
            for prefix in DYNAMIC_FIELD_PREFIXES:
                if key.startswith(prefix):
                    matched_prefix = prefix
                    break
            if not matched_prefix:
                continue
            base_name = key[len(matched_prefix):]
            col_name = re.sub(r'[^A-Za-z0-9_]', '_', base_name)
            dynamic_fields[col_name] = value

        for col_name, value in dynamic_fields.items():
            sql_type = get_sql_type(value)
            add_column_if_not_exists(conn, table_name, col_name, sql_type)

        insert_data_row(conn, table_name, dev_id, now_ts, dynamic_fields)
        logger.debug(f"Data for {dev_name} saved into {table_name}")
    else:
        logger.info(f"No project ID for {dev_name} – data discarded (device record updated)")

def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode('utf-8').strip()

    # Handle $SYS/broker/log/N for connection tracking
    if topic == "$SYS/broker/log/N":
        logger.debug(f"SYS log: {payload}")
        conn_match = re.search(r'as (\S+) \(', payload)
        if conn_match:
            client_id = conn_match.group(1)
            logger.info(f"Device '{client_id}' connected")
            try:
                db = get_db_connection()
                create_device_connection(db, client_id)
            except Exception as e:
                logger.error(f"Failed to process connect event: {e}")
            return
        disconn_match = re.search(r'Client (\S+) disconnected\.', payload)
        if disconn_match:
            client_id = disconn_match.group(1)
            logger.info(f"Device '{client_id}' disconnected")
            try:
                db = get_db_connection()
                close_device_connection(db, client_id)
            except Exception as e:
                logger.error(f"Failed to process disconnect event: {e}")
            return
        return

    # --- Extract MQTT 5.0 User Property "client-id" ---
    publisher_client_id = None
    if hasattr(msg, 'properties') and msg.properties:
        for key, value in msg.properties.UserProperty:
            if key == "client-id":
                publisher_client_id = value
                break

    # --- Attempt to parse JSON ---
    try:
        data = json.loads(payload)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        # Malformed JSON: use the publisher's client ID (from User Property) to identify the device
        conn = get_db_connection()
        dev_id = None
        if publisher_client_id:
            dev_id = get_devid_from_clientid(conn, publisher_client_id)
            if dev_id:
                logger.warning(f"Malformed JSON from device '{publisher_client_id}' (devID={dev_id}): {e}")
        now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_unified_error(conn, dev_id, now_ts, str(e),
                          category='M', topic=topic, payload=payload[:65535])
        return

    # --- Valid JSON: determine message type ---
    if topic.startswith("IOT_DB/DAT"):
        msg_type = "DAT"
    elif topic.startswith("IOT_DB/DIAG"):
        msg_type = "DIAG"
    else:
        return

    # Get device name from JSON (fallback to MQTT client ID if missing)
    dev_name = data.get('dNM')
    if not dev_name:
        if publisher_client_id:
            logger.warning(f"Missing dNM in {msg_type} message, using MQTT client ID '{publisher_client_id}'")
            dev_name = publisher_client_id
        else:
            logger.warning(f"Missing dNM in {msg_type} message and no MQTT client ID – ignoring")
            return

    db = None
    try:
        db = get_db_connection()
        if msg_type == "DAT":
            process_data_message(db, data, topic, dev_name)
        else:  # DIAG
            root_diag_type = data.get('dDGT')
            if not root_diag_type:
                logger.warning(f"Missing dDGT in DIAG message from {topic} – ignoring entire message")
            else:
                root_table = sanitize_name(root_diag_type, "dia")
                root_id = process_diagnostic_entry(db, data, dev_name, root_table, parent_id=None)
                logger.debug(f"Root diagnostic saved with id={root_id}")

                diag_list = data.get(DIAG_ARRAY_FIELD)
                if isinstance(diag_list, list) and root_id:
                    for idx, entry in enumerate(diag_list):
                        if not isinstance(entry, dict):
                            logger.warning(f"Diagnostic entry {idx} is not a dict, skipping")
                            continue
                        child_diag = entry.get('dDGT')
                        child_save = entry.get('dS')
                        if not child_diag:
                            logger.warning(f"Diagnostic entry {idx} missing dDGT, skipping")
                            continue
                        if child_save not in ('S', 's', 'Y', 'y', '1'):
                            logger.debug(f"Skipping diagnostic entry {idx} (dS={child_save})")
                            continue
                        child_table = sanitize_name(f"{root_diag_type}_{child_diag}", "dia")
                        process_diagnostic_entry(db, entry, dev_name, child_table, parent_id=root_id)
    except Exception as e:
        logger.error(f"Device: {dev_name} – {str(e)}")
        if db:
            try:
                dev_id = None
                with db.cursor() as cur:
                    cur.execute("SELECT devID FROM DEVICES WHERE devName = %s", (dev_name,))
                    row = cur.fetchone()
                    if row:
                        dev_id = row[0]
                now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                log_unified_error(db, dev_id, now_ts, f"Processing error in {msg_type}: {str(e)[:500]}", category='D')
            except Exception as log_err:
                logger.error(f"Failed to log processing error: {log_err}")

def signal_handler(sig, frame):
    logger.info("Shutting down...")
    global db_conn
    if db_conn:
        db_conn.close()
    sys.exit(0)

def main():
    global db_conn
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Use MQTT 5.0 protocol to receive User Properties
    client = mqtt.Client(protocol=MQTTv5, callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    if MQTT_TLS_ENABLED:
        try:
            client.tls_set(ca_certs=MQTT_TLS_CA_CERTS,
                           tls_version=ssl.PROTOCOL_TLSv1_2)
            logger.info("TLS configured with CA cert: %s", MQTT_TLS_CA_CERTS)
        except Exception as e:
            logger.error(f"TLS setup failed: {e}")
            return

    if MQTT_USERNAME and MQTT_PASSWORD:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    logger.info(f"Connecting to {MQTT_BROKER}:{MQTT_PORT} (TLS={MQTT_TLS_ENABLED})")
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_forever()

if __name__ == "__main__":
    main()