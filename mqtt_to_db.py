#!/usr/bin/env python3
import os
import sys
import ssl
import json
import logging
import signal
import paho.mqtt.client as mqtt
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
MQTT_USERNAME = os.getenv("MQTT_USER", "edolis")
MQTT_PASSWORD = os.getenv("MQTT_PASS", "")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER", "pyBridge")
DB_PASSWORD = os.getenv("DB_PASS", "pyBridgeSpring")
DB_NAME = os.getenv("DB_NAME", "IOT_DB")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Reserved JSON keys
RESERVED_KEYS_DAT = {"dNM", "dPJ", "dS"}
RESERVED_KEYS_DIAG = {"dNM", "dDGT"}

# Session timeout (minutes)
SESSION_TIMEOUT_MINUTES = 30

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
        return "BIGINT"
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
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS `{table_name}` (
                    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
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

def log_error(conn, dev_id, ts, message):
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO E_LOG (devID, ts, message) VALUES (%s, %s, %s)",
                (dev_id, ts, message[:1000])
            )
            conn.commit()
    except MySQLError as e:
        logger.error(f"Failed to log error: {e}")

def insert_data_row(conn, table_name, dev_id, ts, data_fields):
    columns = ['deviceID', 'ts'] + [f"`{k}`" for k in data_fields.keys()]
    placeholders = ['%s'] * (2 + len(data_fields))
    values = [dev_id, ts] + list(data_fields.values())
    sql = f"INSERT INTO `{table_name}` ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql, values)
            conn.commit()
    except MySQLError as e:
        if e.args[0] in (1406, 1264):
            log_error(conn, dev_id, ts, f"Insert failed: {e} - field values: {data_fields}")
            logger.warning(f"Logged data error for device {dev_id}")
        else:
            logger.error(f"Insert error: {e}")
            raise

def get_or_create_connection(conn, dev_id):
    """Return current open connection ID for the device, or create a new one.
       Also updates last_msg_ts for that connection."""
    with conn.cursor() as cursor:
        # Look for an open connection (disconnected_ts IS NULL)
        cursor.execute("""
            SELECT conn_id, last_msg_ts FROM DEVICE_CONN
            WHERE devID = %s AND disconnected_ts IS NULL
            ORDER BY last_msg_ts DESC LIMIT 1
        """, (dev_id,))
        row = cursor.fetchone()
        now = datetime.now()
        if row:
            conn_id, last_msg = row
            # If last message is older than timeout, close this session and create a new one
            if now - last_msg > timedelta(minutes=SESSION_TIMEOUT_MINUTES):
                cursor.execute("UPDATE DEVICE_CONN SET disconnected_ts = %s WHERE conn_id = %s",
                               (now, conn_id))
                conn.commit()
                row = None
        if not row:
            # Create a new connection session
            cursor.execute("INSERT INTO DEVICE_CONN (devID, connected_ts, last_msg_ts) VALUES (%s, %s, %s)",
                           (dev_id, now, now))
            conn_id = cursor.lastrowid
            conn.commit()
        else:
            conn_id = row[0]
        # Update last_msg_ts for this connection (always)
        cursor.execute("UPDATE DEVICE_CONN SET last_msg_ts = %s WHERE conn_id = %s", (now, conn_id))
        conn.commit()
        return conn_id

def get_db_connection():
    global db_conn
    try:
        if db_conn is None or not db_conn.open:
            db_conn = pymysql.connect(host=DB_HOST, user=DB_USER,
                                      password=DB_PASSWORD, database=DB_NAME,
                                      autocommit=False, connect_timeout=5)
            logger.info("Database connected")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise
    return db_conn

# ---------- MQTT callbacks ----------
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        logger.info(f"Connected to MQTT broker, subscribing to {MQTT_TOPIC}")
        client.subscribe(MQTT_TOPIC)
    else:
        logger.error(f"MQTT connection failed with code {rc}")

def on_disconnect(client, userdata, rc, properties=None, reason_code=None):
    logger.warning(f"Disconnected (rc={rc}, reason={reason_code})")

def process_diagnostics(conn, data, topic, dev_name):
    diag_type = data.get('dDGT')
    if not diag_type:
        logger.warning(f"Missing dDGT in DIAG message from {topic} (device {dev_name}) – ignoring")
        return

    with conn.cursor() as cursor:
        cursor.execute("CALL GetDeviceID(%s, @devID)", (dev_name,))
        cursor.execute("SELECT @devID")
        dev_id = cursor.fetchone()[0]
        conn.commit()
        logger.debug(f"Device {dev_name} -> devID {dev_id} (diagnostic only)")

    # Update connection session
    conn_id = get_or_create_connection(conn, dev_id)
    logger.debug(f"Connection ID {conn_id} last_msg_ts updated")

    table_name = sanitize_name(diag_type, "dia")
    ensure_data_table(conn, table_name)

    dynamic_fields = {}
    for key, value in data.items():
        if key in RESERVED_KEYS_DIAG:
            continue
        col_name = "d_" + re.sub(r'[^A-Za-z0-9_]', '_', key)
        dynamic_fields[col_name] = value

    for col_name, value in dynamic_fields.items():
        sql_type = get_sql_type(value)
        add_column_if_not_exists(conn, table_name, col_name, sql_type)

    now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    insert_data_row(conn, table_name, dev_id, now_ts, dynamic_fields)
    logger.debug(f"Diagnostic data for {dev_name} saved into {table_name}")

def process_data(conn, data, topic, dev_name):
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

    # Update connection session
    conn_id = get_or_create_connection(conn, dev_id)
    logger.debug(f"Connection ID {conn_id} last_msg_ts updated")

    if proj_id:
        table_name = sanitize_name(proj_id, "dt")
        ensure_data_table(conn, table_name)

        dynamic_fields = {}
        for key, value in data.items():
            if key in RESERVED_KEYS_DAT:
                continue
            col_name = "d_" + re.sub(r'[^A-Za-z0-9_]', '_', key)
            dynamic_fields[col_name] = value

        for col_name, value in dynamic_fields.items():
            sql_type = get_sql_type(value)
            add_column_if_not_exists(conn, table_name, col_name, sql_type)

        insert_data_row(conn, table_name, dev_id, now_ts, dynamic_fields)
        logger.debug(f"Data for {dev_name} saved into {table_name}")
    else:
        logger.info(f"No project ID for {dev_name} – data discarded (device record updated)")

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode('utf-8')
        data = json.loads(payload)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return

    topic = msg.topic
    if topic.startswith("IOT_DB/DAT"):
        msg_type = "DAT"
    elif topic.startswith("IOT_DB/DIAG"):
        msg_type = "DIAG"
    else:
        return

    dev_name = data.get('dNM')
    if not dev_name:
        logger.warning(f"Missing dNM in {msg_type} message from {topic} – ignoring")
        return

    try:
        conn = get_db_connection()
        if msg_type == "DAT":
            process_data(conn, data, topic, dev_name)
        else:
            process_diagnostics(conn, data, topic, dev_name)
    except Exception as e:
        logger.error(f"Device: {dev_name} – {str(e)}")

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

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
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