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

# ==================== CONFIGURATION (environment) ====================
MQTT_BROKER = os.getenv("MQTT_BROKER", "raspi00")
MQTT_PORT = int(os.getenv("MQTT_PORT", "8883"))
MQTT_TLS_ENABLED = os.getenv("MQTT_TLS_ENABLED", "true").lower() == "true"
MQTT_TLS_CA_CERTS = os.getenv("MQTT_TLS_CA_CERTS", "/etc/mosquitto/certs/ca.crt")
MQTT_USERNAME = os.getenv("MQTT_USER", "your_mqtt_username")
MQTT_PASSWORD = os.getenv("MQTT_PASS", "your_mqtt_password")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER", "your_db_user")
DB_PASSWORD = os.getenv("DB_PASS", "your_db_password")
DB_NAME = os.getenv("DB_NAME", "IOT_DB")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Reserved JSON keys
RESERVED_KEYS_DAT = {"dNM", "dPJ", "dS"}
RESERVED_KEYS_DIAG = {"dNM", "dDGT", "dS"}

# Dynamic field prefixes (keys starting with these are stored, prefix removed)
DYNAMIC_FIELD_PREFIXES = ["d_"]

# Name of the array for extra diagnostic blocks
DIAG_ARRAY_FIELD = "diagnostics"

# Retention limits for E_LOG categories
MAX_MESSAGE_LOGS = 200      # JSON errors
MAX_DB_LOGS = 1000          # database errors

# Global database connection and column cache
db_conn = None
column_cache = set()

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
    """Create parent diagnostic or data table (no parent_id)"""
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

def insert_parent_only(conn, dev_name, table_name):
    """Insert a minimal parent row with only deviceID and ts (no dynamic fields)."""
    dev_id = get_device_id_from_name(conn, dev_name)
    if dev_id is None:
        logger.error(f"Cannot insert parent: device {dev_name} not found")
        return None
    ensure_data_table(conn, table_name)
    now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"INSERT INTO `{table_name}` (deviceID, ts) VALUES (%s, %s)", (dev_id, now_ts))
            conn.commit()
            return cursor.lastrowid
    except Exception as e:
        logger.error(f"Failed to insert parent row: {e}")
        conn.rollback()
        return None

# ---------- Connection tracking functions ----------
def get_device_id_from_name(conn, dev_name):
    try:
        with conn.cursor() as cursor:
            cursor.execute("CALL GetDeviceID(%s, @devID)", (dev_name,))
            cursor.execute("SELECT @devID")
            dev_id = cursor.fetchone()[0]
            conn.commit()
            return dev_id
    except Exception as e:
        logger.error(f"Failed to get/create device ID for {dev_name}: {e}")
        conn.rollback()
        return None

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
            # Close any existing open connection
            cursor.execute("""
                UPDATE DEVICE_CONN
                SET disconnected_ts = NOW()
                WHERE devID = %s AND disconnected_ts IS NULL
            """, (dev_id,))
            conn.commit()
            if cursor.rowcount > 0:
                logger.info(f"Closed previous open connection for device {dev_name}")
            # Insert new connection record
            cursor.execute("""
                INSERT INTO DEVICE_CONN (devID, projID, connected_ts, last_msg_ts, disconnected_ts)
                VALUES (%s, %s, NOW(), NOW(), NULL)
            """, (dev_id, proj_id))
            conn.commit()
            logger.info(f"New connection created for device {dev_name}")
    except Exception as e:
        logger.error(f"Error creating connection for {dev_name}: {e}")
        conn.rollback()

def update_device_last_msg_ts(conn, dev_id):
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE DEVICE_CONN SET last_msg_ts = NOW()
                WHERE devID = %s AND disconnected_ts IS NULL
            """, (dev_id,))
            conn.commit()
            if cursor.rowcount == 0:
                cursor.execute("""
                    INSERT INTO DEVICE_CONN (devID, connected_ts, last_msg_ts, disconnected_ts)
                    VALUES (%s, NOW(), NOW(), NULL)
                """, (dev_id,))
                conn.commit()
                logger.debug(f"Created new connection session for device {dev_id}")
    except Exception as e:
        logger.error(f"Failed to update last_msg_ts for devID {dev_id}: {e}")
        conn.rollback()

def update_device_uptime(conn, dev_id, uptime_str):
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE DEVICE_CONN
                SET uptime_str = %s
                WHERE devID = %s AND disconnected_ts IS NULL
            """, (uptime_str[:31], dev_id))
            conn.commit()
            if cursor.rowcount > 0:
                logger.debug(f"Updated uptime for device {dev_id} to {uptime_str}")
    except Exception as e:
        logger.error(f"Failed to update uptime for devID {dev_id}: {e}")
        conn.rollback()

def update_device_firmware_info(conn, client_id, fw_data):
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE DEVICES SET
                    fw_version = %s,
                    fw_tag = %s,
                    fw_major = %s,
                    fw_minor = %s,
                    fw_patch = %s,
                    fw_build = %s,
                    fw_hash_short = %s,
                    fw_hash_full = %s,
                    fw_build_id = %s,
                    fw_dirty = %s,
                    project_name = %s
                WHERE devName = %s
            """, (
                fw_data.get('version'), fw_data.get('tag'),
                fw_data.get('major'), fw_data.get('minor'), fw_data.get('patch'),
                fw_data.get('build'), fw_data.get('hash_short'), fw_data.get('hash_full'),
                fw_data.get('build_id'), fw_data.get('dirty'), fw_data.get('project'),
                client_id
            ))
            conn.commit()
            if cursor.rowcount:
                logger.info(f"Updated firmware info for device {client_id}")
    except Exception as e:
        logger.error(f"Failed to update firmware info for {client_id}: {e}")
        conn.rollback()

def get_db_connection():
    global db_conn
    try:
        if db_conn is None or not db_conn.open:
            db_conn = pymysql.connect(host=DB_HOST, user=DB_USER,
                                      password=DB_PASSWORD, database=DB_NAME,
                                      autocommit=False, connect_timeout=5)
            logger.info("Database connected")
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
                    logger.info("Extended E_LOG table")
            with db_conn.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(*) FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'DEVICE_CONN' AND COLUMN_NAME = 'uptime_str'
                """, (DB_NAME,))
                if cursor.fetchone()[0] == 0:
                    cursor.execute("ALTER TABLE DEVICE_CONN ADD COLUMN uptime_str VARCHAR(32) NULL")
                    db_conn.commit()
                    logger.info("Added uptime_str column to DEVICE_CONN")
            for col in ['fw_version', 'fw_tag', 'fw_major', 'fw_minor', 'fw_patch', 'fw_build', 'fw_hash_short', 'fw_hash_full', 'fw_build_id', 'fw_dirty', 'project_name']:
                with db_conn.cursor() as cursor:
                    cursor.execute(f"""
                        SELECT COUNT(*) FROM information_schema.COLUMNS
                        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'DEVICES' AND COLUMN_NAME = '{col}'
                    """, (DB_NAME,))
                    if cursor.fetchone()[0] == 0:
                        if col == 'fw_dirty':
                            cursor.execute(f"ALTER TABLE DEVICES ADD COLUMN {col} BOOLEAN NULL")
                        else:
                            cursor.execute(f"ALTER TABLE DEVICES ADD COLUMN {col} VARCHAR(64) NULL")
                        db_conn.commit()
                        logger.info(f"Added column {col} to DEVICES")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise
    return db_conn

def get_devid_from_clientid(conn, client_id):
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

def process_diagnostic_entry(conn, data_dict, dev_name, table_name, parent_id=None):
    dev_id = get_device_id_from_name(conn, dev_name)
    if dev_id is None:
        logger.error(f"Cannot process diagnostic for {dev_name}: no devID")
        return None
    if parent_id is not None:
        ensure_child_table(conn, table_name)
    else:
        ensure_data_table(conn, table_name)
    dynamic_fields = {}
    for key, value in data_dict.items():
        if key in RESERVED_KEYS_DIAG:
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
    logger.debug(f"Diagnostic saved into {table_name} (parent_id={parent_id}), id={row_id}")
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
    update_device_last_msg_ts(conn, dev_id)
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
        logger.info(f"No project ID for {dev_name} – data discarded")

# ---------- MQTT callbacks ----------
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        logger.info("Connected to MQTT broker")
        client.subscribe("devices/+/status")
        client.subscribe("devices/+/data")
        client.subscribe("devices/+/diag")
        client.subscribe("IOT_DB/DAT/#")
        client.subscribe("IOT_DB/DIAG/#")
        logger.info("Subscribed to new and legacy topics")
    else:
        logger.error(f"MQTT connection failed with code {rc}")

def on_disconnect(client, userdata, rc, properties=None, reason_code=None):
    logger.warning(f"Disconnected (rc={rc}, reason={reason_code})")

def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode('utf-8').strip()
    parts = topic.split('/')

    # --- Extract client-id from MQTT5 User Property (primary) ---
    publisher_client_id = None
    if hasattr(msg, 'properties') and msg.properties:
        for key, value in msg.properties.UserProperty:
            if key == "client-id":
                publisher_client_id = value
                break

    # --- Fallback: extract from topic for new hierarchy ---
    topic_client_id = None
    if len(parts) >= 2 and parts[0] == "devices":
        topic_client_id = parts[1]

    # Final client_id
    client_id = publisher_client_id or topic_client_id

        # --- Handle status messages (LWT and firmware) ---
    if topic.endswith("/status") or (len(parts) >= 3 and parts[2] == "status"):
        if not client_id:
            logger.warning(f"Cannot extract client ID from status topic {topic}")
            return
        try:
            data = json.loads(payload)

            # --- Custom format: device field present, version field present (no "status") ---
            if "device" in data and "version" in data:
                logger.info(f"Device {data['device']} online (custom status format)")
                fw_data = {
                    "version": data.get("version"),
                    "tag": data.get("tag"),
                    "major": data.get("major"),
                    "minor": data.get("minor"),
                    "patch": data.get("patch"),
                    "build": data.get("build"),
                    "hash_short": data.get("hash_short"),
                    "hash_full": data.get("hash_full"),
                    "build_id": data.get("build_id"),
                    "dirty": data.get("dirty"),
                    "project": data.get("project")
                }
                db = get_db_connection()
                update_device_firmware_info(db, data["device"], fw_data)
                create_device_connection(db, data["device"])
                return

            # --- Standard format (status + fw) ---
            status = data.get("status")
            if status == "online":
                logger.info(f"Device {client_id} online (birth)")
                db = get_db_connection()
                fw = data.get("fw")
                if fw and isinstance(fw, dict):
                    update_device_firmware_info(db, client_id, fw)
                create_device_connection(db, client_id)
            elif status == "offline":
                logger.info(f"Device {client_id} offline (LWT)")
                db = get_db_connection()
                close_device_connection(db, client_id)
            else:
                logger.debug(f"Unknown status payload: {payload}")
        except json.JSONDecodeError:
            # Legacy plain string "online"/"offline"
            if payload == "online":
                logger.info(f"Device {client_id} online (legacy)")
                db = get_db_connection()
                create_device_connection(db, client_id)
            elif payload == "offline":
                logger.info(f"Device {client_id} offline (legacy)")
                db = get_db_connection()
                close_device_connection(db, client_id)
        return

    # --- Determine message type (data or diag) ---
    msg_type = None
    if topic.startswith("devices/") and len(parts) >= 3:
        sub_type = parts[2]
        if sub_type == "data":
            msg_type = "DAT"
        elif sub_type == "diag":
            msg_type = "DIAG"
    elif topic.startswith("IOT_DB/DAT"):
        msg_type = "DAT"
    elif topic.startswith("IOT_DB/DIAG"):
        msg_type = "DIAG"

    if not msg_type:
        return

    # --- Parse JSON ---
    try:
        data = json.loads(payload)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        conn = get_db_connection()
        dev_id = None
        if client_id:
            dev_id = get_device_id_from_name(conn, client_id)
        now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_unified_error(conn, dev_id, now_ts, str(e), category='M', topic=topic, payload=payload[:65535])
        if dev_id:
            update_device_last_msg_ts(conn, dev_id)
        return

    # --- Determine device name (client_id from property/topic takes precedence) ---
    dev_name = None
    if client_id:
        dev_name = client_id
    else:
        dev_name = data.get('dNM')
        if not dev_name and publisher_client_id:
            dev_name = publisher_client_id
    if not dev_name:
        logger.warning(f"Missing device identifier in {msg_type} message from {topic}")
        return

    db = get_db_connection()
    try:
        if msg_type == "DAT":
            process_data_message(db, data, topic, dev_name)
        else:  # DIAG
            # Ensure device exists and update last_msg_ts
            dev_id = get_device_id_from_name(db, dev_name)
            if dev_id is None:
                logger.error(f"Cannot process DIAG for {dev_name}: no devID")
                return
            update_device_last_msg_ts(db, dev_id)

            # Update uptime if present
            uptime = data.get('d_UPT') or data.get('d_dUPT')
            if uptime and isinstance(uptime, str):
                update_device_uptime(db, dev_id, uptime)

            root_diag_type = data.get('dDGT')
            if not root_diag_type:
                logger.warning(f"Missing dDGT in DIAG message from {topic}")
                # Log malformed message to E_LOG
                now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                log_unified_error(
                    db, dev_id, now_ts,
                    "Missing required field 'dDGT' in DIAG message",
                    category='M', topic=topic, payload=payload[:65535]
                )
                return

            root_save = data.get('dS')
            root_save_valid = root_save in ('S', 's', 'Y', 'y', '1')

            # Process diagnostics array
            diag_list = data.get(DIAG_ARRAY_FIELD)
            children_to_save = []
            if isinstance(diag_list, list):
                for idx, entry in enumerate(diag_list):
                    if not isinstance(entry, dict):
                        logger.warning(f"Entry {idx} not dict, skipping")
                        continue
                    child_diag = entry.get('dDGT')
                    child_save = entry.get('dS')
                    if not child_diag:
                        logger.warning(f"Entry {idx} missing dDGT, skipping")
                        continue
                    if child_save in ('S', 's', 'Y', 'y', '1'):
                        children_to_save.append((child_diag, entry))
                    else:
                        logger.debug(f"Skipping entry {idx} (dS={child_save})")

            need_parent = root_save_valid or len(children_to_save) > 0
            parent_id = None
            if need_parent:
                root_table = sanitize_name(root_diag_type, "dia")
                if root_save_valid:
                    parent_id = process_diagnostic_entry(db, data, dev_name, root_table, parent_id=None)
                else:
                    parent_id = insert_parent_only(db, dev_name, root_table)
                    if parent_id is None:
                        logger.error(f"Failed to create parent row for device {dev_name}, cannot save children")
                        children_to_save = []  # prevent child processing
                logger.debug(f"Parent row inserted with id={parent_id}")

            # Process children
            for child_diag, child_entry in children_to_save:
                if parent_id is None:
                    logger.error(f"Cannot save child {child_diag} because parent_id is None")
                    continue
                child_table = sanitize_name(f"{root_diag_type}_{child_diag}", "dia")
                process_diagnostic_entry(db, child_entry, dev_name, child_table, parent_id=parent_id)
                logger.debug(f"Child saved into {child_table}")

    except Exception as e:
        logger.error(f"Device: {dev_name} – {str(e)}")
        if db:
            try:
                dev_id = get_device_id_from_name(db, dev_name)
                now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                log_unified_error(db, dev_id, now_ts, f"Processing error: {str(e)[:500]}", category='D')
            except Exception as log_err:
                logger.error(f"Failed to log error: {log_err}")

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
    client = mqtt.Client(protocol=MQTTv5, callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    if MQTT_TLS_ENABLED:
        try:
            client.tls_set(ca_certs=MQTT_TLS_CA_CERTS, tls_version=ssl.PROTOCOL_TLSv1_2)
            logger.info("TLS configured")
        except Exception as e:
            logger.error(f"TLS setup failed: {e}")
            return
    if MQTT_USERNAME and MQTT_PASSWORD:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    logger.info(f"Connecting to {MQTT_BROKER}:{MQTT_PORT}")
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_forever()

if __name__ == "__main__":
    main()