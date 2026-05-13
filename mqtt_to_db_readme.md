# MQTT to MariaDB Bridge – MQTT 5.0, LWT & Firmware Tracking

## Overview

This service subscribes to MQTT topics and stores JSON messages from ESP32 devices into a MariaDB database.
It uses **MQTT 5.0 User Properties** (`client-id`) as the primary device identifier and **Last Will & Testament (LWT)** messages to track connection state.

The bridge handles three categories of messages:

- **Data messages** – store sensor readings into project‑specific tables (`dt_<projectID>`).
- **Diagnostic messages** – store system health metrics into parent‑child tables (`dia_<type>`, `dia_<parent>_<child>`).
- **Status messages** – report device online/offline state and firmware information (updates `DEVICES` table).

---

## MQTT 5.0 Client Identification

Every published message **must** include an MQTT 5.0 User Property with key `"client-id"` and value equal to the device name (e.g., `"ESP_32:97:54"`). The bridge reads this property first; it is the most reliable source of the device identity. Fallbacks (topic, JSON field `dNM`) are used only if the property is missing.

ESP32 example (ESP‑IDF MQTT 5.0):

```c
esp_mqtt5_user_property_handle_t user_property = NULL;
esp_mqtt5_client_set_user_property(NULL, &user_property, "client-id", client_id);
esp_mqtt_client_publish(client, topic, payload, 0, qos, retain, user_property);
```

---

## MQTT Topics (New Hierarchy)

| Topic pattern                          | Purpose                                   | Identifier source             |
|----------------------------------------|-------------------------------------------|-------------------------------|
| `devices/<client-id>/status`           | LWT and firmware information              | MQTT5 property (`client-id`)  |
| `devices/<client-id>/data`             | Sensor data (project‑specific)            | MQTT5 property or topic       |
| `devices/<client-id>/diag`             | Diagnostic data (health metrics)          | MQTT5 property or topic       |

**Legacy topics** (still supported for backward compatibility):

- `IOT_DB/DAT/#`
- `IOT_DB/DIAG/#`

---

## Message Formats

### 1. Status Message (online / offline + firmware)

**Topic:** `devices/<client-id>/status`
**Payload:** JSON object with `"status"` and optional `"fw"` object.

**Example (online with firmware):**

```json
{
  "status": "online",
  "fw": {
    "version": "v1.2.3",
    "tag": "release-1.2.3",
    "major": 1,
    "minor": 2,
    "patch": 3,
    "build": 42,
    "hash_short": "a1b2c3d",
    "hash_full": "a1b2c3d4e5f67890...",
    "build_id": "P20260513-123456",
    "dirty": false,
    "project": "IOT_BRIDGE"
  }
}
```

**Example (offline):**

```json
{"status": "offline"}
```

**Behaviour:**
- `online` → closes any previous open connection, creates a new `DEVICE_CONN` row, updates `DEVICES` with firmware info (if provided).
- `offline` → sets `disconnected_ts = NOW()` for the current open connection.

---

### 2. Data Message (sensor readings)

**Topic:** `devices/<client-id>/data` (or `IOT_DB/DAT/#`)
**Required fields:**

| Field | Description | Example |
|-------|-------------|---------|
| `dPJ` | Project ID – if empty, **no data saved** | `"P001"` |
| `dS`  | Save flag (must be `S`, `s`, `Y`, `y`, `1`) | `"S"` |

All other keys that start with the configured prefix (default `"d_"`) are stored as dynamic columns (prefix stripped). Example: `d_temperature` → column `temperature`.

**Example:**

```json
{
  "dPJ": "P002",
  "dS": "S",
  "d_temperature": 23.5,
  "d_humidity": 60
}
```

→ Inserts into table `dt_P002` with columns `temperature`, `humidity`.
→ Updates `last_msg_ts` in `DEVICE_CONN` for the device.

---

### 3. Diagnostic Message (health metrics)

**Topic:** `devices/<client-id>/diag` (or `IOT_DB/DIAG/#`)

Diagnostics use a **parent‑child** table structure. The parent table (`dia_<rootDGT>`) stores common fields (including device uptime). Child tables (`dia_<rootDGT>_<childDGT>`) store specific metrics, each with a `parent_id` linking to the parent row.

**Saving logic:**

- A parent row is created **if and only if**:
  - The root block contains a valid `dS` flag, **OR**
  - At least one entry in the `diagnostics` array has a valid `dS` flag.
- **If parent is created and root `dS` is valid**: root‑level dynamic fields (e.g., `d_UPS`) are stored in the parent table.
- **If parent is created but root `dS` is not valid**: parent row contains only `deviceID` and `ts` (no dynamic fields).
- A child row is created **only if** that child entry has a valid `dS` flag. It is linked to the parent via `parent_id`.
- The device’s uptime (`d_UPT`) is always stored in `DEVICE_CONN.uptime_str` for the current connection.

#### Root block fields

| Field | Description | Example |
|-------|-------------|---------|
| `dDGT` | Parent diagnostic type – table name `dia_<type>` | `"DTF"` |
| `dS`  | **Save flag for root** – if valid, root dynamic fields are saved | `"S"` |
| `d_UPT` | Device uptime string – stored in `DEVICE_CONN.uptime_str` | `"1 d 21:08:59"` |
| other `d_` prefixed keys | Become dynamic columns in the parent table (only if `dS` valid) | `d_UPS: 43168` |

#### Additional blocks (inside `diagnostics` array)

| Field | Description | Example |
|-------|-------------|---------|
| `dDGT` | Child diagnostic type – table name `dia_<parent>_<child>` | `"DTM"` |
| `dS`  | Save flag – if not set or invalid, child is skipped | `"S"` |
| other `d_` prefixed keys | Become dynamic columns in the child table | `d_hfree: 219240` |

**Example: root `dS=Y`, child `dS=Y`**

```json
{
  "dDGT": "DTF",
  "dS": "S",
  "d_UPT": "1 d 21:08:59",
  "d_UPS": 43168,
  "diagnostics": [
    {
      "dDGT": "DTM",
      "dS": "S",
      "d_hfree": 219240
    }
  ]
}
```

**Result:**
- `DEVICE_CONN.uptime_str` updated to `"1 d 21:08:59"`.
- Parent table `dia_DTF`: new row with columns `UPS`.
- Child table `dia_DTF_DTM`: new row with column `hfree`, `parent_id` pointing to parent row.

**Example: root `dS=N`, child `dS=Y`**

```json
{
  "dDGT": "DTF",
  "dS": "N",
  "d_UPT": "2 d 03:15:22",
  "diagnostics": [
    {
      "dDGT": "DTM",
      "dS": "S",
      "d_hfree": 219240
    }
  ]
}
```

**Result:**
- `DEVICE_CONN.uptime_str` updated.
- Parent row created with only `deviceID`, `ts` (no dynamic columns).
- Child row created in `dia_DTF_DTM` with `parent_id` linking to parent.

**Example: root `dS=N`, no child with `dS=Y`** → No rows inserted in `dia_*` tables; only `last_msg_ts` and `uptime_str` updated.

---

### 4. Malformed JSON

If the payload cannot be parsed as JSON, the message is logged to `E_LOG` with category `'M'`, including the raw payload and MQTT topic. If the MQTT 5.0 `client-id` property is present, `devID` is also recorded.

---

## Connection Tracking – Table `DEVICE_CONN`

| Column           | Type                     | Description                                             |
|------------------|--------------------------|---------------------------------------------------------|
| `conn_id`        | INT UNSIGNED             | Auto‑incremented connection identifier                  |
| `devID`          | SMALLINT UNSIGNED        | Device ID (references `DEVICES.devID`)                  |
| `projID`         | VARCHAR(251)             | Project ID at connection time (snapshot)                |
| `connected_ts`   | TIMESTAMP                | Time of connection (online message or first message)    |
| `last_msg_ts`    | TIMESTAMP                | Time of last message from this device                   |
| `uptime_str`     | VARCHAR(32)              | Device uptime reported via `d_UPT` (root diagnostic)    |
| `disconnected_ts`| TIMESTAMP                | Time of disconnection (offline LWT or manual close)     |

### View `vw_CONN_LOG`

```sql
DROP VIEW IF EXISTS vw_CONN_LOG;
CREATE VIEW vw_CONN_LOG AS
WITH uptime_seconds AS (
    SELECT
        c.conn_id,
        c.devID,
        c.projID,
        c.connected_ts,
        c.last_msg_ts,
        c.uptime_str AS device_uptime,
        c.disconnected_ts,
        TIMESTAMPDIFF(SECOND, c.connected_ts, IFNULL(c.disconnected_ts, NOW())) AS duration_sec
    FROM DEVICE_CONN c
)
SELECT
    u.conn_id,
    d.devName,
    u.projID,
    u.connected_ts,
    u.last_msg_ts,
    u.device_uptime,
    u.disconnected_ts,
    CONCAT(
        FLOOR(u.duration_sec / 86400), 'd ',
        FLOOR((u.duration_sec % 86400) / 3600), 'h ',
        FLOOR((u.duration_sec % 3600) / 60), 'm'
    ) AS duration_str
FROM uptime_seconds u
JOIN DEVICES d ON u.devID = d.devID
ORDER BY u.connected_ts DESC;
```

---

## Firmware Information in `DEVICES`

The following columns are added to the `DEVICES` table and updated when a status message with `"fw"` object is received:

| Column           | Type          | Description                     |
|------------------|---------------|---------------------------------|
| `fw_version`     | VARCHAR(32)   | Version string (e.g., `v1.2.3`) |
| `fw_tag`         | VARCHAR(64)   | Git tag (e.g., `release-1.2.3`) |
| `fw_major`       | SMALLINT      | Major version number            |
| `fw_minor`       | SMALLINT      | Minor version number            |
| `fw_patch`       | SMALLINT      | Patch version number            |
| `fw_build`       | INT           | Build number                    |
| `fw_hash_short`  | VARCHAR(16)   | Short Git commit hash           |
| `fw_hash_full`   | VARCHAR(64)   | Full Git commit hash            |
| `fw_build_id`    | VARCHAR(64)   | Build identifier                |
| `fw_dirty`       | BOOLEAN       | Whether build had uncommitted changes |
| `project_name`   | VARCHAR(64)   | Project name (from firmware)    |

---

## Error Logging – Table `E_LOG`

| Column     | Type          | Description                                      |
|------------|---------------|--------------------------------------------------|
| `id`       | INT UNSIGNED  | Auto‑incremented log ID                         |
| `devID`    | SMALLINT      | Device ID (may be NULL if not extractable)      |
| `category` | CHAR(1)       | `'M'` for message/JSON errors, `'D'` for database errors |
| `topic`    | VARCHAR(255)  | MQTT topic of the erroneous message             |
| `payload`  | TEXT          | Raw message payload (up to 65535 chars)         |
| `ts`       | TIMESTAMP     | Timestamp of the error                          |
| `message`  | TEXT          | Error description                               |

- **Category `'M'`** : max 200 rows (oldest automatically deleted)
- **Category `'D'`** : max 1000 rows (oldest automatically deleted)

---

## Configuration (Environment Variables)

Create `/etc/mqtt2db/env`:

```env
MQTT_BROKER=raspi00
MQTT_PORT=8883
MQTT_USER=your_mqtt_username
MQTT_PASS=your_mqtt_password
MQTT_TLS_ENABLED=true
MQTT_TLS_CA_CERTS=/etc/mosquitto/certs/ca.crt
DB_HOST=localhost
DB_USER=your_db_user
DB_PASS=your_db_password
DB_NAME=IOT_DB
LOG_LEVEL=INFO
```

---

## Installing as a Systemd Service

1. Create the environment file (above).
2. Create `/etc/systemd/system/mqtt2db.service`:

```ini
[Unit]
Description=MQTT to MariaDB Bridge (MQTT5 + LWT + Firmware)
After=network.target mariadb.service mosquitto.service

[Service]
Type=simple
User=pi
EnvironmentFile=/etc/mqtt2db/env
WorkingDirectory=/home/pi/MyStuff/IOT_DB
ExecStart=/home/pi/MyStuff/IOT_DB/venv/bin/python /home/pi/MyStuff/IOT_DB/mqtt_to_db.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

3. Reload and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable mqtt2db.service
sudo systemctl start mqtt2db.service
```

---

## Logging and Debugging

- View live logs: `sudo journalctl -u mqtt2db.service -f`
- Set `LOG_LEVEL=DEBUG` in the environment file and restart for verbose output.
- JSON parsing errors: query `SELECT * FROM E_LOG WHERE category = 'M' ORDER BY ts DESC LIMIT 20;`
- Database errors: query `SELECT * FROM E_LOG WHERE category = 'D' ORDER BY ts DESC LIMIT 20;`

---

## Troubleshooting

| Symptom | Likely cause / check |
|---------|----------------------|
| Service won’t start | `sudo journalctl -u mqtt2db.service -n 50` |
| MQTT connection fails | Verify broker, port, TLS certs, credentials |
| Data not saved | `dS` invalid? `dPJ` empty? Topic not under `devices/.../data` or legacy `IOT_DB/DAT/`? |
| Root diagnostic not saved | No valid `dS` in root AND no child with valid `dS` |
| Child diagnostic not saved | Child’s `dS` invalid or parent was not created |
| Uptime not recorded | Missing `d_UPT` (or `d_dUPT`) in root block |
| Firmware not updated | Status message missing `"fw"` object or not published to `.../status` |
| `client-id` not recognised | Ensure MQTT 5.0 User Property is set; otherwise fallbacks (topic, `dNM`) are used |

---

*Documentation version 10.0 – full MQTT5 support, new topic hierarchy, firmware tracking.*