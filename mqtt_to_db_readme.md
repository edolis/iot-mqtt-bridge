# MQTT to MariaDB Bridge – MQTT 5.0 Client Identification

## Overview

This service subscribes to MQTT topics under `IOT_DB/#` and stores JSON messages from ESP32 devices into a MariaDB database.
It uses **MQTT 5.0 User Properties** to reliably identify the sending client (the ESP32) via a `client-id` property. This guarantees that even malformed JSON payloads can be attributed to the correct device, and all errors logged in `E_LOG` have a valid `devID`.

Two message types are supported:

- **Data messages** (`IOT_DB/DAT/...`) – require `dNM`, `dPJ`, `dS` fields. Saved into project‑specific tables `dt_<projectID>`.
- **Diagnostic messages** (`IOT_DB/DIAG/...`) – store diagnostic information. A root diagnostic block is **always saved**; additional blocks inside a `diagnostics` array can be saved conditionally.

The service also subscribes to `$SYS/broker/log/N` for real‑time client connection/disconnection events.

---

## MQTT 5.0 Client Identification

Each message published by an ESP32 must include a User Property with key `"client-id"` and value equal to the device’s name (e.g., `"ESP_32:97:54"`). The bridge reads this property from every incoming message and uses it to:

- Identify the device when the JSON payload is malformed (so that `devID` can still be logged).
- Fallback when the JSON field `dNM` is missing.

On the ESP32 (using ESP‑IDF MQTT 5.0 client), the code to add the property is:

```c
esp_mqtt5_user_property_handle_t user_property = NULL;
esp_mqtt5_client_set_user_property(NULL, &user_property, "client-id", client_id);
esp_mqtt_client_publish(client, topic, payload, 0, qos, retain, user_property);
```

---

## MQTT Topics

| Topic pattern          | Purpose                                                   | Required fields                     |
|-----------------------|-----------------------------------------------------------|-------------------------------------|
| `IOT_DB/DAT/#`        | Sensor data (e.g., temperature, humidity)                 | `dNM`, `dPJ`, `dS`                  |
| `IOT_DB/DIAG/#`       | Diagnostic data (e.g., system health, memory stats)       | `dNM`, `dDGT` (root block)          |

The service also subscribes to `$SYS/broker/log/N` for real‑time client connection/disconnection events.

---

## JSON Message Formats

### Data Messages (`IOT_DB/DAT/#`)

**Required fields:**

| Field | Description | Example |
|-------|-------------|---------|
| `dNM` | Device name (may be omitted, bridge uses client‑id instead) | `"ESP_41:2F:6C"` |
| `dPJ` | Project ID – if empty, **no sensor data is saved** | `"P001"` |
| `dS`  | Save flag (must be `S`, `s`, `Y`, `y`, `1`) | `"S"` |

All other keys that start with a configured prefix (default `"d_"`) are stored as dynamic columns (prefix stripped). Other keys are ignored.

**Example:**

```json
{
  "dNM": "ESP_ABC123",
  "dPJ": "P002",
  "dS": "S",
  "d_temperature": 23.5,
  "d_humidity": 60
}
```

This creates/inserts into `dt_P002` with columns `temperature` and `humidity`.

---

### Diagnostic Messages (`IOT_DB/DIAG/#`)

Diagnostic messages can contain a **root block** (always saved) and **additional blocks** inside a `diagnostics` array (saved only if they have `dS` set to a valid save flag).

#### Root block (always saved)

| Field | Description | Example |
|-------|-------------|---------|
| `dNM` | Device name (optional – client‑id is used as fallback) | `"ESP_32:97:54"` |
| `dDGT` | Diagnostic type – table name `dia_<type>` | `"DTF"` |
| (other fields) | Any keys with the configured prefix become dynamic columns | `d_UPT: "0d11h59m"` |

**Note:** The root block does **not** require a `dS` field – it is always stored.

#### Additional blocks (inside `diagnostics` array)

Each entry in the array is a JSON object containing:

| Field | Description | Example |
|-------|-------------|---------|
| `dDGT` | Child diagnostic type – table name `dia_<parent>_<child>` | `"DTM"` |
| `dS`  | Save flag – if not set or invalid, the entry is skipped | `"S"` |
| (other fields) | Dynamic fields (prefix stripped) become columns in the child table | `d_hfree: 219240` |

#### Example: Single message with root + one additional diagnostic

```json
{
  "dNM": "ESP_32:97:54",
  "dDGT": "DTF",
  "d_UPS": 43168,
  "d_UPT": "0d11h59m",
  "d_rssi_dbm": -70,
  "d_rssi_ID": "Edolis",
  "diagnostics": [
    {
      "dDGT": "DTM",
      "dS": "S",
      "d_hfree": 219240,
      "d_hlarge": 172032
    }
  ]
}
```

**What happens:**

- The root block is always saved: a new row is inserted into `dia_DTF` with the dynamic fields `UPS`, `UPT`, `rssi_dbm`, `rssi_ID`.
- The additional block is saved **only if** `dS` is `"S"`. A new row is inserted into `dia_DTF_DTM` with a `parent_id` column that stores the `id` of the root row.

---

## Database Schema

### Diagnostic Tables

#### Parent table (e.g., `dia_DTF`)

Created automatically when the first root diagnostic of type `DTF` arrives.

```sql
CREATE TABLE dia_DTF (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    deviceID SMALLINT UNSIGNED NOT NULL,
    ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    -- dynamic columns (e.g., UPS, UPT, rssi_dbm, ...)
    PRIMARY KEY (id)
);
```

#### Child table (e.g., `dia_DTF_DTM`)

Created automatically when the first additional diagnostic of type `DTM` arrives under parent `DTF`.

```sql
CREATE TABLE dia_DTF_DTM (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    deviceID SMALLINT UNSIGNED NOT NULL,
    parent_id INT UNSIGNED NOT NULL,
    ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    -- dynamic columns (e.g., hfree, hlarge, ...)
    PRIMARY KEY (id)
);
```

- `parent_id` references the `id` in `dia_DTF`, linking child data to the parent message.

### Error Log Table (`E_LOG`)

The bridge logs two categories of errors:

- **`M` (Message errors)** – JSON parsing failures (malformed payloads). These now always include a valid `devID` because the bridge extracts the client ID from the MQTT 5.0 User Property. The raw payload and MQTT topic are also stored. At most 200 rows are kept.
- **`D` (Database errors)** – insertion failures, data too long, out of range, stored procedure errors. At most 1000 rows are kept.

Table structure:

```sql
CREATE TABLE E_LOG (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    devID SMALLINT UNSIGNED NULL,
    category CHAR(1) NOT NULL DEFAULT 'D',
    topic VARCHAR(255) NULL,
    payload TEXT NULL,
    ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    message TEXT NOT NULL,
    PRIMARY KEY (id),
    INDEX idx_category_ts (category, ts)
);
```

To view recent message errors (JSON parse failures):

```sql
SELECT * FROM E_LOG WHERE category = 'M' ORDER BY ts DESC LIMIT 20;
```

### Other Tables

| Table | Purpose |
|-------|---------|
| `DEVICES` | Device metadata (`devID`, `devName`, `devLastSeen`, `devPROJID`) |
| `DEVICE_CONN` | Connection sessions (tracked via `$SYS` logs) |
| `dt_<projectID>` | Dynamic tables for sensor data |
| `dia_<type>` | Parent diagnostic tables |
| `dia_<parent>_<child>` | Child diagnostic tables (with `parent_id`) |

### View `vw_CONN_LOG` for connection history

```sql
CREATE OR REPLACE VIEW vw_CONN_LOG AS
WITH uptime_seconds AS (
    SELECT
        c.conn_id,
        c.devID,
        c.projID,
        c.connected_ts,
        c.last_msg_ts,
        c.disconnected_ts,
        TIMESTAMPDIFF(SECOND, c.connected_ts, IFNULL(c.disconnected_ts, NOW())) AS uptime_sec
    FROM DEVICE_CONN c
)
SELECT
    u.conn_id,
    d.devName,
    u.projID,
    u.connected_ts,
    u.last_msg_ts,
    u.disconnected_ts,
    CONCAT(
        FLOOR(u.uptime_sec / 86400), 'd ',
        FLOOR((u.uptime_sec % 86400) / 3600), 'h ',
        FLOOR((u.uptime_sec % 3600) / 60), 'm'
    ) AS uptime_str
FROM uptime_seconds u
JOIN DEVICES d ON u.devID = d.devID
ORDER BY u.connected_ts DESC;
```

### Query example: join parent and child tables

To retrieve child diagnostic data along with the parent’s timestamp:

```sql
SELECT
    p.ts AS parent_ts,
    c.*
FROM dia_DTF_DTM c
JOIN dia_DTF p ON c.parent_id = p.id
WHERE p.deviceID = 1;
```

---

## Configuration (Environment Variables)

All settings are read from environment variables. Create a file `/etc/mqtt2db/env`:

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

### Changing Prefixes or Array Field Name

Inside the Python script you can modify:

- `DYNAMIC_FIELD_PREFIXES = ["d_"]` – add more prefixes if needed.
- `DIAG_ARRAY_FIELD = "diagnostics"` – rename the array key.

After changes, restart the service.

---

## Mosquitto Configuration for Connection Tracking

To enable the `$SYS/broker/log/N` topic, add the following line to your `/etc/mosquitto/mosquitto.conf`:

```ini
log_dest topic
```

Restart Mosquitto:

```bash
sudo systemctl restart mosquitto
```

---

## Installing as a Systemd Service

1. Create the environment file (see above).
2. Create the service file `/etc/systemd/system/mqtt2db.service`:

```ini
[Unit]
Description=MQTT to MariaDB Data Saver with client-id User Property
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

3. Reload systemd and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable mqtt2db.service
sudo systemctl start mqtt2db.service
```

---

## Logging

- Service logs to **journald** (view with `journalctl -u mqtt2db.service -f`).
- **JSON parsing errors** (malformed payloads) are logged to `E_LOG` with category `'M'`. Thanks to the MQTT 5.0 `client-id` property, `devID` is always populated. The raw payload and MQTT topic are stored. At most 200 rows are kept.
- **Database errors** (insertion failures, truncation, etc.) are logged to `E_LOG` with category `'D'`, with a limit of 1000 rows.

To view debug logs, set `LOG_LEVEL=DEBUG` in the environment and restart.

---

## Troubleshooting

| Symptom | Likely cause / check |
|---------|----------------------|
| Service won’t start | `sudo journalctl -u mqtt2db.service -n 50` |
| MQTT connection fails | Verify `MQTT_BROKER`, `MQTT_PORT`, TLS cert path, credentials. |
| Data not saved | `dS` missing/invalid? `dPJ` empty? Topic not under `IOT_DB/DAT/`? |
| Diagnostic root not saved | `dDGT` missing? Topic not under `IOT_DB/DIAG/`? |
| Child diagnostic not saved | Check `dS` in array entry. |
| Malformed JSON errors logged | See `E_LOG` with `category = 'M'`; the client ID is always present. |
| Connection table not updating | Ensure `log_dest topic` is set in Mosquitto and restarted. |

---

*Documentation version 7.0 – MQTT 5.0 User Property `client-id` for robust device identification.*