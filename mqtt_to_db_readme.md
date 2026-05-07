# MQTT to MariaDB Bridge – Integrated Connection Tracking

## Overview

This service subscribes to MQTT topics under `IOT_DB/#` and stores JSON messages from ESP32 devices into a MariaDB database.
It also **tracks device connection sessions in real time** by subscribing to Mosquitto’s `$SYS/broker/log/N` topic. When a client connects or disconnects, the `DEVICE_CONN` table is updated immediately – no separate tracker service is needed.

Two message types are supported:

- **Data messages** (`IOT_DB/DAT/...`) – require `dNM`, `dPJ`, `dS` fields. Saved into project‑specific tables `dt_<projectID>`.
- **Diagnostic messages** (`IOT_DB/DIAG/...`) – require `dNM`, `dDGT` fields. Saved into diagnostic tables `dia_<diagnostic_type>`.

---

## MQTT Topics

| Topic pattern               | Purpose                                                   | Required JSON fields                     |
|-----------------------------|-----------------------------------------------------------|------------------------------------------|
| `IOT_DB/DAT/#`              | Sensor data (e.g., temperature, humidity)                 | `dNM`, `dPJ`, `dS`                       |
| `IOT_DB/DIAG/#`             | Diagnostic data (e.g., CPU temperature, memory usage)     | `dNM`, `dDGT`                            |

The service also subscribes to `$SYS/broker/log/N` for real‑time client connection/disconnection events.

---

## JSON Message Formats

### Data Messages (`IOT_DB/DAT/#`)

**Required fields:**

| Field | Description | Example |
|-------|-------------|---------|
| `dNM` | Device name (unique identifier, max 12 chars) | `"ESP_41:2F:6C"` |
| `dPJ` | Project ID (e.g., `"P001"`). If empty or missing, **no sensor data is saved** – only device record is updated. | `"P001"` |
| `dS`  | Save flag – one of: `"S"`, `"s"`, `"Y"`, `"y"`, `"1"`. Otherwise message is ignored. | `"S"` |

Any additional key‑value pairs are stored as dynamic columns prefixed with `d_` (e.g., `temperature` → `d_temperature`).

**Example:**

```json
{
  "dNM": "ESP_ABC123",
  "dPJ": "P002",
  "dS": "S",
  "temperature": 23.5,
  "humidity": 60
}
```

### Diagnostic Messages (`IOT_DB/DIAG/#`)

**Required fields:**

| Field | Description | Example |
|-------|-------------|---------|
| `dNM` | Device name (unique identifier) | `"ESP_41:2F:6C"` |
| `dDGT` | Diagnostic type – table name `dia_<type>` | `"cpu"`, `"memory"` |

All other fields are stored as dynamic columns (prefixed with `d_`). The device’s project ID (`devPROJID`) is **not** modified.

**Example:**

```json
{
  "dNM": "ESP_ABC123",
  "dDGT": "cpu",
  "temperature": 65.2,
  "load": 0.75
}
```

---

## Configuration (Environment Variables)

All settings are read from environment variables. Create a file `/etc/mqtt2db/env`:

```env
MQTT_BROKER=raspi00
MQTT_PORT=8883
MQTT_USER=edolis
MQTT_PASS=your_mqtt_password
MQTT_TLS_ENABLED=true
MQTT_TLS_CA_CERTS=/etc/mosquitto/certs/ca.crt
DB_HOST=localhost
DB_USER=pyBridge
DB_PASS=pyBridgeSpring
DB_NAME=IOT_DB
LOG_LEVEL=INFO
```

### Changing Username/Password

- MQTT authentication: edit `MQTT_USER` and `MQTT_PASS`.
- Database authentication: edit `DB_USER` and `DB_PASS`.

After changes, restart the service: `sudo systemctl restart mqtt2db.service`.

---

## Mosquitto Configuration for Connection Tracking

To enable the `$SYS/broker/log/N` topic, add the following line to your `/etc/mosquitto/mosquitto.conf`:

```ini
log_dest topic
```

If you already have `log_type all`, no further changes are needed. Restart Mosquitto:

```bash
sudo systemctl restart mosquitto
```

This allows the bridge to receive real‑time connect/disconnect events.

---

## Connection Tracking – Table `DEVICE_CONN`

The bridge maintains a table `DEVICE_CONN` that records each device’s connection session:

- **`connected_ts`** – when the device connected (first message after a gap or broker‑reported connect).
- **`last_msg_ts`** – updated on every sensor or diagnostic message.
- **`disconnected_ts`** – set when the device disconnects (from `$SYS/broker/log/N`).
- **`projID`** – the project ID the device had at the time of connection (snapshot).

### Schema

```sql
CREATE TABLE DEVICE_CONN (
    conn_id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    devID SMALLINT UNSIGNED NOT NULL,
    projID VARCHAR(251) NULL,
    connected_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    last_msg_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    disconnected_ts TIMESTAMP NULL DEFAULT NULL,
    PRIMARY KEY (conn_id),
    INDEX (devID, last_msg_ts)
);
```

### View `vw_CONN_LOG` – human‑readable connection log

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

### Query examples

- **Current active connections**:
  ```sql
  SELECT * FROM vw_CONN_LOG WHERE disconnected_ts IS NULL;
  ```
- **History for a device**:
  ```sql
  SELECT * FROM vw_CONN_LOG WHERE devName = 'ESP_32:97:54';
  ```

---

## Database Schema Overview

| Table | Purpose |
|-------|---------|
| `DEVICES` | Device metadata (`devID`, `devName`, `devLastSeen`, `devPROJID`) |
| `dt_<projectID>` | Dynamic tables for sensor data (once per project) |
| `dia_<diagnostic_type>` | Dynamic tables for diagnostic data |
| `DEVICE_CONN` | Connection sessions (real‑time tracking via `$SYS` logs) |
| `E_LOG` | Error log (data type mismatches, truncation, etc.) |

---

## Installing as a Systemd Service

1. Create the environment file (see above).
2. Create the service file `/etc/systemd/system/mqtt2db.service`:

```ini
[Unit]
Description=MQTT to MariaDB Data Saver (with connection tracking)
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

- The service logs to **journald** (view with `journalctl -u mqtt2db.service -f`).
- Errors are also written to the `E_LOG` table in MariaDB.
- Connection events (connect/disconnect) are logged at `INFO` level when detected via `$SYS/broker/log/N`.

To view debug logs, set `LOG_LEVEL=DEBUG` in the environment file and restart.

---

## Troubleshooting

| Symptom | Likely cause / check |
|---------|----------------------|
| Service won’t start | `sudo journalctl -u mqtt2db.service -n 50` |
| MQTT connection fails | Verify `MQTT_BROKER`, `MQTT_PORT`, TLS cert path, credentials. |
| Data not saved | `dS` missing/invalid? `dPJ` empty? Topic not under `IOT_DB/DAT/`? |
| Diagnostic not saved | `dDGT` missing? Topic not under `IOT_DB/DIAG/`? |
| Connection table not updating | Ensure `log_dest topic` is set in Mosquitto and restarted. Subscribe manually: `mosquitto_sub -t '$SYS/broker/log/N'` |
| Disconnections not logged | Check Mosquitto log format; the script expects `Client <id> disconnected.` – adjust regex if needed. |

---

*Documentation version 5.0 – integrated connection tracking, no separate tracker service.*