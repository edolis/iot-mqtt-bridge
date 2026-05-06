# MQTT to MariaDB Bridge – Connection Tracking Edition

## Overview

This service subscribes to MQTT topics under `IOT_DB/#` and stores JSON messages from ESP32 devices into a MariaDB database.
It also tracks **device connection sessions**: each time a device (re)connects (or resumes sending after a timeout), a new session is created with a unique connection ID. That session’s `last_msg_ts` is updated on every subsequent message, and the session is automatically closed after a defined inactivity period.

Two message types are supported:

- **Data messages** (`IOT_DB/DAT/...`) – require `dNM`, `dPJ`, `dS` fields. Saved into project‑specific tables `dt_<projectID>`.
- **Diagnostic messages** (`IOT_DB/DIAG/...`) – require `dNM`, `dDGT` fields. Saved into diagnostic tables `dia_<diagnostic_type>`.

---

## MQTT Topics

| Topic pattern               | Purpose                                                   | Required JSON fields                     |
|-----------------------------|-----------------------------------------------------------|------------------------------------------|
| `IOT_DB/DAT/#`              | Sensor data (e.g., temperature, humidity)                 | `dNM`, `dPJ`, `dS`                       |
| `IOT_DB/DIAG/#`             | Diagnostic data (e.g., CPU temperature, memory usage)     | `dNM`, `dDGT`                            |

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

```
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

## Connection Tracking – Table `DEVICE_CONN`

The bridge maintains a table `DEVICE_CONN` that records each device’s connection session. A new session is created when:

- The device sends its first message after the service starts, or
- The device resumes sending after an inactivity period longer than `SESSION_TIMEOUT_MINUTES` (default 30 minutes).

Every message updates the `last_msg_ts` of the current active session. When the timeout expires, the session is closed (`disconnected_ts` set) and the next message opens a new session.

### Schema

```sql
CREATE TABLE DEVICE_CONN (
    conn_id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    devID SMALLINT UNSIGNED NOT NULL,
    connected_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    last_msg_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    disconnected_ts TIMESTAMP NULL DEFAULT NULL,
    PRIMARY KEY (conn_id),
    INDEX (devID, last_msg_ts)
);
```

### Query examples

**Current active connections** (devices still sending):

```sql
SELECT d.devName, c.* FROM DEVICE_CONN c
JOIN DEVICES d ON c.devID = d.devID
WHERE c.disconnected_ts IS NULL;
```

**Connection history for a device**:

```sql
SELECT * FROM DEVICE_CONN WHERE devID = 1 ORDER BY connected_ts DESC;
```

**Devices that haven't sent a message in the last hour** (i.e., the active connection’s `last_msg_ts` is old):

```sql
SELECT d.devName, c.last_msg_ts
FROM DEVICE_CONN c JOIN DEVICES d ON c.devID = d.devID
WHERE c.disconnected_ts IS NULL
  AND c.last_msg_ts < NOW() - INTERVAL 1 HOUR;
```

---

## Database Schema Overview

| Table | Purpose |
|-------|---------|
| `DEVICES` | Device metadata (`devID`, `devName`, `devLastSeen`, `devPROJID`) |
| `dt_<projectID>` | Dynamic tables for sensor data (once per project) |
| `dia_<diagnostic_type>` | Dynamic tables for diagnostic data |
| `DEVICE_CONN` | Connection sessions (first/last message timestamps per connection) |
| `E_LOG` | Error log (data type mismatches, truncation, etc.) |

---

## Installing as a Systemd Service

1. Create the environment file (see above).
2. Create the service file `/etc/systemd/system/mqtt2db.service`:

```ini
[Unit]
Description=MQTT to MariaDB Data Saver
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
- Connection timeouts and session creations are logged at `DEBUG` level.

To view debug logs, set `LOG_LEVEL=DEBUG` in the environment file and restart.

---

## Customising the Session Timeout

In the Python script, find the line:

```python
SESSION_TIMEOUT_MINUTES = 30
```

Change the value (in minutes) and restart the service.

To make it configurable via environment variable (optional), replace with:

```python
SESSION_TIMEOUT_MINUTES = int(os.getenv("SESSION_TIMEOUT_MINUTES", "30"))
```

Then add `SESSION_TIMEOUT_MINUTES=45` to the environment file.

---

## Troubleshooting

| Symptom | Likely cause / check |
|---------|----------------------|
| Service won’t start | `sudo journalctl -u mqtt2db.service -n 50` |
| MQTT connection fails | Verify `MQTT_BROKER`, `MQTT_PORT`, TLS cert path, credentials. |
| Data not saved | `dS` missing/invalid? `dPJ` empty? Topic not under `IOT_DB/DAT/`? |
| Diagnostic not saved | `dDGT` missing? Topic not under `IOT_DB/DIAG/`? |
| Connection table not updating | Check that `DEVICE_CONN` exists; verify script version. |
| Timeout not closing sessions | The timeout is checked only when a new message arrives. Older sessions remain open if the device stops sending. |

---

*Documentation version 4.0 – connection tracking edition, compatible with the bridge script that includes `DEVICE_CONN` and session timeouts.*