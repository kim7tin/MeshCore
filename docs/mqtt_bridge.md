# MQTT bridge for repeaters

Links two (or more) areas that are out of radio range over MQTT, so that nodes in one area can
message nodes in another as if the repeaters were in range of each other.

Supported: ESP32 repeaters (`examples/simple_repeater`), build flag `WITH_MQTT_BRIDGE`.

## How it works

- **Neighbors.** For every flood packet the repeater hears it learns the nearby node (the last hop
  of the path, or the sender when the path is empty) and subscribes to `<prefix>/n/<hash>` for
  3 minutes (refreshed each time it hears that node again).
- **Publish.** When the repeater forwards a flood packet that has a destination hash, it publishes
  the packet (without its own hash) to `<prefix>/n/<dest hash>`. When it forwards a direct packet
  it publishes to the next hop in the path (or the destination at the last hop).
- **Receive.** A repeater subscribed to that topic re-transmits the packet like a normal repeater:
  flood packets get its own hash appended, direct packets go out as they are.
- **Public channel.** Group text on the Public channel (hash `0x11`) uses `<prefix>/g/11` and is
  rate limited (6 packets/min per direction by default).
- **Loops.** The MQTT message carries the sender's relay id (ignored by the sender), packets that
  came from MQTT are never published back, and the normal seen-table de-duplicates the rest.

`<hash>` is the first byte of the node hash as 2 lowercase hex characters.
MQTT message: `[version=1][flags=0][relay id, 4 bytes][raw packet as from Packet::writeTo]`,
QoS 0, no retain.

## Build

Use the env `Heltec_v3_repeater_bridge_mqtt` as a template. It contains no WiFi or broker details:
one firmware image works for everybody and is configured from the CLI (next section).

## Configure from the CLI

Use the serial console (or the remote admin CLI). Until a WiFi network **and** a first broker are
set, the bridge stays off and the repeater works as a normal LoRa repeater.

```
set mqtt.ssid <wifi name>            get mqtt.ssid
set mqtt.wifipass <wifi password>    get mqtt.wifipass     (shows ***)
set mqtt.host <broker host or ip>    get mqtt.host
set mqtt.port <1-65535>              get mqtt.port         (default 1883)
set mqtt.user <user>                 get mqtt.user
set mqtt.pass <password>             get mqtt.pass         (shows ***)
set mqtt.host2 / port2 / user2 / pass2     optional second broker
set mqtt.prefix <topic prefix>       get mqtt.prefix       (default meshcore, all bridged repeaters must agree)
set mqtt.group on|off                bridge the Public channel (default on)
get mqtt                             one-line status: WiFi and every broker up/down
```

- The value is the rest of the line, so names with spaces work (`set mqtt.ssid My Home WiFi`).
  An empty value clears a setting (`set mqtt.pass`).
- Every change restarts the bridge (if `bridge.enabled` is on) with the new settings.
- Settings are kept in `/mqtt.cfg` on the internal flash as plain `key=value` lines, **passwords
  included and not encrypted**. `get` never shows them.
- Typical session:

```
set mqtt.ssid My Home WiFi
set mqtt.wifipass my wifi password
set mqtt.host broker.example.org
set mqtt.user relay
set mqtt.pass broker password
get mqtt
```

The `MQTT_*` build flags (`MQTT_WIFI_SSID`, `MQTT_WIFI_PASSWORD`, `MQTT_TOPIC_PREFIX`,
`MQTT_BROKER1_HOST/PORT/USER/PASS`, `MQTT_BROKER2_*`) are optional defaults for the same settings;
anything stored with `set` takes precedence.

`bridge.enabled on|off` (existing CLI) starts/stops the bridge.

## Limitations (v1)

- Adverts are not bridged: **both nodes must add each other as contacts manually** (QR / public key).
- A standalone flood ACK, TRACE, CONTROL and MULTIPART packets are not bridged.
- Plain MQTT only (no TLS): the MQTT user name and password cross the network in clear text. Use a
  private broker or a VPN, and broker ACLs on `<prefix>/#`.
- Every Public-channel message from another area is re-transmitted by every subscribed repeater
  in range: airtime grows with the number of linked areas. Keep the group rate limit low.
- Connecting to a broker (DNS, TCP and the MQTT handshake) blocks the main loop while it runs, and
  at most one broker is tried per loop iteration. Attempts back off from 1 s to 60 s while a broker
  is unreachable. Subscriptions are queued and sent from the loop, never from the radio path, but
  a publish to a half-open connection can still stall the loop until the TCP timeout.
- Packets coming from MQTT are only accepted if they are of a type this bridge publishes, have
  payload version 1, and arrive on the topic that matches their destination or channel.
- The bridge ignores MQTT traffic while `repeat` is off.
- The repeater keeps working as a normal LoRa repeater when WiFi or the brokers are down.
