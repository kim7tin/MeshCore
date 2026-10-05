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

Use the env `Heltec_v3_repeater_bridge_mqtt` as a template and set the flags:

| Flag | Meaning |
|---|---|
| `MQTT_WIFI_SSID`, `MQTT_WIFI_PASSWORD` | WiFi |
| `MQTT_TOPIC_PREFIX` | Topic prefix shared by all bridged repeaters (default `ktm`) |
| `MQTT_BROKER1_HOST/PORT/USER/PASS` | First broker (plain MQTT 3.1.1) |
| `MQTT_BROKER2_HOST/PORT/USER/PASS` | Optional second broker |

`bridge.enabled on|off` (existing CLI) starts/stops the bridge.

## Limitations (v1)

- Adverts are not bridged: **both nodes must add each other as contacts manually** (QR / public key).
- A standalone flood ACK, TRACE, CONTROL and MULTIPART packets are not bridged.
- Plain MQTT only (no TLS); use a private broker or a VPN, and broker ACLs on `<prefix>/#`.
- Every Public-channel message from another area is re-transmitted by every subscribed repeater
  in range: airtime grows with the number of linked areas. Keep the group rate limit low.
- Connecting to a broker can block the main loop for up to 2 s per attempt; attempts back off from
  1 s to 60 s while a broker is unreachable.
- The repeater keeps working as a normal LoRa repeater when WiFi or the brokers are down.
