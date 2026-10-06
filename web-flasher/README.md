# Web flasher for the MQTT bridge (Heltec V3)

Not part of the upstream MeshCore project. This folder, `mqtt_public.ini` and
`.github/workflows/mqtt-bridge-flasher.yml` live only on the `web-flasher` branch of the fork.

## What it does

On every push to `web-flasher` (or "Run workflow" in the Actions tab) GitHub Actions:

1. runs the host tests of the bridge logic,
2. builds `Heltec_v3_repeater_bridge_mqtt` with the defaults of `mqtt_public.ini` and merges it into one flash image,
3. publishes `index.html`, `manifest.json`, `heltec_v3_mqtt_bridge.bin` and `SHA256SUMS.txt` to the
   `gh-pages` branch, which GitHub Pages serves.

The page uses [ESP Web Tools](https://esphome.github.io/esp-web-tools/) to flash from Chrome/Edge and
Web Serial to send the `set mqtt.*` commands (see `docs/mqtt_bridge.md`). Nothing typed on the page leaves
the browser except to the board over USB.

## One-time setup in the GitHub repository

1. **Actions** tab: if GitHub shows "Workflows aren't being run on this forked repository", click
   "I understand my workflows, go ahead and enable them".
2. Run the workflow once (push to `web-flasher`, or Actions > "MQTT bridge web flasher" > Run workflow).
   This creates the `gh-pages` branch.
3. **Settings > Pages**: Source "Deploy from a branch", branch `gh-pages`, folder `/ (root)`, Save.
4. The page is at `https://<user>.github.io/<repo>/` after a minute.

## Defaults baked into the image, and what is NOT

`mqtt_public.ini` sets only non-secret defaults: broker host, port 1883 and topic prefix `ktm`.
**No MQTT user name or password is stored in the repository or in the firmware.** Each user types the account the
maintainer sends them privately; it is saved in the board's flash (`/mqtt.cfg`).

To point the build at another broker, edit `mqtt_public.ini` and push.

## Share with a friend

Send the page link and, **privately** (never on the page or in the repo), the MQTT user name and password.
The friend flashes the board, connects, fills in the WiFi name/password plus the MQTT user/password, and presses the
send button. The page contains the full step-by-step guide (Vietnamese).
