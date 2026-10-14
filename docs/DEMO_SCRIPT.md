# Demo video script (≤ 5 minutes)

Setup before recording: Pi 4 running the official mock and `pipeline.service`, online against the deployed cloud for a few minutes. Two terminals side by side on the recording machine (SSH into the Pi), plus a browser tab or third terminal for the cloud.

Terminal A (left), a live status line:

```sh
watch -n 1 "pipeline status | cut -c1-220"
```

Terminal B (right): commands below. Keep a phone camera or second camera on the Pi for the power-cut part.

| Time | Show | Say (short) |
|---|---|---|
| 0:00–0:30 | Terminal A: `"mode":"online"`, `samples_saved` rising, `backlog` small. Terminal B: `curl -s -H "Authorization: Bearer $T" $CLOUD/api/v1/sessions/$(pipeline status | jq -r .session)/status` | Collector saves every mock reply in one SQLite transaction; a heartbeat every 2 s decides online/offline; the cloud computes one result per second. |
| 0:30–0:50 | `pipeline results --session current --from cloud --mode online | tail -2` | Results in the cloud carry `computed_at`; they appear about 1 s after the window ends. |
| 0:50–2:00 | **Network cut**: `sudo ip link set wlan0 down` (or unplug Ethernet). Watch Terminal A flip to `"mode":"offline"` within ~6 s, `backlog` grows, `offline_windows` grows. Wait ~40 s. `sudo ip link set wlan0 up`. Watch `"mode":"online"` within a few seconds and `backlog` drain. | Offline: still saving, results computed on the device with the same code. Back online: live data first, the missed range is reconciled and sent in gzip batches. |
| 2:00–2:20 | `pipeline results --session current | grep '"offline"' | tail -1` | Offline results for the outage windows exist on the device and are uploaded too. |
| 2:20–3:10 | **Process kill**: `sudo systemctl kill -s SIGKILL pipeline`; `journalctl -u pipeline -n 3 -f` shows systemd restarting it after 1 s. Terminal A shows a new `pid` and `samples_saved` continuing. Then `pipeline sessions` shows `loss_ranges: 0`. | The restarted daemon resumes from the last committed index with `from_index`; the mock's 10 s buffer covers the gap, so nothing is lost. |
| 3:10–4:30 | **Power cut**: pull the Pi's power cable on camera. Plug it back. After boot (~40 s, can be cut in the edit), `pipeline sessions`: old session `closed`, tail loss with `to_index: null`. `pipeline export --session <old> --from device | tail -2` shows the loss lines. | Committed data is fsynced (WAL, synchronous=FULL). The mock also restarted, so its unread data is gone; the pipeline lists exactly which range was lost and why. At most about a second per sensor. |
| 4:30–5:00 | `pipeline status` online again, `backlog` back to about one second. Optional: run the official `check_ground_truth` on an export. | Summary: no silent loss, losses declared with reasons, cloud copy complete after catch-up. |

Tips: rehearse once; keep commands in a text file to paste; trim boot time in the edit; keep the final cut under 5:00.
