SPEC (assessment text, condensed but complete)

Problem: A bedside device records two sensors continuously, for hours. Hospital WiFi drops without warning, power can be cut, nobody watches it. Build a pipeline on a Raspberry Pi-class ARM64 Linux board (max 4 CPU cores, 2 GB RAM) plus a free cloud service that: never loses data (every sample saved on device); processes live in the cloud when the network is available (online mode); keeps working offline by processing on the device with the same logic (offline mode), then sends everything to the cloud when the network returns. The mock service and the pipeline both run on the board. Judging runs on a real Raspberry Pi 4 and the participant's cloud account.

Requirements
1. Collect and save: read both sensors from the mock continuously; save every sample to device disk; saved data survives program crashes, reboots and sudden power cuts.
2. Online mode: stream to cloud continuously; cloud produces a result for each 1-second window within 5 seconds.
3. Offline mode: keep collecting and saving; process on the device with the same logic.
4. Switch automatically: to offline when the network is lost, back to online when it returns, each within 10 seconds; no sample lost or duplicated because of a switch.
5. Catch up: when the network returns, send every saved sample the cloud lacks so the cloud has a complete copy with every sample exactly once. Outages of up to 2 hours; cloud copy complete within 15 minutes after the network returns.
6. Handle loss honestly: if samples are lost, record exactly which (sensor, index range) and why. Never fill a gap with invented values.

Processing logic (identical online and offline)
Windows are one second: window w holds samples 400w to 400w+399 of each sensor. Per window output:
- missing_1, missing_2: number of samples of sensor 1 and sensor 2 not available in the window
- complete: true if both are 0
- mean, min, max: per channel, over available samples (null if none)
- magnitude: mean of sqrt(ch1^2 + ch2^2) over the available sensor 2 samples
- heart_rate: BPM from sensor 1 channel 2 (rule below)
- mode: "online" (processed in cloud) or "offline" (processed on device)
Heart-rate rule:
1. Sample i is a peak when ALL hold: the 800 samples before it and the sample after it are available; it is larger than both neighbours; it is larger than half of the maximum of the 800 samples before it; it is at least 100 samples after the previous peak.
2. For window w, take the peaks in the last 5 seconds (samples 400(w-4) to 400(w+1)-1). heart_rate = 60 / (mean gap between consecutive peaks, in seconds).
3. heart_rate is null if w < 4, if any sample in those 5 seconds is missing, or if there are fewer than 2 peaks.
Round decimal results to 1 decimal place.

Completion criteria (judges test automatically)
1. Every sample the device received is on disk exactly once, unchanged (compared to mock ground truth).
2. Every sample not on disk is listed as lost with a reason; no silent loss.
3. Data survives a crash, a reboot and a power cut. A power cut loses at most a stated amount (in seconds) that we must state.
4. Online: each second's result appears in the cloud within 5 s (results timestamps).
5. Mode switches within 10 s of the network changing, without loss or duplication.
6. After the network returns (outages up to 2 h), cloud copy complete within 15 minutes.
7. Results follow the processing logic exactly (compared to a reference implementation).
8. Runs within ARM64, at most 4 cores and 2 GB, with CPU, memory, disk and network use reported.
Scoring (100): data safety on device (1-3) = 35; online mode and switching (4-5) = 25; complete cloud copy (6) = 20; correct results (7) = 10; design note, code quality, resource use (8) = 10. Deductions: 5 points per silent loss or unflagged wrong value, up to 30.

Required commands (one program named "pipeline", on the device)
- pipeline status: one JSON line with current mode, samples saved, samples in the cloud, backlog.
- pipeline export --session ID --from device|cloud: JSON lines, one per sample, {"sensor": 1, "index": 120000, "values": [720, 1200, 1560]}, then one line per loss, {"sensor": 2, "from_index": 4000, "to_index": 4399, "reason": "..."}.
- pipeline results --session ID: one JSON line per window and mode with the processing outputs above.

Mock service ("biosignal-mock"), protocol
Command: biosignal-mock --mode signal --seed 7 --duration 3600 --port 7100. Newline-delimited JSON over TCP, one client at a time.
Data: Sensor 1 has 3 channels, sensor 2 has 2 channels. Both 400 samples/s, signed 24-bit integers, index from 0. Sample i of both sensors is at the same moment. Mode "signal": sensor 1 channel 2 is ECG-like with one sharp peak per beat (60 bpm minutes 0-2, 90 bpm minutes 2-4, 72 bpm minutes 4-6, repeating); sensor 1 channels 1 and 3 are channel 2 times 0.6 and 1.3; sensor 2 channel 1 near 400,000 and channel 2 near 150,000, varying slowly. Mode "ramp": sensor 1 channel c at sample i = (3i + c) mod 8388608; sensor 2 channel c = (2i + c) mod 8388608. Same seed gives same values.
Request: {"cmd": "read", "sensor": 1}
Reply (at most 1 second of data): {"session_id": "5f0c2a9e", "sensor": 1, "first_index": 120000, "count": 3, "lost": [], "session_end": false, "crc32": "f9a5bf62", "data": [[720, 1200, 1560], [708, 1180, 1534], [726, 1210, 1573]]}
Fields: session_id changes when the mock restarts and indexes restart from 0. lost lists ranges the mock could not deliver, e.g. {"from_index": 4000, "to_index": 4399, "reason": "buffer_overflow"}. session_end is true in the last reply of a session. crc32 is CRC-32 (zlib) of the data text exactly as sent, from "[" to the matching "]"; a wrong value means a corrupt reply that must not be used. So the client must capture the raw text of the data array and checksum that, never a re-serialised copy.
Re-read: {"cmd": "read", "sensor": 1, "from_index": 120000}
Buffer: the mock keeps only the last 10 seconds of each sensor; unread data is lost and reported in lost.
Faults judges inject: mock stops answering for a while; corrupt reply (wrong crc32); same reply sent twice; one sensor stops for a while (reported in lost); mock restarts giving a new session (unread old-session data is lost and must be listed, with "to_index": null when its end is unknown). Judges also cut the network, kill processes, pull the power, fill the disk, and change the system clock. A tool check_ground_truth (provided with the mock) compares an export with ground truth.

Rules: do NOT modify the mock; all data is synthetic; disclose AI tools used; free services and open-source software only.
Submission: code repo with a README to install and run on a fresh board; a design note of at most 2 pages (architecture; what is guaranteed and not; maximum data loss on a power cut and why); evidence of failure tests and results; resource figures over 1 hour in online mode (average CPU, peak memory, MB written to disk, MB sent over the network, and which environment was used); a demo video of at most 5 minutes showing a network cut, a process kill and a power cut.
