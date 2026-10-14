package com.mayacardio.pipeline.device.collector;

import com.fasterxml.jackson.databind.JsonNode;
import com.mayacardio.pipeline.core.Json;
import com.mayacardio.pipeline.core.LossRange;
import com.mayacardio.pipeline.core.Signal;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

public final class ReplyParser {

    public static final class CorruptReplyException extends Exception {
        public CorruptReplyException(String message) {
            super(message);
        }
    }

    private ReplyParser() {
    }

    public static MockReply parse(String line) throws CorruptReplyException {
        int[] span = topLevelValueSpan(line, "data");
        if (span == null) {
            throw new CorruptReplyException("no data field");
        }
        String dataText = line.substring(span[0], span[1]);
        if (dataText.isEmpty() || dataText.charAt(0) != '[') {
            throw new CorruptReplyException("data is not an array");
        }
        JsonNode n;
        try {
            n = Json.read(line);
        } catch (IllegalArgumentException e) {
            throw new CorruptReplyException("invalid json");
        }
        if (!n.isObject() || n.has("error")) {
            throw new CorruptReplyException("error reply: " + n.path("error").asText(""));
        }
        String crcText = n.path("crc32").asText("");
        long expected;
        try {
            expected = Long.parseLong(crcText.trim(), 16);
        } catch (NumberFormatException e) {
            throw new CorruptReplyException("bad crc32 field");
        }
        CRC32 crc = new CRC32();
        crc.update(dataText.getBytes(StandardCharsets.UTF_8));
        if (crc.getValue() != expected) {
            throw new CorruptReplyException("crc32 mismatch");
        }
        JsonNode sid = n.get("session_id");
        if (sid == null || !sid.isTextual() || sid.asText().isEmpty()) {
            throw new CorruptReplyException("missing session_id");
        }
        int sensor = n.path("sensor").asInt(-1);
        if (!Signal.validSensor(sensor)) {
            throw new CorruptReplyException("bad sensor");
        }
        JsonNode fi = n.get("first_index");
        if (fi == null || !fi.isIntegralNumber() || fi.asLong() < 0) {
            throw new CorruptReplyException("bad first_index");
        }
        JsonNode data = n.get("data");
        int channels = Signal.channels(sensor);
        int[][] rows = new int[data.size()][];
        for (int k = 0; k < rows.length; k++) {
            JsonNode row = data.get(k);
            if (!row.isArray() || row.size() != channels) {
                throw new CorruptReplyException("bad row shape");
            }
            int[] r = new int[channels];
            for (int c = 0; c < channels; c++) {
                JsonNode v = row.get(c);
                if (!v.isIntegralNumber() || !Signal.validValue(v.asLong())) {
                    throw new CorruptReplyException("bad value");
                }
                r[c] = (int) v.asLong();
            }
            rows[k] = r;
        }
        JsonNode countNode = n.get("count");
        if (countNode != null && countNode.isIntegralNumber() && countNode.asInt() != rows.length) {
            throw new CorruptReplyException("count does not match data");
        }
        List<LossRange> lost = new ArrayList<>();
        JsonNode lostNode = n.get("lost");
        if (lostNode != null && lostNode.isArray()) {
            for (JsonNode l : lostNode) {
                JsonNode from = l.get("from_index");
                if (from == null || !from.isIntegralNumber()) {
                    throw new CorruptReplyException("bad lost entry");
                }
                JsonNode to = l.get("to_index");
                Long toIdx = to == null || to.isNull() ? null : to.asLong();
                if (toIdx != null && toIdx < from.asLong()) {
                    throw new CorruptReplyException("bad lost range");
                }
                String reason = l.path("reason").asText("");
                lost.add(new LossRange(sensor, from.asLong(), toIdx, reason.isEmpty() ? "unspecified_by_mock" : reason));
            }
        }
        return new MockReply(sid.asText(), sensor, fi.asLong(), rows.length, lost, n.path("session_end").asBoolean(false), rows);
    }

    static int[] topLevelValueSpan(String s, String wanted) {
        int i = skipWs(s, 0);
        if (i >= s.length() || s.charAt(i) != '{') {
            return null;
        }
        i++;
        while (true) {
            i = skipWs(s, i);
            if (i >= s.length()) {
                return null;
            }
            if (s.charAt(i) == '}') {
                return null;
            }
            if (s.charAt(i) != '"') {
                return null;
            }
            int keyEnd = skipString(s, i);
            if (keyEnd < 0) {
                return null;
            }
            String key = s.substring(i + 1, keyEnd - 1);
            i = skipWs(s, keyEnd);
            if (i >= s.length() || s.charAt(i) != ':') {
                return null;
            }
            i = skipWs(s, i + 1);
            int valueStart = i;
            int valueEnd = skipValue(s, i);
            if (valueEnd < 0) {
                return null;
            }
            if (key.equals(wanted)) {
                return new int[]{valueStart, valueEnd};
            }
            i = skipWs(s, valueEnd);
            if (i < s.length() && s.charAt(i) == ',') {
                i++;
            }
        }
    }

    private static int skipWs(String s, int i) {
        while (i < s.length() && (s.charAt(i) == ' ' || s.charAt(i) == '\t' || s.charAt(i) == '\r' || s.charAt(i) == '\n')) {
            i++;
        }
        return i;
    }

    private static int skipString(String s, int i) {
        i++;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c == '\\') {
                i += 2;
                continue;
            }
            if (c == '"') {
                return i + 1;
            }
            i++;
        }
        return -1;
    }

    private static int skipValue(String s, int i) {
        if (i >= s.length()) {
            return -1;
        }
        char c = s.charAt(i);
        if (c == '"') {
            return skipString(s, i);
        }
        if (c == '[' || c == '{') {
            int depth = 0;
            while (i < s.length()) {
                char ch = s.charAt(i);
                if (ch == '"') {
                    i = skipString(s, i);
                    if (i < 0) {
                        return -1;
                    }
                    continue;
                }
                if (ch == '[' || ch == '{') {
                    depth++;
                } else if (ch == ']' || ch == '}') {
                    depth--;
                    if (depth == 0) {
                        return i + 1;
                    }
                }
                i++;
            }
            return -1;
        }
        while (i < s.length()) {
            char ch = s.charAt(i);
            if (ch == ',' || ch == '}' || ch == ']' || ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n') {
                break;
            }
            i++;
        }
        return i;
    }
}
