package com.mayacardio.pipeline.core;

import com.fasterxml.jackson.databind.JsonNode;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;

public final class ResultJson {
    public static final String ONLINE = "online";
    public static final String OFFLINE = "offline";

    private ResultJson() {
    }

    public static String line(String session, String mode, WindowResult r, Map<String, String> timestamps) {
        StringBuilder sb = new StringBuilder(512);
        sb.append("{\"session\": ").append(Json.quote(session));
        sb.append(", \"window\": ").append(r.window());
        sb.append(", \"mode\": ").append(Json.quote(mode));
        sb.append(", \"missing_1\": ").append(r.missing1());
        sb.append(", \"missing_2\": ").append(r.missing2());
        sb.append(", \"complete\": ").append(r.complete());
        sb.append(", \"mean_1\": ").append(decimals(r.mean1()));
        sb.append(", \"min_1\": ").append(ints(r.min1()));
        sb.append(", \"max_1\": ").append(ints(r.max1()));
        sb.append(", \"mean_2\": ").append(decimals(r.mean2()));
        sb.append(", \"min_2\": ").append(ints(r.min2()));
        sb.append(", \"max_2\": ").append(ints(r.max2()));
        sb.append(", \"magnitude\": ").append(r.magnitude() == null ? "null" : r.magnitude().toPlainString());
        sb.append(", \"heart_rate\": ").append(r.heartRate() == null ? "null" : r.heartRate().toPlainString());
        if (timestamps != null) {
            for (Map.Entry<String, String> e : timestamps.entrySet()) {
                sb.append(", ").append(Json.quote(e.getKey())).append(": ")
                        .append(e.getValue() == null ? "null" : Json.quote(e.getValue()));
            }
        }
        return sb.append('}').toString();
    }

    public static WindowResult parse(JsonNode n) {
        return new WindowResult(
                n.get("window").asLong(),
                n.get("missing_1").asInt(),
                n.get("missing_2").asInt(),
                decimalArray(n.get("mean_1")),
                intArray(n.get("min_1")),
                intArray(n.get("max_1")),
                decimalArray(n.get("mean_2")),
                intArray(n.get("min_2")),
                intArray(n.get("max_2")),
                decimal(n.get("magnitude")),
                decimal(n.get("heart_rate")));
    }

    public static Map<String, String> timestamps(String... kv) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i + 1 < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    private static String decimals(BigDecimal[] a) {
        if (a == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < a.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(a[i].toPlainString());
        }
        return sb.append(']').toString();
    }

    private static String ints(int[] a) {
        if (a == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < a.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(a[i]);
        }
        return sb.append(']').toString();
    }

    private static BigDecimal decimal(JsonNode n) {
        if (n == null || n.isNull()) {
            return null;
        }
        return n.decimalValue().setScale(1, Rounding.MODE);
    }

    private static BigDecimal[] decimalArray(JsonNode n) {
        if (n == null || n.isNull()) {
            return null;
        }
        BigDecimal[] out = new BigDecimal[n.size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = n.get(i).decimalValue().setScale(1, Rounding.MODE);
        }
        return out;
    }

    private static int[] intArray(JsonNode n) {
        if (n == null || n.isNull()) {
            return null;
        }
        int[] out = new int[n.size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = n.get(i).asInt();
        }
        return out;
    }
}
