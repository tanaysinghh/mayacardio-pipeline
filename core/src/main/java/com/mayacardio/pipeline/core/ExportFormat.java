package com.mayacardio.pipeline.core;

public final class ExportFormat {

    private ExportFormat() {
    }

    public static String sampleLine(int sensor, long index, int[] values) {
        StringBuilder sb = new StringBuilder(64);
        sb.append("{\"sensor\": ").append(sensor).append(", \"index\": ").append(index).append(", \"values\": [");
        for (int c = 0; c < values.length; c++) {
            if (c > 0) {
                sb.append(", ");
            }
            sb.append(values[c]);
        }
        return sb.append("]}").toString();
    }

    public static String lossLine(LossRange loss) {
        return "{\"sensor\": " + loss.sensor() + ", \"from_index\": " + loss.fromIndex()
                + ", \"to_index\": " + (loss.toIndex() == null ? "null" : loss.toIndex().toString())
                + ", \"reason\": " + Json.quote(loss.reason()) + "}";
    }
}
