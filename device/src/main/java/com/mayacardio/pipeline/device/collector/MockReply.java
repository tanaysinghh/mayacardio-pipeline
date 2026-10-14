package com.mayacardio.pipeline.device.collector;

import com.mayacardio.pipeline.core.LossRange;

import java.util.List;

public record MockReply(String sessionId, int sensor, long firstIndex, int count, List<LossRange> lost,
                        boolean sessionEnd, int[][] rows) {
}
