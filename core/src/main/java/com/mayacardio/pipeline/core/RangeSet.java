package com.mayacardio.pipeline.core;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public final class RangeSet implements Iterable<long[]> {
    private final TreeMap<Long, Long> ranges = new TreeMap<>();

    public static RangeSet of(List<long[]> list) {
        RangeSet set = new RangeSet();
        for (long[] r : list) {
            set.add(r[0], r[1]);
        }
        return set;
    }

    public RangeSet copy() {
        RangeSet c = new RangeSet();
        c.ranges.putAll(ranges);
        return c;
    }

    public void add(long from, long to) {
        if (to < from) {
            return;
        }
        long start = from;
        long end = to;
        Map.Entry<Long, Long> lower = ranges.floorEntry(from);
        if (lower != null && (lower.getValue() == Long.MAX_VALUE || lower.getValue() + 1 >= from)) {
            start = lower.getKey();
            end = Math.max(end, lower.getValue());
            ranges.remove(lower.getKey());
        }
        while (true) {
            Long nextKey = ranges.ceilingKey(start);
            if (nextKey == null) {
                break;
            }
            if (end != Long.MAX_VALUE && nextKey > end + 1) {
                break;
            }
            end = Math.max(end, ranges.get(nextKey));
            ranges.remove(nextKey);
        }
        ranges.put(start, end);
    }

    public void remove(long from, long to) {
        if (to < from) {
            return;
        }
        List<long[]> updates = new ArrayList<>();
        Long key = ranges.floorKey(from);
        if (key == null) {
            key = ranges.ceilingKey(from);
        }
        while (key != null && key <= to) {
            long s = key;
            long e = ranges.get(key);
            Long next = ranges.higherKey(key);
            if (e >= from) {
                ranges.remove(key);
                if (s < from) {
                    updates.add(new long[]{s, from - 1});
                }
                if (e > to) {
                    updates.add(new long[]{to + 1, e});
                }
            }
            key = next;
        }
        for (long[] u : updates) {
            ranges.put(u[0], u[1]);
        }
    }

    public boolean contains(long idx) {
        Map.Entry<Long, Long> e = ranges.floorEntry(idx);
        return e != null && e.getValue() >= idx;
    }

    public boolean containsAll(long from, long to) {
        if (to < from) {
            return true;
        }
        Map.Entry<Long, Long> e = ranges.floorEntry(from);
        return e != null && e.getValue() >= to;
    }

    public boolean intersects(long from, long to) {
        Map.Entry<Long, Long> e = ranges.floorEntry(to);
        return e != null && e.getValue() >= from;
    }

    public RangeSet minus(RangeSet other) {
        RangeSet out = copy();
        for (long[] r : other) {
            out.remove(r[0], r[1]);
        }
        return out;
    }

    public RangeSet clip(long from, long to) {
        RangeSet out = new RangeSet();
        for (long[] r : this) {
            long s = Math.max(r[0], from);
            long e = Math.min(r[1], to);
            if (s <= e) {
                out.ranges.put(s, e);
            }
        }
        return out;
    }

    public long count() {
        long total = 0;
        for (Map.Entry<Long, Long> e : ranges.entrySet()) {
            if (e.getValue() == Long.MAX_VALUE) {
                return Long.MAX_VALUE;
            }
            total += e.getValue() - e.getKey() + 1;
        }
        return total;
    }

    public boolean isEmpty() {
        return ranges.isEmpty();
    }

    public int size() {
        return ranges.size();
    }

    public Long first() {
        return ranges.isEmpty() ? null : ranges.firstKey();
    }

    public Long last() {
        return ranges.isEmpty() ? null : ranges.lastEntry().getValue();
    }

    public List<long[]> toList() {
        List<long[]> out = new ArrayList<>(ranges.size());
        for (Map.Entry<Long, Long> e : ranges.entrySet()) {
            out.add(new long[]{e.getKey(), e.getValue()});
        }
        return out;
    }

    @Override
    public Iterator<long[]> iterator() {
        return toList().iterator();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");
        for (Map.Entry<Long, Long> e : ranges.entrySet()) {
            if (sb.length() > 1) {
                sb.append(", ");
            }
            sb.append(e.getKey()).append('-').append(e.getValue() == Long.MAX_VALUE ? "inf" : e.getValue().toString());
        }
        return sb.append(']').toString();
    }
}
