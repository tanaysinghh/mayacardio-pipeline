package com.mayacardio.pipeline.device.collector;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReplyParserTest {

    private static String crc(String s) {
        CRC32 c = new CRC32();
        c.update(s.getBytes(StandardCharsets.UTF_8));
        return String.format("%08x", c.getValue());
    }

    private static String reply(String data, String crc) {
        return "{\"session_id\": \"5f0c2a9e\", \"sensor\": 1, \"first_index\": 120000, \"count\": 3, \"lost\": [],"
                + " \"session_end\": false, \"crc32\": \"" + crc + "\", \"data\": " + data + "}";
    }

    @Test
    void specExampleChecksumIsOverRawText() throws Exception {
        String data = "[[720, 1200, 1560], [708, 1180, 1534], [726, 1210, 1573]]";
        MockReply r = ReplyParser.parse(reply(data, crc(data)));
        assertEquals(120000, r.firstIndex());
        assertArrayEquals(new int[]{708, 1180, 1534}, r.rows()[1]);
    }

    @Test
    void compactAndOddWhitespaceIsChecksummedAsSent() throws Exception {
        String data = "[[720,1200,1560],[708 ,1180,1534],\t[726,1210,1573]]";
        MockReply r = ReplyParser.parse(reply(data, crc(data)));
        assertEquals(3, r.count());
        String reserialised = "[[720, 1200, 1560], [708, 1180, 1534], [726, 1210, 1573]]";
        assertThrows(ReplyParser.CorruptReplyException.class, () -> ReplyParser.parse(reply(data, crc(reserialised))));
    }

    @Test
    void uppercaseCrcAccepted() throws Exception {
        String data = "[[1, 2, 3], [4, 5, 6], [7, 8, 9]]";
        assertEquals(3, ReplyParser.parse(reply(data, crc(data).toUpperCase())).count());
    }

    @Test
    void rejectsCorruptionShapeAndRange() {
        String data = "[[720, 1200, 1560], [708, 1180, 1534], [726, 1210, 1573]]";
        String flipped = data.replace("708", "709");
        assertThrows(ReplyParser.CorruptReplyException.class, () -> ReplyParser.parse(reply(flipped, crc(data))));
        String shape = "[[720, 1200], [708, 1180, 1534], [726, 1210, 1573]]";
        assertThrows(ReplyParser.CorruptReplyException.class, () -> ReplyParser.parse(reply(shape, crc(shape))));
        String range = "[[720, 1200, 8388608], [708, 1180, 1534], [726, 1210, 1573]]";
        assertThrows(ReplyParser.CorruptReplyException.class, () -> ReplyParser.parse(reply(range, crc(range))));
        assertThrows(ReplyParser.CorruptReplyException.class, () -> ReplyParser.parse(reply(data, crc(data)).substring(0, 80)));
        assertThrows(ReplyParser.CorruptReplyException.class, () -> ReplyParser.parse("{\"error\": \"x\"}"));
    }

    @Test
    void lostEntriesWithNullEnd() throws Exception {
        String line = "{\"session_id\": \"ab\", \"sensor\": 2, \"first_index\": 4400, \"count\": 0, \"lost\": "
                + "[{\"from_index\": 4000, \"to_index\": 4399, \"reason\": \"buffer_overflow\"}, {\"from_index\": 9000, \"to_index\": null}],"
                + " \"session_end\": true, \"crc32\": \"" + crc("[]") + "\", \"data\": []}";
        MockReply r = ReplyParser.parse(line);
        assertEquals(2, r.lost().size());
        assertEquals(4399L, r.lost().get(0).toIndex());
        assertNull(r.lost().get(1).toIndex());
        assertTrue(r.sessionEnd());
    }

    @Test
    void dataFieldMayAppearBeforeOtherFields() throws Exception {
        String data = "[[1, 2]]";
        String line = "{\"data\": " + data + ", \"crc32\": \"" + crc(data) + "\", \"session_id\": \"x\", \"sensor\": 2, \"first_index\": 0, \"count\": 1, \"lost\": [], \"session_end\": false}";
        assertEquals(1, ReplyParser.parse(line).count());
    }

    @Test
    void crcAsNumberOrWithHexPrefix() throws Exception {
        String data = "[[1, 2, 3], [4, 5, 6], [7, 8, 9]]";
        long v = Long.parseLong(crc(data), 16);
        String numeric = reply(data, "X").replace("\"crc32\": \"X\"", "\"crc32\": " + v);
        assertEquals(3, ReplyParser.parse(numeric).count());
        assertEquals(3, ReplyParser.parse(reply(data, "0x" + crc(data))).count());
        assertThrows(ReplyParser.CorruptReplyException.class, () -> ReplyParser.parse(reply(data, "1" + crc(data))));
    }

    @Test
    void unknownFieldsIgnoredAndOddLostEntriesSkipped() throws Exception {
        String data = "[[1, 2]]";
        String line = "{\"extra\": {\"data\": [1]}, \"session_id\": \"x\", \"sensor\": 2, \"first_index\": \"40\", \"count\": \"1\","
                + " \"lost\": [{\"from\": 1, \"to\": 2}, 7, {\"from_index\": 10, \"to_index\": 5}, {\"from_index\": 20, \"to_index\": 39}],"
                + " \"note\": \"hi\", \"crc32\": \"" + crc(data) + "\", \"data\": " + data + "}";
        MockReply r = ReplyParser.parse(line);
        assertEquals(40, r.firstIndex());
        assertEquals(1, r.lost().size());
        assertEquals(20, r.lost().get(0).fromIndex());
        assertEquals("unspecified_by_mock", r.lost().get(0).reason());
    }

    @Test
    void missingSensorInferredFromRowWidthOrRequest() throws Exception {
        String d1 = "[[1, 2, 3]]";
        String line1 = "{\"session_id\": \"x\", \"first_index\": 0, \"crc32\": \"" + crc(d1) + "\", \"data\": " + d1 + "}";
        assertEquals(1, ReplyParser.parse(line1, 2).sensor());
        String d0 = "[]";
        String line0 = "{\"session_id\": \"x\", \"first_index\": 5, \"crc32\": \"" + crc(d0) + "\", \"data\": " + d0 + "}";
        assertEquals(2, ReplyParser.parse(line0, 2).sensor());
        assertThrows(ReplyParser.CorruptReplyException.class, () -> ReplyParser.parse(line0, -1));
    }
}
