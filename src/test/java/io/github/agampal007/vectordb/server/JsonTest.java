package io.github.agampal007.vectordb.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.agampal007.vectordb.server.Json.JsonException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;

class JsonTest {

    @Test
    void parsesEveryKindOfValue() {
        Object parsed = Json.parse(
                " { \"a\" : [1, -2.5, 1e3, true, false, null], \"b\": {\"c\": \"d\"}, \"e\": [], \"f\": {} } ");
        Map<String, Object> expected = new LinkedHashMap<>();
        expected.put("a", Arrays.asList(1L, -2.5, 1000.0, true, false, null));
        expected.put("b", Map.of("c", "d"));
        expected.put("e", List.of());
        expected.put("f", Map.of());
        assertEquals(expected, parsed);
        assertNull(Json.parse("null"));
        assertEquals("x", Json.parse("\"x\""));
    }

    @Test
    void integersKeepAll64Bits() {
        // 2^53 + 1 is the first integer a double cannot hold.
        assertEquals(9_007_199_254_740_993L, Json.parse("9007199254740993"));
        assertEquals(Long.MAX_VALUE, Json.parse("9223372036854775807"));
        assertEquals(Long.MIN_VALUE, Json.parse("-9223372036854775808"));
        assertEquals(9.223372036854775808E18, Json.parse("9223372036854775808")); // too big: a double
        assertEquals(0L, Json.parse("0"));
        assertEquals(-0.0, Json.parse("-0.0"));
    }

    @Test
    void readsStringEscapes() {
        assertEquals("a\"b\\c/d\b\f\n\r\te\u00e9\ud83d\ude00", Json.parse(
                "\"a\\\"b\\\\c\\/d\\b\\f\\n\\r\\te\\u00e9\\ud83d\\ude00\""));
    }

    @Test
    void rejectsMalformedInput() {
        String[] bad = {
            "", " ", "{", "}", "[", "[1,]", "[1 2]", "{\"a\"}", "{\"a\":}", "{\"a\":1,}", "{a:1}", "{\"a\":1 \"b\":2}",
            "01", "1.", ".5", "-", "1e", "+1", "0x10", "NaN", "Infinity", "tru", "nul", "'x'", "\"x", "\"\\x\"",
            "\"\\u12\"", "\"\\u12zz\"", "\"line\nbreak\"", "1 2", "[1] x", "{} {}"
        };
        for (String text : bad) {
            assertThrows(JsonException.class, () -> Json.parse(text), "accepted: " + text);
        }
    }

    @Test
    void nestingDepthIsCapped() {
        String atLimit = "[".repeat(Json.MAX_DEPTH) + "]".repeat(Json.MAX_DEPTH);
        assertTrue(Json.parse(atLimit) instanceof List);
        String tooDeep = "[".repeat(Json.MAX_DEPTH + 1) + "]".repeat(Json.MAX_DEPTH + 1);
        assertThrows(JsonException.class, () -> Json.parse(tooDeep));
        // Deep enough to overflow the stack if the cap did not stop it first.
        assertThrows(JsonException.class, () -> Json.parse("[".repeat(1_000_000)));
        assertThrows(JsonException.class, () -> Json.parse("{\"a\":".repeat(1_000_000)));
    }

    @Test
    void writesWhatItReads() {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("text", "quote \" backslash \\ newline \n tab \t bell \u0007 é");
        value.put("numbers", Arrays.asList(1L, -7L, 2.5, Long.MAX_VALUE));
        value.put("flags", Arrays.asList(true, false, null));
        value.put("nested", Map.of("empty", List.of()));
        assertEquals(value, Json.parse(Json.write(value)));
        assertEquals("{\"a\":1,\"b\":[true,null]}", Json.write(Json.parse("{ \"a\" : 1, \"b\" : [ true, null ] }")));
    }

    @Test
    void floatsSurviveTheRoundTripExactly() {
        Random random = new Random(5);
        float[] floats = new float[10_000];
        for (int i = 0; i < floats.length; i++) {
            floats[i] = Float.intBitsToFloat(random.nextInt());
            if (!Float.isFinite(floats[i])) {
                floats[i] = i;
            }
        }
        floats[0] = Float.MIN_VALUE;
        floats[1] = Float.MAX_VALUE;
        floats[2] = -0f;
        List<?> parsed = (List<?>) Json.parse(Json.write(floats));
        assertEquals(floats.length, parsed.size());
        for (int i = 0; i < floats.length; i++) {
            float back = ((Number) parsed.get(i)).floatValue();
            assertEquals(Float.floatToIntBits(floats[i]), Float.floatToIntBits(back), "float " + floats[i]);
        }
    }

    @Test
    void valuesJsonCannotExpressBecomeNull() {
        assertEquals("[null,null,1.5]", Json.write(new float[] {Float.NaN, Float.POSITIVE_INFINITY, 1.5f}));
        assertEquals("null", Json.write(Double.NEGATIVE_INFINITY));
        assertThrows(IllegalArgumentException.class, () -> Json.write(new Object()));
    }
}
