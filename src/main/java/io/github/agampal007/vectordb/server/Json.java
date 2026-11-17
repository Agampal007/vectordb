package io.github.agampal007.vectordb.server;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A small JSON reader and writer (RFC 8259), so the server needs nothing outside the JDK.
 *
 * <p>Parsing gives {@link Map} (insertion-ordered, string keys), {@link List}, {@link String},
 * {@link Boolean}, {@code null}, and for numbers a {@link Long} when the text is an integer that
 * fits in one, otherwise a {@link Double}. Ids are 64-bit, so they must not pass through a double.
 *
 * <p>Input comes from the network, so nesting depth is capped: a body of a million {@code [} must
 * fail cleanly instead of overflowing the stack.
 */
final class Json {

    /** Malformed JSON, with the character offset where reading stopped. */
    static final class JsonException extends IllegalArgumentException {

        private static final long serialVersionUID = 1L;

        JsonException(String message, int position) {
            super("invalid JSON at character " + position + ": " + message);
        }
    }

    static final int MAX_DEPTH = 32;

    private final String text;
    private int position;

    private Json(String text) {
        this.text = text;
    }

    /** Parses one complete JSON value. */
    static Object parse(String text) {
        Json parser = new Json(text);
        Object value = parser.value(0);
        parser.skipWhitespace();
        if (parser.position != text.length()) {
            throw parser.error("unexpected text after the value");
        }
        return value;
    }

    /**
     * Writes maps, iterables, strings, numbers, booleans, {@code null} and {@code float[]}. JSON
     * has no way to write NaN or infinity, so those become {@code null}.
     */
    static String write(Object value) {
        StringBuilder out = new StringBuilder();
        write(value, out);
        return out.toString();
    }

    private Object value(int depth) {
        skipWhitespace();
        if (position >= text.length()) {
            throw error("unexpected end of input");
        }
        char c = text.charAt(position);
        switch (c) {
            case '{':
                return object(depth);
            case '[':
                return array(depth);
            case '"':
                return string();
            case 't':
                return literal("true", Boolean.TRUE);
            case 'f':
                return literal("false", Boolean.FALSE);
            case 'n':
                return literal("null", null);
            default:
                if (c == '-' || (c >= '0' && c <= '9')) {
                    return number();
                }
                throw error("unexpected character '" + c + "'");
        }
    }

    private Map<String, Object> object(int depth) {
        if (depth >= MAX_DEPTH) {
            throw error("nested deeper than " + MAX_DEPTH + " levels");
        }
        position++; // {
        Map<String, Object> map = new LinkedHashMap<>();
        skipWhitespace();
        if (peek() == '}') {
            position++;
            return map;
        }
        while (true) {
            skipWhitespace();
            if (peek() != '"') {
                throw error("expected a string key");
            }
            String key = string();
            skipWhitespace();
            expect(':');
            map.put(key, value(depth + 1));
            skipWhitespace();
            char c = next();
            if (c == '}') {
                return map;
            }
            if (c != ',') {
                position--;
                throw error("expected ',' or '}'");
            }
        }
    }

    private List<Object> array(int depth) {
        if (depth >= MAX_DEPTH) {
            throw error("nested deeper than " + MAX_DEPTH + " levels");
        }
        position++; // [
        List<Object> list = new ArrayList<>();
        skipWhitespace();
        if (peek() == ']') {
            position++;
            return list;
        }
        while (true) {
            list.add(value(depth + 1));
            skipWhitespace();
            char c = next();
            if (c == ']') {
                return list;
            }
            if (c != ',') {
                position--;
                throw error("expected ',' or ']'");
            }
        }
    }

    private String string() {
        position++; // opening quote
        StringBuilder out = new StringBuilder();
        while (true) {
            char c = next();
            if (c == '"') {
                return out.toString();
            }
            if (c < 0x20) {
                position--;
                throw error("control character inside a string");
            }
            if (c != '\\') {
                out.append(c);
                continue;
            }
            char escaped = next();
            switch (escaped) {
                case '"', '\\', '/' -> out.append(escaped);
                case 'b' -> out.append('\b');
                case 'f' -> out.append('\f');
                case 'n' -> out.append('\n');
                case 'r' -> out.append('\r');
                case 't' -> out.append('\t');
                case 'u' -> out.append(unicodeEscape());
                default -> {
                    position--;
                    throw error("unknown escape '\\" + escaped + "'");
                }
            }
        }
    }

    private char unicodeEscape() {
        if (position + 4 > text.length()) {
            throw error("unfinished \\u escape");
        }
        int code = 0;
        for (int i = 0; i < 4; i++) {
            int digit = Character.digit(text.charAt(position + i), 16);
            if (digit < 0) {
                throw error("bad \\u escape");
            }
            code = code * 16 + digit;
        }
        position += 4;
        return (char) code;
    }

    private Number number() {
        int start = position;
        boolean integer = true;
        if (peek() == '-') {
            position++;
        }
        if (peek() == '0') {
            position++;
        } else if (digits() == 0) {
            throw error("expected a digit");
        }
        if (peek() == '.') {
            integer = false;
            position++;
            if (digits() == 0) {
                throw error("expected a digit after '.'");
            }
        }
        if (peek() == 'e' || peek() == 'E') {
            integer = false;
            position++;
            if (peek() == '+' || peek() == '-') {
                position++;
            }
            if (digits() == 0) {
                throw error("expected a digit in the exponent");
            }
        }
        String token = text.substring(start, position);
        if (integer) {
            try {
                return Long.parseLong(token);
            } catch (NumberFormatException e) {
                // Too large for a long: fall through and keep it as a double.
            }
        }
        return Double.parseDouble(token);
    }

    private int digits() {
        int start = position;
        while (position < text.length() && text.charAt(position) >= '0' && text.charAt(position) <= '9') {
            position++;
        }
        return position - start;
    }

    private Object literal(String word, Object value) {
        if (!text.startsWith(word, position)) {
            throw error("expected " + word);
        }
        position += word.length();
        return value;
    }

    private void skipWhitespace() {
        while (position < text.length()) {
            char c = text.charAt(position);
            if (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
                return;
            }
            position++;
        }
    }

    /** The current character, or 0 at the end of the input. */
    private char peek() {
        return position < text.length() ? text.charAt(position) : 0;
    }

    private char next() {
        if (position >= text.length()) {
            throw error("unexpected end of input");
        }
        return text.charAt(position++);
    }

    private void expect(char c) {
        if (peek() != c) {
            throw error("expected '" + c + "'");
        }
        position++;
    }

    private JsonException error(String message) {
        return new JsonException(message, position);
    }

    private static void write(Object value, StringBuilder out) {
        if (value == null) {
            out.append("null");
        } else if (value instanceof String s) {
            writeString(s, out);
        } else if (value instanceof Float f) {
            out.append(Float.isFinite(f) ? Float.toString(f) : "null");
        } else if (value instanceof Double d) {
            out.append(Double.isFinite(d) ? Double.toString(d) : "null");
        } else if (value instanceof Number || value instanceof Boolean) {
            out.append(value);
        } else if (value instanceof float[] floats) {
            out.append('[');
            for (int i = 0; i < floats.length; i++) {
                if (i > 0) {
                    out.append(',');
                }
                out.append(Float.isFinite(floats[i]) ? Float.toString(floats[i]) : "null");
            }
            out.append(']');
        } else if (value instanceof Map<?, ?> map) {
            out.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (!first) {
                    out.append(',');
                }
                first = false;
                writeString(String.valueOf(entry.getKey()), out);
                out.append(':');
                write(entry.getValue(), out);
            }
            out.append('}');
        } else if (value instanceof Iterable<?> items) {
            out.append('[');
            boolean first = true;
            for (Object item : items) {
                if (!first) {
                    out.append(',');
                }
                first = false;
                write(item, out);
            }
            out.append(']');
        } else {
            throw new IllegalArgumentException("cannot write " + value.getClass().getName() + " as JSON");
        }
    }

    private static void writeString(String s, StringBuilder out) {
        out.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        out.append('"');
    }
}
