package io.maestro.sdk;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal JSON writer/reader. Zero dependencies.
 * ponytail: good enough for control-plane payloads; swap for Jackson if perf matters.
 */
final class Json {
    private Json() {}

    // ── Writer helpers ──

    static void field(StringBuilder sb, String key, String value) {
        sb.append('"').append(key).append("\":\"").append(escape(value)).append("\",");
    }

    static void field(StringBuilder sb, String key, int value) {
        sb.append('"').append(key).append("\":").append(value).append(',');
    }

    static void field(StringBuilder sb, String key, long value) {
        sb.append('"').append(key).append("\":").append(value).append(',');
    }

    static void field(StringBuilder sb, String key, boolean value) {
        sb.append('"').append(key).append("\":").append(value).append(',');
    }

    static void mapField(StringBuilder sb, String key, Map<String, String> map) {
        if (map == null || map.isEmpty()) return;
        sb.append('"').append(key).append("\":{");
        map.forEach((k, v) -> sb.append('"').append(escape(k)).append("\":\"").append(escape(v)).append("\","));
        sb.setLength(sb.length() - 1); // trailing comma
        sb.append("},");
    }

    static String escape(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }

    static String object(Object... kvPairs) {
        var sb = new StringBuilder(256);
        sb.append('{');
        for (int i = 0; i < kvPairs.length; i += 2) {
            String key = (String) kvPairs[i];
            Object val = kvPairs[i + 1];
            if (val == null) continue;
            if (val instanceof String s) field(sb, key, s);
            else if (val instanceof Boolean b) field(sb, key, b);
            else if (val instanceof Integer n) field(sb, key, n);
            else if (val instanceof Long n) field(sb, key, n);
            else sb.append('"').append(key).append("\":").append(val).append(',');
        }
        if (sb.length() > 1 && sb.charAt(sb.length() - 1) == ',') sb.setLength(sb.length() - 1);
        sb.append('}');
        return sb.toString();
    }

    // ── Minimal recursive-descent parser ──

    /**
     * Parse a JSON string into Map/List/String/Double/Boolean/null.
     * ponytail: handles the Maestro API responses; not a spec-complete parser.
     */
    @SuppressWarnings("unchecked")
    static Map<String, Object> parseObject(String json) {
        if (json == null || json.isBlank()) return Map.of();
        var result = parse(json.trim(), new int[]{0});
        if (result instanceof Map<?,?> m) return (Map<String, Object>) m;
        throw new IllegalArgumentException("expected JSON object, got: " + result);
    }

    @SuppressWarnings("unchecked")
    static List<Map<String, Object>> parseArray(String json) {
        if (json == null || json.isBlank()) return List.of();
        var result = parse(json.trim(), new int[]{0});
        if (result instanceof List<?> l) return (List<Map<String, Object>>) l;
        throw new IllegalArgumentException("expected JSON array, got: " + result);
    }

    private static Object parse(String s, int[] pos) {
        skipWhitespace(s, pos);
        if (pos[0] >= s.length()) return null;
        char c = s.charAt(pos[0]);
        return switch (c) {
            case '{' -> parseObj(s, pos);
            case '[' -> parseArr(s, pos);
            case '"' -> parseString(s, pos);
            case 't', 'f' -> parseBool(s, pos);
            case 'n' -> parseNull(s, pos);
            default -> parseNumber(s, pos);
        };
    }

    private static Map<String, Object> parseObj(String s, int[] pos) {
        pos[0]++; // skip {
        var map = new LinkedHashMap<String, Object>();
        skipWhitespace(s, pos);
        if (s.charAt(pos[0]) == '}') { pos[0]++; return map; }
        while (pos[0] < s.length()) {
            skipWhitespace(s, pos);
            String key = parseString(s, pos);
            skipWhitespace(s, pos);
            pos[0]++; // skip :
            Object value = parse(s, pos);
            map.put(key, value);
            skipWhitespace(s, pos);
            if (s.charAt(pos[0]) == ',') { pos[0]++; continue; }
            if (s.charAt(pos[0]) == '}') { pos[0]++; break; }
        }
        return map;
    }

    private static List<Object> parseArr(String s, int[] pos) {
        pos[0]++; // skip [
        var list = new ArrayList<>();
        skipWhitespace(s, pos);
        if (s.charAt(pos[0]) == ']') { pos[0]++; return list; }
        while (pos[0] < s.length()) {
            list.add(parse(s, pos));
            skipWhitespace(s, pos);
            if (s.charAt(pos[0]) == ',') { pos[0]++; continue; }
            if (s.charAt(pos[0]) == ']') { pos[0]++; break; }
        }
        return list;
    }

    private static String parseString(String s, int[] pos) {
        pos[0]++; // skip opening "
        var sb = new StringBuilder();
        while (pos[0] < s.length()) {
            char c = s.charAt(pos[0]++);
            if (c == '"') return sb.toString();
            if (c == '\\') {
                char next = s.charAt(pos[0]++);
                switch (next) {
                    case '"', '\\', '/' -> sb.append(next);
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    case 't' -> sb.append('\t');
                    case 'u' -> {
                        sb.append((char) Integer.parseInt(s.substring(pos[0], pos[0] + 4), 16));
                        pos[0] += 4;
                    }
                    default -> sb.append(next);
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private static Object parseNumber(String s, int[] pos) {
        int start = pos[0];
        boolean isFloat = false;
        while (pos[0] < s.length()) {
            char c = s.charAt(pos[0]);
            if (c == '.' || c == 'e' || c == 'E') isFloat = true;
            if (c == '-' || c == '+' || c == '.' || (c >= '0' && c <= '9') || c == 'e' || c == 'E') {
                pos[0]++;
            } else break;
        }
        String num = s.substring(start, pos[0]);
        if (isFloat) return Double.parseDouble(num);
        long l = Long.parseLong(num);
        if (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) return (int) l;
        return l;
    }

    private static Boolean parseBool(String s, int[] pos) {
        if (s.startsWith("true", pos[0])) { pos[0] += 4; return true; }
        if (s.startsWith("false", pos[0])) { pos[0] += 5; return false; }
        throw new IllegalArgumentException("expected boolean at pos " + pos[0]);
    }

    private static Object parseNull(String s, int[] pos) {
        pos[0] += 4; // skip "null"
        return null;
    }

    private static void skipWhitespace(String s, int[] pos) {
        while (pos[0] < s.length() && Character.isWhitespace(s.charAt(pos[0]))) pos[0]++;
    }
}
