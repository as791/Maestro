package io.maestro.sdk;

import java.util.List;
import java.util.Map;

/**
 * Typed wrapper over a raw JSON API response.
 * Provides accessors that dig into the parsed Map without requiring a model class per endpoint.
 * ponytail: avoids generating 15 response POJOs; callers use getString/getInt/etc.
 */
public final class MaestroResponse {
    private final int statusCode;
    private final String rawBody;
    private Map<String, Object> parsed;

    MaestroResponse(int statusCode, String rawBody) {
        this.statusCode = statusCode;
        this.rawBody = rawBody;
    }

    public int statusCode() { return statusCode; }
    public String rawBody() { return rawBody; }

    /** Parse the body as a JSON object. Cached after first call. */
    public Map<String, Object> json() {
        if (parsed == null) parsed = Json.parseObject(rawBody);
        return parsed;
    }

    /** Parse the body as a JSON array of objects. */
    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> jsonArray() {
        return Json.parseArray(rawBody);
    }

    public String getString(String key) {
        Object v = json().get(key);
        return v == null ? null : v.toString();
    }

    public int getInt(String key, int defaultValue) {
        Object v = json().get(key);
        if (v instanceof Number n) return n.intValue();
        return defaultValue;
    }

    public long getLong(String key, long defaultValue) {
        Object v = json().get(key);
        if (v instanceof Number n) return n.longValue();
        return defaultValue;
    }

    public boolean getBool(String key) {
        Object v = json().get(key);
        return v instanceof Boolean b && b;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getObject(String key) {
        Object v = json().get(key);
        if (v instanceof Map<?,?> m) return (Map<String, Object>) m;
        return null;
    }

    @Override
    public String toString() {
        return "MaestroResponse{status=%d, body=%s}".formatted(statusCode, rawBody);
    }
}
