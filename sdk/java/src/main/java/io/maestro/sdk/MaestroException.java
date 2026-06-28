package io.maestro.sdk;

/** Wraps non-2xx HTTP responses from the Maestro API. */
public class MaestroException extends RuntimeException {
    private final int statusCode;
    private final String body;

    public MaestroException(int statusCode, String body) {
        super("Maestro API error %d: %s".formatted(statusCode, body));
        this.statusCode = statusCode;
        this.body = body;
    }

    public int statusCode() { return statusCode; }
    public String body() { return body; }
}
