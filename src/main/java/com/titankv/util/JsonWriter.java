package com.titankv.util;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Locale;

/**
 * Writes JSON text: enough for the HTTP endpoints, which only ever produce it. Commas and string
 * escaping are handled here so callers cannot get them wrong.
 */
final class JsonWriter {

    private final StringBuilder out = new StringBuilder();
    // One entry per open object or array: whether it already has an element
    private final Deque<Boolean> hasElement = new ArrayDeque<>();
    private boolean afterName;

    JsonWriter beginObject() {
        separate();
        out.append('{');
        hasElement.push(false);
        return this;
    }

    JsonWriter endObject() {
        out.append('}');
        hasElement.pop();
        return this;
    }

    JsonWriter beginArray() {
        separate();
        out.append('[');
        hasElement.push(false);
        return this;
    }

    JsonWriter endArray() {
        out.append(']');
        hasElement.pop();
        return this;
    }

    JsonWriter name(String name) {
        separate();
        quote(name);
        out.append(':');
        afterName = true;
        return this;
    }

    JsonWriter value(String value) {
        separate();
        if (value == null) {
            out.append("null");
        } else {
            quote(value);
        }
        return this;
    }

    JsonWriter value(long value) {
        separate();
        out.append(value);
        return this;
    }

    JsonWriter value(boolean value) {
        separate();
        out.append(value);
        return this;
    }

    /**
     * A number with a fixed count of decimals. JSON has no NaN or infinity, so those become 0.
     */
    JsonWriter value(double value, int decimals) {
        separate();
        double finite = Double.isFinite(value) ? value : 0.0;
        out.append(String.format(Locale.ROOT, "%." + decimals + "f", finite));
        return this;
    }

    /**
     * Insert text that is already JSON.
     */
    JsonWriter raw(String json) {
        separate();
        out.append(json);
        return this;
    }

    private void separate() {
        if (afterName) {
            afterName = false;
            return;
        }
        if (!hasElement.isEmpty()) {
            if (hasElement.pop()) {
                out.append(',');
            }
            hasElement.push(true);
        }
    }

    private void quote(String text) {
        out.append('"');
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            switch (c) {
                case '"':
                    out.append("\\\"");
                    break;
                case '\\':
                    out.append("\\\\");
                    break;
                case '\n':
                    out.append("\\n");
                    break;
                case '\r':
                    out.append("\\r");
                    break;
                case '\t':
                    out.append("\\t");
                    break;
                default:
                    // Control characters must be escaped; the two line separators are legal JSON
                    // but not legal inside a JavaScript string, so they are escaped as well
                    if (c < 0x20 || c == 0x2028 || c == 0x2029) {
                        out.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
            }
        }
        out.append('"');
    }

    @Override
    public String toString() {
        return out.toString();
    }
}
