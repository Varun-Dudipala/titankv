package com.titankv.util;

/**
 * Checks that text is one well-formed JSON object and nothing else. Used before JSON that came
 * from another process is passed on inside this node's own answer: text that only looks like an
 * object at both ends could close this node's structure early or leave it unparsable.
 */
final class JsonCheck {

    private static final int MAX_DEPTH = 32;

    private final String text;
    private int at;

    private JsonCheck(String text) {
        this.text = text;
    }

    /**
     * @return true if the text is exactly one JSON object, optionally surrounded by whitespace
     */
    static boolean isObject(String text) {
        if (text == null) {
            return false;
        }
        JsonCheck check = new JsonCheck(text);
        check.skipWhitespace();
        if (check.at >= text.length() || text.charAt(check.at) != '{') {
            return false;
        }
        if (!check.value(0)) {
            return false;
        }
        check.skipWhitespace();
        return check.at == text.length();
    }

    private boolean value(int depth) {
        skipWhitespace();
        if (at >= text.length() || depth > MAX_DEPTH) {
            return false;
        }
        char c = text.charAt(at);
        switch (c) {
            case '{':
                return object(depth);
            case '[':
                return array(depth);
            case '"':
                return string();
            case 't':
                return literal("true");
            case 'f':
                return literal("false");
            case 'n':
                return literal("null");
            default:
                return number();
        }
    }

    private boolean object(int depth) {
        at++; // {
        skipWhitespace();
        if (peek('}')) {
            at++;
            return true;
        }
        while (true) {
            skipWhitespace();
            if (!peek('"') || !string()) {
                return false;
            }
            skipWhitespace();
            if (!peek(':')) {
                return false;
            }
            at++;
            if (!value(depth + 1)) {
                return false;
            }
            skipWhitespace();
            if (peek(',')) {
                at++;
            } else if (peek('}')) {
                at++;
                return true;
            } else {
                return false;
            }
        }
    }

    private boolean array(int depth) {
        at++; // [
        skipWhitespace();
        if (peek(']')) {
            at++;
            return true;
        }
        while (true) {
            if (!value(depth + 1)) {
                return false;
            }
            skipWhitespace();
            if (peek(',')) {
                at++;
            } else if (peek(']')) {
                at++;
                return true;
            } else {
                return false;
            }
        }
    }

    private boolean string() {
        at++; // opening quote
        while (at < text.length()) {
            char c = text.charAt(at++);
            if (c == '"') {
                return true;
            }
            if (c < 0x20) {
                return false;
            }
            if (c == '\\') {
                if (at >= text.length()) {
                    return false;
                }
                char escaped = text.charAt(at++);
                if (escaped == 'u') {
                    if (at + 4 > text.length()) {
                        return false;
                    }
                    for (int i = 0; i < 4; i++) {
                        if (Character.digit(text.charAt(at++), 16) < 0) {
                            return false;
                        }
                    }
                } else if ("\"\\/bfnrt".indexOf(escaped) < 0) {
                    return false;
                }
            }
        }
        return false; // no closing quote
    }

    private boolean number() {
        int start = at;
        if (peek('-')) {
            at++;
        }
        if (peek('0')) {
            at++;
        } else if (!digits()) {
            return false;
        }
        if (peek('.')) {
            at++;
            if (!digits()) {
                return false;
            }
        }
        if (peek('e') || peek('E')) {
            at++;
            if (peek('+') || peek('-')) {
                at++;
            }
            if (!digits()) {
                return false;
            }
        }
        return at > start;
    }

    private boolean digits() {
        int start = at;
        while (at < text.length() && text.charAt(at) >= '0' && text.charAt(at) <= '9') {
            at++;
        }
        return at > start;
    }

    private boolean literal(String word) {
        if (text.startsWith(word, at)) {
            at += word.length();
            return true;
        }
        return false;
    }

    private boolean peek(char c) {
        return at < text.length() && text.charAt(at) == c;
    }

    private void skipWhitespace() {
        while (at < text.length()) {
            char c = text.charAt(at);
            if (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
                return;
            }
            at++;
        }
    }
}
