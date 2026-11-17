package com.titankv.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class JsonCheckTest {

    @Test
    void acceptsWhatJsonWriterProduces() {
        JsonWriter json = new JsonWriter();
        json.beginObject()
                .name("node").value("node-1")
                .name("ready").value(true)
                .name("nothing").value((String) null)
                .name("ops").beginObject().name("gets").value(12).name("mean").value(0.125, 3).endObject()
                .name("members").beginArray()
                .beginObject().name("id").value("a \"quoted\" id\n ").endObject()
                .beginArray().value(0.5, 6).value(-3).endArray()
                .endArray()
                .endObject();

        assertThat(JsonCheck.isObject(json.toString())).isTrue();
        assertThat(JsonCheck.isObject("  " + json + "\n")).isTrue();
        assertThat(JsonCheck.isObject("{}")).isTrue();
        assertThat(JsonCheck.isObject("{\"n\":[-0.5e+10,1E3,0,[],{}],\"s\":\"\\u00e9\\/\\b\"}")).isTrue();
    }

    @Test
    void rejectsTextThatOnlyLooksLikeAnObjectAtBothEnds() {
        String[] bad = {
            null, "", "   ", "[]", "\"text\"", "12", "null",
            "{\"node\":\"a\"}{\"x\":1}",            // two objects
            "{\"node\":\"a\"},\"injected\":{}",     // ends the object early, then carries on
            "{\"node\":\"a\"} trailing",
            "{\"node\":\"a\",}",                    // trailing comma
            "{\"node\":\"a\" \"b\":1}",             // missing comma
            "{\"node\":}",
            "{\"node\"}",
            "{node:1}",
            "{\"node\":'a'}",
            "{\"node\":\"unterminated}",
            "{\"node\":\"bad \\x escape\"}",
            "{\"node\":\"bad \\u12 escape\"}",
            "{\"node\":\"raw\nnewline\"}",
            "{\"n\":01}", "{\"n\":1.}", "{\"n\":.5}", "{\"n\":1e}", "{\"n\":-}", "{\"n\":NaN}",
            "{\"n\":tru}", "{\"n\":[1,2}", "{\"n\":[1,,2]}", "{\"n\":[1 2]}",
            "{\"node\":\"a\"",
        };
        for (String text : bad) {
            assertThat(JsonCheck.isObject(text)).as(String.valueOf(text)).isFalse();
        }
    }

    @Test
    void refusesToFollowNestingWithoutEnd() {
        StringBuilder deep = new StringBuilder("{\"a\":");
        for (int i = 0; i < 5_000; i++) {
            deep.append('[');
        }
        for (int i = 0; i < 5_000; i++) {
            deep.append(']');
        }
        deep.append('}');

        assertThat(JsonCheck.isObject(deep.toString())).isFalse(); // and no StackOverflowError
    }
}
