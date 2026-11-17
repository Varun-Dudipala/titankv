package com.titankv.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * JsonWriter: separators between elements, nesting, and escaping of text.
 */
class JsonWriterTest {

    @Test
    void separatesElementsOfObjectsAndArrays() {
        String json = new JsonWriter().beginObject()
                .name("id").value("node-1")
                .name("up").value(true)
                .name("count").value(42L)
                .name("empty").beginObject().endObject()
                .name("list").beginArray()
                .value(1L).value(2L)
                .beginArray().value(0.5, 3).value("x").endArray()
                .beginObject().name("a").value(1L).endObject()
                .endArray()
                .endObject().toString();

        assertThat(json).isEqualTo(
                "{\"id\":\"node-1\",\"up\":true,\"count\":42,\"empty\":{},\"list\":[1,2,[0.500,\"x\"],{\"a\":1}]}");
    }

    @Test
    void escapesTextSoThatItCannotEndTheString() {
        String json = new JsonWriter().beginArray()
                .value("say \"hi\"")
                .value("back\\slash")
                .value("line\nbreak\ttab\rreturn")
                .value("bell\u0007 separator ")
                .value("\"},{\"injected\":true")
                .endArray().toString();

        assertThat(json).isEqualTo("[\"say \\\"hi\\\"\",\"back\\\\slash\",\"line\\nbreak\\ttab\\rreturn\","
                + "\"bell\\u0007 separator\\u2028\",\"\\\"},{\\\"injected\\\":true\"]");
    }

    @Test
    void namesAreEscapedLikeValues() {
        assertThat(new JsonWriter().beginObject().name("a\"b").value(1L).endObject().toString())
                .isEqualTo("{\"a\\\"b\":1}");
    }

    @Test
    void writesNullAndNumbersThatJsonCanHold() {
        String json = new JsonWriter().beginArray()
                .value((String) null)
                .value(Double.NaN, 2)
                .value(Double.POSITIVE_INFINITY, 1)
                .value(1234.5678, 2)
                .value(-3L)
                .endArray().toString();

        assertThat(json).isEqualTo("[null,0.00,0.0,1234.57,-3]");
    }

    @Test
    void insertsJsonThatIsAlreadyWritten() {
        String json = new JsonWriter().beginObject()
                .name("self").raw("{\"node\":\"node-2\"}")
                .name("next").value(false)
                .endObject().toString();

        assertThat(json).isEqualTo("{\"self\":{\"node\":\"node-2\"},\"next\":false}");
    }
}
