package com.pinterest.singer.writer.memq.commons.serde;

import com.pinterest.singer.writer.memq.commons.Deserializer;

public class StringDeserializer implements Deserializer<String> {

    @Override
    public String deserialize(byte[] bytes) {
        return new String(bytes);
    }
}
