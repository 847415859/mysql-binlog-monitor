package com.qiankun.mysql.disruptor.schemma.column;

import org.apache.commons.codec.Charsets;

public class StringColumnParser extends ColumnParser {

    private String charset;

    public StringColumnParser(String charset) {
        this.charset = charset.toLowerCase();
    }

    @Override
    public Object getValue(Object value) {

        if (value == null) {
            return null;
        }

        if (value instanceof String) {
            return value;
        }

        byte[] bytes = (byte[]) value;

        switch (charset) {
            case "utf8":
            case "utf8mb4":
                return new String(bytes, Charsets.UTF_8);
            case "latin1":
            case "ascii":
                return new String(bytes, Charsets.ISO_8859_1);
            case "ucs2":
                return new String(bytes, Charsets.UTF_16);
            default:
                return new String(bytes, Charsets.toCharset(charset));

        }
    }
}
