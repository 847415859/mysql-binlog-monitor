package com.qiankun.mysql.disruptor.schemma.column;

import org.apache.commons.codec.binary.Base64;

public class DefaultColumnParser extends ColumnParser {

    @Override
    public Object getValue(Object value) {

        if (value == null) {
            return null;
        }

        if (value instanceof byte[]) {
            return Base64.encodeBase64String((byte[]) value);
        }

        return value;
    }
}
