package com.qiankun.mysql.disruptor.schemma.column;

import java.sql.Time;
import java.sql.Timestamp;

public class TimeColumnParser extends ColumnParser {

    @Override
    public Object getValue(Object value) {

        if (value == null) {
            return null;
        }

        if (value instanceof Timestamp) {

            return new Time(((Timestamp) value).getTime());
        }

        return value;
    }
}
