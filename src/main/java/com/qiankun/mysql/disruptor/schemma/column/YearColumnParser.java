package com.qiankun.mysql.disruptor.schemma.column;

import java.sql.Date;
import java.util.Calendar;

public class YearColumnParser extends ColumnParser {

    @Override
    public Object getValue(Object value) {

        if (value == null) {
            return null;
        }

        if (value instanceof Date) {
            Calendar calendar = Calendar.getInstance();
            calendar.setTime((Date) value);
            return calendar.get(Calendar.YEAR);
        }

        return value;
    }
}
