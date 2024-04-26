package com.qiankun.mysql.disruptor.schemma.column;

public class EnumColumnParser extends ColumnParser {

    private String[] enumValues;

    public EnumColumnParser(String colType) {
        enumValues = extractEnumValues(colType);
    }

    @Override
    public Object getValue(Object value) {

        if (value == null) {
            return null;
        }

        if (value instanceof String) {
            return value;
        }

        Integer i = (Integer) value;
        if (i == 0) {
            return null;
        } else {
            return enumValues[i - 1];
        }
    }
}
