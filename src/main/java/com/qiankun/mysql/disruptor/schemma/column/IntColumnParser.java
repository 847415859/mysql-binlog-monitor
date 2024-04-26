package com.qiankun.mysql.disruptor.schemma.column;

public class IntColumnParser extends ColumnParser {

    private int bits;
    private boolean signed;

    public IntColumnParser(String dataType, String colType) {

        switch (dataType) {
            case "tinyint":
                bits = 8;
                break;
            case "smallint":
                bits = 16;
                break;
            case "mediumint":
                bits = 24;
                break;
            case "int":
                bits = 32;
        }

        this.signed = !colType.matches(".* unsigned$");
    }

    @Override
    public Object getValue(Object value) {

        if (value == null) {
            return null;
        }

        if (value instanceof Long) {
            return value;
        }

        if (value instanceof Integer) {
            Integer i = (Integer) value;
            if (signed || i > 0) {
                return i;
            } else {
                return (1L << bits) + i;
            }
        }

        return value;
    }
}
