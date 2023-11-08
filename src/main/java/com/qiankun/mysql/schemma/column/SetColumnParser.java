package com.qiankun.mysql.schemma.column;

public class SetColumnParser extends ColumnParser {

    private String[] enumValues;

    public SetColumnParser(String colType) {
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

        StringBuilder builder = new StringBuilder();
        long l = (Long) value;

        boolean needSplit = false;
        for (int i = 0; i < enumValues.length; i++) {
            if (((l >> i) & 1) == 1) {
                if (needSplit)
                    builder.append(",");

                builder.append(enumValues[i]);
                needSplit = true;
            }
        }

        return builder.toString();
    }
}
