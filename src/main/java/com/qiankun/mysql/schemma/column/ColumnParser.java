package com.qiankun.mysql.schemma.column;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 列解析器
 */
public abstract class ColumnParser {

    /**
     * 根据mysql数据类型，匹配类型转换器
     * @param dataType
     * @param colType
     * @param charset
     * @return
     */
    public static ColumnParser getColumnParser(String dataType, String colType, String charset) {

        switch (dataType) {
            case "tinyint":
            case "smallint":
            case "mediumint":
            case "int":
                return new IntColumnParser(dataType, colType);
            case "bigint":
                return new BigIntColumnParser(colType);
            case "tinytext":
            case "text":
            case "mediumtext":
            case "longtext":
            case "varchar":
            case "char":
                return new StringColumnParser(charset);
            case "date":
            case "datetime":
            case "timestamp":
                return new DateTimeColumnParser();
            case "time":
                return new TimeColumnParser();
            case "year":
                return new YearColumnParser();
            case "enum":
                return new EnumColumnParser(colType);
            case "set":
                return new SetColumnParser(colType);
            default:
                return new DefaultColumnParser();
        }
    }

    public static String[] extractEnumValues(String colType) {
        String[] enumValues = {};
        Matcher matcher = Pattern.compile("(enum|set)\\((.*)\\)").matcher(colType);
        if (matcher.matches()) {
            enumValues = matcher.group(2).replace("'", "").split(",");
        }

        return enumValues;
    }

    public abstract Object getValue(Object value);

}
