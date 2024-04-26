/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qiankun.mysql;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.*;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


public class Config {

    public String mysqlAddr;
    public Integer mysqlPort;
    public String mysqlUsername;
    public String mysqlPassword;

    /**
     * 需要监控的库，如果不设置则监控所有
     */
    public String dbs;

    public Map<String, Predicate<String>> tablePredicateMap = new HashMap<>();

    /**
     * 超过多少数据作为一个批次刷盘
     */
    public int batchSize = 1000;
    /**
     * 多次时间默认刷盘一次
     */
    public long interval = 30_000;

    // 多线程处理任务
    public int threadPoolNum = 5;

    public String startType = "DEFAULT";
    public String binlogFilename;
    public Long nextPosition;
    public Integer maxTransactionRows = 100;

    public String mongoAddr = "localhost";
    public Integer mongoPort = 27017;
    public String mongoDb = "ModelLog";
    public String authSource = "admin";
    public String mongoCollection = "ModelLog";
    public String mongoUsername = "root";
    public String mongoPassword = "root";

    /**
     * Processor
     */
    // 对比差异时，不需要对比的列 多个字段,分割开
    public String viewNotMatchFields;
    // 对比差异时，不需要对比的列 多个字段, 去除_, 全部转化为小写
    private List<String> viewNotMatchFieldList;

    final static Pattern PATTERN = Pattern.compile("^-\\w+#(include|exclude)$");


    public void load(Properties properties) throws IOException {
        properties2Object(properties, this);
        // 解析包含或者排除的表
        Enumeration<Object> keys = properties.keys();
        while (keys.hasMoreElements()){
            String key = keys.nextElement().toString();
            // 判断是否是tableFilte格式
            if (key.length() > 4 && PATTERN.matcher(key).matches()) {
                String tables = properties.getProperty(key);
                List<String> filterTableNames = Arrays.asList(tables.split(",")).stream()
                        .filter(StringUtils::isNoneBlank).collect(Collectors.toList());
                String db = key.substring(1).split("#")[0];
                String identifier = key.substring(1).split("#")[1];
                if (containsDb(db)) {
                    // 如果包含include，exclude只能存在一个,include优先级高,如果为null，则默认都监控
                    if("include".equals(identifier)){
                        tablePredicateMap.put(db,tableName -> filterTableNames.isEmpty() || filterTableNames.contains(tableName) || prefixFuzzy(filterTableNames, tableName));
                    }else if("exclude".equals(identifier)){
                        // 如果之前存储过 Predicate ,则不覆盖，避免覆盖之前include的 Predicate
                        tablePredicateMap.putIfAbsent(db, tableName -> filterTableNames.isEmpty() || !filterTableNames.contains(tableName) || prefixFuzzy(filterTableNames, tableName));
                    }
                }
            }
        }
    }

    /**
     * 前缀匹配
     * @param filterTableNames
     * @param tableName
     * @return
     */
    private boolean prefixFuzzy(List<String> filterTableNames, String tableName) {
        if(CollectionUtils.isEmpty(filterTableNames)){
            return false;
        }
        List<String> fuzzyTableNames = filterTableNames.stream().filter(StringUtils::isNotBlank).filter(i -> i.endsWith("*")).collect(Collectors.toList());
        if(CollectionUtils.isEmpty(fuzzyTableNames)){
            return false;
        }
        return fuzzyTableNames.stream().anyMatch(fuzzyTableName -> tableName.startsWith(fuzzyTableName.replaceAll("\\*", "")));
    }




    public void load() throws IOException {
        InputStream in = Config.class.getClassLoader().getResourceAsStream("mysql-monitor.conf");
        Properties properties = new Properties();
        properties.load(in);

        load(properties);
    }

    private void properties2Object(final Properties p, final Object object) {
        Method[] methods = object.getClass().getMethods();
        for (Method method : methods) {
            String mn = method.getName();
            if (mn.startsWith("set")) {
                try {
                    String tmp = mn.substring(4);
                    String first = mn.substring(3, 4);

                    String key = first.toLowerCase() + tmp;
                    String property = p.getProperty(key);
                    if (property != null) {
                        Class<?>[] pt = method.getParameterTypes();
                        if (pt != null && pt.length > 0) {
                            String cn = pt[0].getSimpleName();
                            Object arg;
                            if (cn.equals("int") || cn.equals("Integer")) {
                                arg = Integer.parseInt(property);
                            } else if (cn.equals("long") || cn.equals("Long")) {
                                arg = Long.parseLong(property);
                            } else if (cn.equals("double") || cn.equals("Double")) {
                                arg = Double.parseDouble(property);
                            } else if (cn.equals("boolean") || cn.equals("Boolean")) {
                                arg = Boolean.parseBoolean(property);
                            } else if (cn.equals("float") || cn.equals("Float")) {
                                arg = Float.parseFloat(property);
                            } else if (cn.equals("String")) {
                                arg = property;
                            } else {
                                continue;
                            }
                            method.invoke(object, arg);
                        }
                    }
                } catch (Throwable ignored) {
                }
            }
        }
    }

    public List<String> getDbs(){
        if(StringUtils.isBlank(dbs)){
            return Lists.newArrayList();
        }
        return Arrays.asList(dbs.split(","));
    }

    /**
     * 判断是否监控表
     * @param dbName    库名
     * @return
     */
    public boolean containsDb(String dbName){
        return getDbs().contains(dbName);
    }

    /**
     * 配置当前表是否需要进行监控
     * @param dbName        库名
     * @param tableName     表明
     * @return
     */
    public boolean containsTable(String dbName,String tableName){
        Predicate<String> predicate = tablePredicateMap.get(dbName);
        return predicate == null || predicate.test(tableName);
    }

    public boolean containsViewNotMatchField(String field){
        if(StringUtils.isBlank(viewNotMatchFields)){
            return false;
        }
        if(viewNotMatchFieldList == null){
            viewNotMatchFieldList = Arrays.stream(viewNotMatchFields.split(","))
                    .map(s -> s.replaceAll("_","").toLowerCase())
                    .collect(Collectors.toList());
        }
        return viewNotMatchFieldList.contains(field.replaceAll("_","").toLowerCase());
    }


    public void setMysqlAddr(String mysqlAddr) {
        this.mysqlAddr = mysqlAddr;
    }

    public void setMysqlPort(Integer mysqlPort) {
        this.mysqlPort = mysqlPort;
    }

    public void setMysqlUsername(String mysqlUsername) {
        this.mysqlUsername = mysqlUsername;
    }

    public void setMysqlPassword(String mysqlPassword) {
        this.mysqlPassword = mysqlPassword;
    }

    public void setBinlogFilename(String binlogFilename) {
        this.binlogFilename = binlogFilename;
    }

    public void setNextPosition(Long nextPosition) {
        this.nextPosition = nextPosition;
    }

    public void setMaxTransactionRows(Integer maxTransactionRows) {
        this.maxTransactionRows = maxTransactionRows;
    }

    public void setStartType(String startType) {
        this.startType = startType;
    }

    public void setMongoAddr(String mongoAddr) {
        this.mongoAddr = mongoAddr;
    }

    public void setThreadPoolNum(int threadPoolNum) {
        this.threadPoolNum = threadPoolNum;
    }

    public void setMongoPort(Integer mongoPort) {
        this.mongoPort = mongoPort;
    }

    public void setMongoDb(String mongoDb) {
        this.mongoDb = mongoDb;
    }

    public void setMongoCollection(String mongoCollection) {
        this.mongoCollection = mongoCollection;
    }

    public void setMongoUsername(String mongoUsername) {
        this.mongoUsername = mongoUsername;
    }

    public void setMongoPassword(String mongoPassword) {
        this.mongoPassword = mongoPassword;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public void setInterval(long interval) {
        this.interval = interval;
    }

    public void setDbs(String dbs) {
        this.dbs = dbs;
    }

    public void setViewNotMatchFields(String viewNotMatchFields) {
        this.viewNotMatchFields = viewNotMatchFields;
    }

    public void setAuthSource(String authSource) {
        this.authSource = authSource;
    }

    public static void main(String[] args) {
        Pattern pattern = Pattern.compile("^-\\w+#(include|exclude)$");
        String str = "-portal#include1";
        Matcher matcher = pattern.matcher(str);
        System.out.println("matcher.matches() = " + matcher.matches());
    }
}