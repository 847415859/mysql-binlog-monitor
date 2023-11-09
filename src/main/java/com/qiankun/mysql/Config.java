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

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


public class Config {

    public String mysqlAddr;
    public Integer mysqlPort;
    public String mysqlUsername;
    public String mysqlPassword;

    /**
     * 需要监控的库，如果不设置则监控所有
     */
    public String dbs;

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
    public String mongoCollection = "ModelLog";
    public String mongoUsername = "root";
    public String mongoPassword = "root";



    public void load() throws IOException {
        InputStream in = Config.class.getClassLoader().getResourceAsStream("mysql-monitor.conf");
        Properties properties = new Properties();
        properties.load(in);
        properties2Object(properties, this);
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
}