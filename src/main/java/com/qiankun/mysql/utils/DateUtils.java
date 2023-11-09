package com.qiankun.mysql.utils;

import org.apache.commons.lang3.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Calendar;
import java.util.Date;
import java.util.Objects;

/**
 * @Description:
 * @Date : 2023/02/04 15:09
 * @Auther : tiankun
 */
public class DateUtils {

    public static final long DAY_TIME = 1_000L * 60L * 60L * 24L;
    public static final long HOUR_TIME = 1_000L * 60L * 60L;
    public static final long MINUTE_TIME = 1_000L * 60L;
    public static final long YEAR_TIME = 365 * DAY_TIME;
    public static final long WEEK_TIME = 7 * DAY_TIME;
    public static final long MONTH_TIME = 30 * DAY_TIME;

    public static final String STANDARD_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String STANDARD_SIMPLE_DATE_FORMAT = "yyyy-MM-dd";


    public static Map<String,SimpleDateFormat> dateFormatMap = new HashMap<>();

    public static String toHourDay(Date date){
        if(Objects.nonNull(date)){
            long residueDate = date.getTime() - System.currentTimeMillis();
            if (residueDate > 0) {
                long day = residueDate / (DAY_TIME);
                long hour = (residueDate - day * DAY_TIME ) / (HOUR_TIME) ;
                return day + "天" + hour + "时";
            }
        }
        return  null;
    }


    public static String toMinuteHourDay(Date date){
        if(Objects.nonNull(date)){
            long residueDate = date.getTime() - System.currentTimeMillis();
            if (residueDate > 0) {
                long day = residueDate / (DAY_TIME);
                long hour = (residueDate - day * DAY_TIME ) / (HOUR_TIME) ;
                long minute = (residueDate - day * DAY_TIME - hour * HOUR_TIME ) / MINUTE_TIME;
                return day + "天" + hour + "时" + minute +"分";
            }
        }
        return  null;
    }

    public static Integer daysBetweenNow(Date smdate){
        if(smdate == null){
            return null;
        }
        return daysBetween(smdate,new Date());
    }

    /**
     * 计算两个日期之间相差的天数
     * @param smdate 较小的时间
     * @param bdate  较大的时间
     * @return 相差天数
     * @throws ParseException
     */
    public static Integer daysBetween(Date smdate,Date bdate) {
        try {
            SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
            smdate=sdf.parse(sdf.format(smdate));
            bdate=sdf.parse(sdf.format(bdate));
            Calendar cal = Calendar.getInstance();
            cal.setTime(smdate);
            long time1 = cal.getTimeInMillis();
            cal.setTime(bdate);
            long time2 = cal.getTimeInMillis();
            long betweenDays=(time2-time1)/(DAY_TIME);

            return Integer.parseInt(String.valueOf(betweenDays));
        } catch (ParseException | NumberFormatException e) {

        }
        return null;
    }


    /**
     * 时间根据时间格式转化为字符串
     * @param date
     * @return
     */
    public static String dateStr(Date date) {
        return dateStr(date,STANDARD_DATE_FORMAT);
    }

    /**
     * 时间根据时间格式转化为字符串
     * @param date
     * @param dateFormat
     * @return
     */
    public static String dateStr(Date date, String dateFormat) {
        if(date == null){
            return null;
        }
        try {
            SimpleDateFormat simpleDateFormat = dateFormatMap.get(dateFormat);
            if(simpleDateFormat == null){
                simpleDateFormat = new SimpleDateFormat(dateFormat);
                dateFormatMap.put(dateFormat,simpleDateFormat);
            }
            return simpleDateFormat.format(date);
        } catch (Exception e) {
        }
        return null;
    }


    /**
     * 将字符串转化为时间类型
     * @param dateStr
     * @return
     */
    public static Date parseDate(String dateStr){
        return parseDate(dateStr, STANDARD_DATE_FORMAT);
    }

    /**
     * 将字符串转化为时间类型
     * @param dateStr
     * @param dateFormat
     * @return
     */
    public static Date parseDate(String dateStr, String dateFormat){
        if(StringUtils.isBlank(dateStr) || StringUtils.isBlank(dateFormat)){
            return null;
        }
        SimpleDateFormat simpleDateFormat = dateFormatMap.get(dateFormat);
        if(simpleDateFormat == null){
            simpleDateFormat = new SimpleDateFormat(dateFormat);
            dateFormatMap.put(dateFormat,simpleDateFormat);
        }
        try {
            return simpleDateFormat.parse(dateStr);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 获取两个时间相差天数
     * @param smallTime
     * @param bigTime
     * @return
     */
    public static Integer differentDaysByMillisecond(long smallTime, long bigTime) {
        if(bigTime <= smallTime){
            return 0;
        }
        long dif = bigTime - smallTime;
        return Integer.min((int) ((dif > 365L * DAY_TIME ? dif - DAY_TIME : dif  )  / (DAY_TIME)),365) ;
    }

    /**
     * 获取一周前的时间戳
     * @return
     */
    public static long recentWeekendTimeStamp(){
        return System.currentTimeMillis() - WEEK_TIME;
    }

    /**
     * 获取一周前的时间
     * @return
     */
    public static Date recentWeekendDate(){
        return new Date(recentWeekendTimeStamp());
    }


    /**
     * 获取一月前的时间戳
     * @return
     */
    public static long recentMonthTimeStamp(){
        return System.currentTimeMillis() - MONTH_TIME;
    }

    /**
     * 获取三个月前的时间戳
     * @return
     */
    public static long recentThreeMonthTimeStamp(){
        return System.currentTimeMillis() - 3 * MONTH_TIME;
    }

    /**
     * 获取一年前的时间戳
     * @return
     */
    public static long recentYearTimeStamp(){
        return System.currentTimeMillis() - YEAR_TIME;
    }

    /**
     * 获取当天的开始时间戳
     * @return
     */
    public static long getStartTime() {
        Calendar todayStart = Calendar.getInstance();
        todayStart.set(Calendar.SECOND, 0);
        todayStart.set(Calendar.MINUTE, 0);
        todayStart.set(Calendar.HOUR_OF_DAY, 0);
        todayStart.set(Calendar.MILLISECOND, 0);
        return todayStart.getTimeInMillis();
    }

    /**
     * 获取当天的结束时间戳
     * @return
     */
    public static long getEndTime() {
        Calendar todayEnd = Calendar.getInstance();
        todayEnd.set(Calendar.SECOND, 59);
        todayEnd.set(Calendar.MINUTE, 59);
        todayEnd.set(Calendar.HOUR_OF_DAY, 23);
        todayEnd.set(Calendar.MILLISECOND, 999);
        return todayEnd.getTimeInMillis();
    }

    /**
     * 获取指定日期的开始时间
     * @param date
     * @return
     */
    public static synchronized Date getStartTime(Date date) {
        return parseDate(dateStr(date,STANDARD_SIMPLE_DATE_FORMAT),STANDARD_SIMPLE_DATE_FORMAT);
    }

    /**
     * 获取指定日期结束时间
     * @param date
     * @return
     */
    public static synchronized Date getEndTime(Date date) {
        return parseDate(dateStr(new Date(date.getTime() + DAY_TIME),STANDARD_SIMPLE_DATE_FORMAT),STANDARD_SIMPLE_DATE_FORMAT);
    }
}
