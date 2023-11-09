package com.qiankun.mysql.dest;

/**
 * @Description:
 * @Date : 2023/11/08 20:03
 * @Auther : tiankun
 */
public class Pair<T1,T2> {
    T1 before;

    T2 after;

    public Pair(T1 before, T2 after) {
        this.before = before;
        this.after = after;
    }
}
