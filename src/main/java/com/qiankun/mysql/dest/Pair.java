package com.qiankun.mysql.dest;

import java.io.Serializable;

/**
 * @Description:
 * @Date : 2023/11/08 20:03
 * @Auther : tiankun
 */
public class Pair<T1,T2> implements Serializable {

    private static final long serialVersionUID = 494709767137042951L;

    T1 before;

    T2 after;

    public Pair(T1 before, T2 after) {
        this.before = before;
        this.after = after;
    }

    public T1 getBefore() {
        return before;
    }

    public void setBefore(T1 before) {
        this.before = before;
    }

    public T2 getAfter() {
        return after;
    }

    public void setAfter(T2 after) {
        this.after = after;
    }
}
