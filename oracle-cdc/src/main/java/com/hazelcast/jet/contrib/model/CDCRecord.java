package com.hazelcast.jet.contrib.model;

import java.util.Map;

/**
 * date: 2019-10-23
 * author: emindemirci
 */
public class CDCRecord {


    private Map<String, String> before;
    private Map<String, String> after;

    public CDCRecord(Map<String, String> before, Map<String, String> after) {
        this.before = before;
        this.after = after;
    }
}
