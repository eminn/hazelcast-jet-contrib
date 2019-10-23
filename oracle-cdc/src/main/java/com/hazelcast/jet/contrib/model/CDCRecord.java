package com.hazelcast.jet.contrib.model;

import java.sql.Timestamp;
import java.util.Map;

/**
 * date: 2019-10-23
 * author: emindemirci
 */
public class CDCRecord {

    private long scn;
    private String segOwner;
    private String segName;
    private String sqlRedo;
    private Timestamp timestamp;
    private String operation;

    private Map<String, String> before;
    private Map<String, String> after;

    public CDCRecord(Map<String, String> before, Map<String, String> after) {
        this.before = before;
        this.after = after;
    }

    public long getScn() {
        return scn;
    }

    public void setScn(long scn) {
        this.scn = scn;
    }

    public String getSegOwner() {
        return segOwner;
    }

    public void setSegOwner(String segOwner) {
        this.segOwner = segOwner;
    }

    public String getSegName() {
        return segName;
    }

    public void setSegName(String segName) {
        this.segName = segName;
    }

    public String getSqlRedo() {
        return sqlRedo;
    }

    public void setSqlRedo(String sqlRedo) {
        this.sqlRedo = sqlRedo;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public Map<String, String> getBefore() {
        return before;
    }

    public void setBefore(Map<String, String> before) {
        this.before = before;
    }

    public Map<String, String> getAfter() {
        return after;
    }

    public void setAfter(Map<String, String> after) {
        this.after = after;
    }


    @Override
    public String toString() {
        return "CDCRecord{" +
                "scn=" + scn +
                ", segOwner='" + segOwner + '\'' +
                ", segName='" + segName + '\'' +
                ", sqlRedo='" + sqlRedo + '\'' +
                ", timestamp=" + timestamp +
                ", operation='" + operation + '\'' +
                ", before=" + before +
                ", after=" + after +
                '}';
    }
}
