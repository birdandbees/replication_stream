package com.avant.data.services.replication;

/**
 * Created by jing on 5/9/16.
 */
public class StreamObj {
    public int xid;
    public String schema;
    public String table;
    public int uuid;
    public String event;
    public String event_ts;
    public String whodunnit;
    public DBRecord object;
    public DBRecord change_set;

}