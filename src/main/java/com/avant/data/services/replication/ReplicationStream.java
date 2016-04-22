package com.avant.data.services.replication;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jing on 3/30/16.
 */
public class ReplicationStream implements Stream {
    int last_xid;
    int xid;
    List<Message> data;
    String plugin_name;

    public ReplicationStream() {
        last_xid = 0;
        xid = 0;
        data = new ArrayList<>();

    }

    public Message[] getStringData() {
        return data.toArray(new Message[0]);
    }

    public Byte[] getBinaryData() {
        return null;
    }

    public void clearStringData() {
        data.clear();
    }

}
