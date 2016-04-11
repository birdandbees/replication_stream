package com.avant.data.services.replication;

/**
 * Created by jing on 4/6/16.
 */
public interface Stream {
    String  getStringData();
    int getKey();
    String getStringKey();
    Byte[] getBinaryData();
}
