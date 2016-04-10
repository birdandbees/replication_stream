package com.avant.data.services.replication;

/**
 * Created by jing on 4/6/16.
 */
public interface ChangeCaptureAdapter {

    void connect() throws Exception;
    int getChanges(int numOfChanges, boolean peek) throws Exception;
    void pushChanges(Stream stream);
    void register(AlertListener al);
}
