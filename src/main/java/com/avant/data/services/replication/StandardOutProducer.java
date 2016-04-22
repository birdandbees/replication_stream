package com.avant.data.services.replication;

/**
 * Created by jing on 4/11/16.
 */
public class StandardOutProducer implements AvantProducer {
    public void push(Stream stream) {
        System.out.println(stream.getStringData());
    }
    public void flush(){};
    public void close(){};
}
