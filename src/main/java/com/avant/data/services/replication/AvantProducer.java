package com.avant.data.services.replication;

/**
 * Created by jing on 4/11/16.
 */
public interface AvantProducer {

    void push(Stream stream);
}
