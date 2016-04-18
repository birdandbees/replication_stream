package com.avant.data.services.replication;

/**
 * Created by jing on 4/6/16.
 */
public class ServerInterruptedException extends Exception {
    public ServerInterruptedException() {
        super();
    }

    public ServerInterruptedException(String message) {
        super(message);
    }
}
