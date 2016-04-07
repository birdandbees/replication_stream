package com.avant.data.services.replication;

/**
 * Created by jing on 4/6/16.
 */
public class PrintAlert implements AlertListener{
    public void alert(String messages)
    {
        System.out.println(messages);
    }
}
