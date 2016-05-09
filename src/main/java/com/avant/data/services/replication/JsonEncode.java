package com.avant.data.services.replication;

import com.google.gson.Gson;

import java.util.List;

/**
 * Created by jing on 5/9/16.
 */
public class JsonEncode {

    public static String encodeJsonFromArray(StreamObj input)
    {
        Gson gson = new Gson();
        return gson.toJson(input);

    }
}
