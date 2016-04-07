package com.avant.data.services.replication;

import com.benfante.experiments.libpqwrapper.swig.SWIGTYPE_p_pg_conn;

/**
 * Created by jing on 3/30/16.
 */
public class ReplicationStream implements Stream{
    int last_xid;
    int xid;
    String last_lsn_start;
    String last_lsn_end;
    String lsn_start;
    String lsn_end;
    String data;
    String connString;
    SWIGTYPE_p_pg_conn conn;
    int status;

    public ReplicationStream()
    {
        last_xid = 0;
        xid = 0;

    }
    public String getStringData()
    {
        return data;
    }
    public Byte[] getBinaryData() { return null; };
}
