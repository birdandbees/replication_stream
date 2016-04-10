/**
 * Created by jing on 4/4/16.
 */
package com.avant.data.services.replication;

import org.apache.log4j.Logger;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class ReplicationSQL implements ChangeCaptureAdapter {
    final static Logger logger = Logger.getLogger(ReplicationSQL.class);

    private Connection db;
    private String slot;
    private Pattern start;
    private Pattern end;
    private java.util.Vector alertListener = new java.util.Vector();
    private ReplicationStream stream;
    private List<String> buffer; // store previous batch in memory

    public void connect() throws java.sql.SQLException
    {
        if  ( !isExist(slot) )
        {
            createReplicationSlot("decode_raw");
        }
    }

    // implement a listener to publish alerts
    public void register(AlertListener al)
    {
        alertListener.add(al);
    }

    public void alertAll( String messages ) {
        for (java.util.Enumeration e=alertListener.elements(); e.hasMoreElements(); )
            ((AlertListener)e.nextElement()).alert(messages);
    }

    public ReplicationSQL(String url, String username, String pass, String slot) throws ServerInterruptedException
    {
        try {
            db = Postgres.connectDB(url, username, pass);
            this.slot = slot;
            start = Pattern.compile("^BEGIN");
            end = Pattern.compile("^COMMIT");
            stream = new ReplicationStream();
            buffer = new ArrayList<String>();
        }
        catch (java.sql.SQLException e )
        {
            logger.warn(e.getMessage());
            throw new ServerInterruptedException("DB connection error");
        }
        catch (java.lang.ClassNotFoundException e)
        {
            logger.warn(e.getMessage());
            throw new ServerInterruptedException("Postgres jdbc driver is not found");
        }

    }

    public boolean isExist(String slot) throws java.sql.SQLException
    {
        String sql = "select * from pg_replication_slots where name = \'" + slot + "\'";
        ResultSet rs = Postgres.execQuery(db, sql);
        boolean ret = rs.first();
        rs.close();
        return ret;

    }


    public void createReplicationSlot(String plugin)
    {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("select pg_create_logical_replication_slot \\(slot_name ");
            sb.append(slot);
            sb.append(", ");
            sb.append(plugin);
            sb.append(" \\)");
            Postgres.execUpdate(db, sb.toString());
        }
        catch (java.sql.SQLException e)
        {
            logger.warn("error creating replication slot");
            logger.warn(e.getMessage());
        }


    }

    public void dropReplicationSlot()
    {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("drop pg_drop_logical_replication_slot \\(slot_name ");
            sb.append(slot);
            sb.append(" \\)");
            Postgres.execUpdate(db, sb.toString());
        }
        catch (java.sql.SQLException e)
        {
            logger.warn("error dropping replication slot");
            logger.warn(e.getMessage());

        }

    }
    private int selectFromReplicationSlots(int numOfChanges, String sqlCommand, List<String> buffer) throws java.sql.SQLException
    {
        StringBuilder sb = new StringBuilder();
        sb.append("select location, xid, data from ");
        sb.append(sqlCommand);
        sb.append("('");
        sb.append(slot);
        sb.append("', NULL, ");
        sb.append(numOfChanges);
        sb.append(" )");
        ResultSet rs = Postgres.execQuery(db, sb.toString());
        if (buffer != null ) {
            buffer.clear();
        }
        int counter = 0;
        while (rs.next())
        {
            String location = rs.getString("location");
            int xid = rs.getInt("xid");
            String data = rs.getString("data");
            if (buffer != null) {
                buffer.add(data);
            }else {
                parseChanges(stream, location, xid, data, "decoder_raw");
            }
            counter++;

        }
        rs.close();
        return counter;

    }

    public int getChanges(int numOfChanges, boolean peek) throws java.sql.SQLException
    {
        if (peek)
        {
            selectFromReplicationSlots(numOfChanges, "pg_logical_slot_peek_changes", buffer);
        }
        return selectFromReplicationSlots(numOfChanges, "pg_logical_slot_get_changes", null);
    }

    private int parseChanges(ReplicationStream stream, String location, int xid, String data, String decoder)
    {
        stream.last_xid = stream.xid;
        stream.xid = xid;
        stream.data = data;
        // for now just check if xid is continuous
        // TODO: need to check location(LSN)
        if ( stream.xid != 0 && stream.xid - stream.last_xid > 1)
        {
            alertAll("xid is not continuous, server may lose messages");
            logger.warn("xid is not continuous, server may lose messages: " + stream.last_xid + "-" + stream.xid);
        }
        pushChanges(stream);
        return 0;
    }
    private int parseChanges(ReplicationStream stream, String location, int xid, String data)
    {
        if ( start.matcher(data).find())
        {
            stream.lsn_start = location;
            stream.last_xid = stream.xid;
            stream.xid = xid;
            stream.data = "";
            return 0;
        }
        if ( end.matcher(data).find())
        {
            stream.lsn_end = location;
            stream.last_lsn_start = stream.lsn_start;
            stream.last_lsn_end = stream.lsn_end;
            stream.xid = xid;
            pushChanges(stream);
            return 0;
        }
        stream.data += data + "\n";
        return 0;
    }

    public void pushChanges(Stream stream)
    {
        System.out.println(stream.getStringData());
    }


    public static void main(String[] args) throws Exception
    {

        ReplicationSQL rSQL = new ReplicationSQL("jdbc:postgresql://localhost:5432/jtest", "postgres", null, "jtest");
        while (true)
        {
            rSQL.getChanges(8, false);
            //Thread.sleep(5);

        }
    }
}
