/**
 * Created by jing on 4/4/16.
 */
package com.avant.data.services.replication;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class ReplicationSQL implements ChangeCaptureAdapter {
    private final static Logger logger = Logger.getLogger(ReplicationSQL.class);

    private Connection db;
    private String slot;
    private Pattern start;
    private Pattern end;
    private java.util.Vector alertListener = new java.util.Vector();
    public ReplicationStream stream;
    private List<Message> buffer; // store previous batch in memory

    public void connect() throws java.sql.SQLException {
        if (!isExist(slot)) {
            createReplicationSlot("wal2json");
        }
    }

    // implement a listener to publish alerts
    public void register(AlertListener al) {
        alertListener.add(al);
    }

    public void alertAll(String messages) {
        for (java.util.Enumeration e = alertListener.elements(); e.hasMoreElements(); )
            ((AlertListener) e.nextElement()).alert(messages);
    }

    public ReplicationSQL(String url, String username, String pass, String slot) throws ServerInterruptedException {
        try {
            db = Postgres.connectDB(url, username, pass);
            this.slot = slot;
            start = Pattern.compile("^BEGIN");
            end = Pattern.compile("^COMMIT");
            stream = new ReplicationStream();
            buffer = new ArrayList<>();
        } catch (java.sql.SQLException e) {
            logger.warn(e.getMessage());
            throw new ServerInterruptedException("DB connection error");
        } catch (java.lang.ClassNotFoundException e) {
            logger.warn(e.getMessage());
            throw new ServerInterruptedException("Postgres jdbc driver is not found");
        }

    }

    public boolean isExist(String slot) throws java.sql.SQLException {
        String sql = "select plugin, catalog_xmin from pg_replication_slots where slot_name = \'" + slot + "\'";
        ResultSet rs = Postgres.execQuery(db, sql);
        boolean ret = rs.isBeforeFirst();
        rs.close();
        if ( ret )
        {
            while (rs.next())
            {
                stream.plugin_name = rs.getString("plugin");
                stream.last_xid = rs.getInt("catalog_xmin");
            }
        }
        return ret;

    }


    public void createReplicationSlot(String plugin) {
        try {
            stream.plugin_name = plugin;
            StringBuilder sb = new StringBuilder();
            sb.append("select pg_create_logical_replication_slot (\'");
            sb.append(slot);
            sb.append("\', \'");
            sb.append(plugin);
            sb.append("\' )");
            Postgres.execUpdate(db, sb.toString());
        } catch (java.sql.SQLException e) {
            logger.fatal("error creating replication slot");
            logger.fatal(e.getMessage());
            System.exit(1);
        }


    }

    public void dropReplicationSlot() {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("drop pg_drop_logical_replication_slot \\(slot_name ");
            sb.append(slot);
            sb.append(" \\)");
            Postgres.execUpdate(db, sb.toString());
        } catch (java.sql.SQLException e) {
            logger.warn("error dropping replication slot");
            logger.warn(e.getMessage());

        }

    }

    private int selectFromReplicationSlots(int numOfChanges, String sqlCommand, List<Message> buffer) throws java.sql.SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("select location, xid, data from ");
        sb.append(sqlCommand);
        sb.append("('");
        sb.append(slot);
        sb.append("', NULL, ");
        sb.append(numOfChanges);
        sb.append(" )");
        ResultSet rs = Postgres.execQuery(db, sb.toString());
        if (buffer != null) {
            buffer.clear();
        }
        int counter = 0;
        while (rs.next()) {
            String location = rs.getString("location");
            int xid = rs.getInt("xid");
            String data = rs.getString("data");
            Message final_data = parseChanges(stream, location, xid, data);
            buffer.add(final_data);
            counter++;
        }
        rs.close();
        return counter;

    }

    public int getChanges(int numOfChanges, boolean peek) throws java.sql.SQLException {
        if (peek) {
            selectFromReplicationSlots(numOfChanges, "pg_logical_slot_peek_changes", buffer);
        }
        return selectFromReplicationSlots(numOfChanges, "pg_logical_slot_get_changes", stream.data);
    }

    private Message parseRawChanges(ReplicationStream stream, String location, int xid, String data)
    {
        stream.last_xid = stream.xid;
        stream.xid = xid;
        Message result = new Message();
        result.key = xid;
        result.content = data;
        // for now just check if xid is continuous
        // TODO: need to check location(LSN)
        if (stream.xid != 0 && stream.xid - stream.last_xid > 1 && !recoverMessageFromBuffer(stream.last_xid, stream.xid)) {
            alertAll("xid is not continuous, server may lose messages");
            logger.warn("xid is not continuous, server may lose messages: " + stream.last_xid + "-" + stream.xid);
        }
        return result;

    }

    private boolean recoverMessageFromBuffer(int last_xid, int xid)
    {
        boolean rescued = false;
        for (Message message : buffer)
        {
            if (message.key > last_xid && message.key < xid)
            {
                rescued = true;
                stream.data.add(message);
            }
        }

        return rescued;
    }

    private Message parseJsonChanges(ReplicationStream stream, String location, int xid, String data)
    {
        // no buffer rescue for json messages for now
        // since json messages are multi-rowed
        stream.last_xid = stream.xid;
        stream.xid = xid;
        Message result = new Message();
        result.key = xid;
        result.content = data;
        return result;
    }

    private Message parseChanges(ReplicationStream stream, String location, int xid, String data) {
        if (stream.plugin_name.compareTo("decoder_raw") == 0 )
        {
            return parseRawChanges(stream, location, xid, data);
        }

        if(stream.plugin_name.compareTo("wal2json") == 0)
        {
            return parseJsonChanges(stream, location, xid, data);
        }

        return null;
    }

    public void pushChanges(Stream stream, AvantProducer producer) {
        producer.push(stream);
        stream.clearStringData();
    }

    public Stream getStream() {
        return stream;
    }

    public static void main(String[] args) throws Exception {

        ReplicationSQL rSQL = new ReplicationSQL("jdbc:postgresql://localhost:5432/jtest", "postgres", null, "jtest");
        AvantProducer producer = new StandardOutProducer();
        while (true) {
            rSQL.getChanges(8, false);
            rSQL.pushChanges(rSQL.stream, producer);

        }
    }
}
