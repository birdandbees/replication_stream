package com.avant.data.services.replication;
import com.benfante.experiments.libpqwrapper.swig.*;
/**
 * Created by jing on 3/30/16.
 */
// Test class to use C library libqp
public class Replication {

    int replication_slot_create(ReplicationStream stream)
    {
        return 0;
    }

    int replication_slot_drop(ReplicationStream stream)
    {
        return 0;
    }

    public static SWIGTYPE_p_pg_conn connectToPostgres(ReplicationStream stream) {
        SWIGTYPE_p_pg_conn connection = pq.PQconnectdb(stream.connString);
        if (pq.PQstatus(connection) != ConnStatusType.CONNECTION_OK) {
            System.out.println("Connection to database failed: " + pq.PQerrorMessage(connection));
            //exitNicely(connection);
        }
        return connection;
    }

    public static int replication_stream_check(ReplicationStream stream)
    {
        SWIGTYPE_p_pg_result res = pq.PQexec(stream.conn, "IDENTIFY_SYSTEM");
        if (pq.PQresultStatus(res) != ExecStatusType.PGRES_TUPLES_OK) {
            System.out.println("IDENTIFY SYSTEM failed: " + pq.PQerrorMessage(stream.conn));
            pq.PQclear(res);
            return 1;
            //exitNicely(connection);
        }
        if (pq.PQntuples(res) != 1 || pq.PQnfields(res) < 4) {
            System.out.println("Unexpected IDENTIFY SYSTEM");
            pq.PQclear(res);
            return 1;
        }
        /* Check that the database name (fourth column of the result tuple) is non-null,
        /* implying a database-specific connection. */
        if (pq.PQgetisnull(res, 0, 3) == 1) {
            System.out.println("Not using a database-specific replication connection.");
            pq.PQclear(res);
            return 1;
        }

        pq.PQclear(res);

        return 0;

    }

    public static int replication_stream_start(ReplicationStream stream)
    {
        SWIGTYPE_p_pg_result res = pq.PQexec(stream.conn, "START_REPLICATION SLOT \"jtest\" LOGICAL 0/318C2A08 ");
        if (pq.PQresultStatus(res) != ExecStatusType.PGRES_COPY_BOTH)
        {
            System.out.println("couldn't send replication command " + pq.PQerrorMessage(stream.conn));
            pq.PQclear(res);
            return 1;
            //exitNicely(connection);
        }
        pq.PQclear(res);
        return 0;

    }

    public static int replication_stream_finish(ReplicationStream stream)
    {
        SWIGTYPE_p_pg_result  res = pq.PQgetResult(stream.conn);
        if (pq.PQresultStatus(res) != ExecStatusType.PGRES_COMMAND_OK) {
            System.out.println("Replication stream was unexpectedly terminated.");
            pq.PQclear(res);
            return 1;
        }
        pq.PQclear(res);
        return 0;

    }

    public static void replication_stream_poll(ReplicationStream stream)
    {
        String buffer[] = {""};
        int ret;
        int err;
        while (true) {
            ret = pq.PQgetCopyData(stream.conn, buffer, 0);
            err = 0;
            /*try {
                if (ret == 0 && pq.PQconsumeInput(stream.conn) == 1) {
                    while (pq.PQisBusy(stream.conn) == 1) {
                        Thread.sleep(2000);
                    }
                    SWIGTYPE_p_pg_result res = pq.PQgetResult(stream.conn);
                    int nTuples = pq.PQntuples(res);
                    int nFields = pq.PQnfields(res);
                    for (int i = 0; i < nFields; i++) {
                        System.out.printf("%-15s|", pq.PQfname(res, i));
                    }
                    System.out.println("");
                    for (int i = 0; i < nTuples; i++) {
                        for (int j = 0; j < nFields; j++) {
                            System.out.printf("%-15s|", pq.PQgetvalue(res, i, j));
                        }
                        System.out.println("");
                    }

                    System.out.println("inspect result");
                }


            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();

            }*/

            if (ret < 0) {
                if (ret == -1) {
                    err = replication_stream_finish(stream);
                } else {
                    //repl_error(stream, "Could not read from replication stream: %s",
                    //PQerrorMessage(stream->conn));
                    //err = EIO;
                }
                //if (buf) PQfreemem(buf);
                stream.status = ret;
                //return err;
            }

            if (ret > 0) {
                stream.status = 1;
                byte[] bytes = buffer[0].getBytes();
                System.out.println("inspect buffer");
            /*switch (buffer[0]) {
                case "k":
                    //err = parse_keepalive_message(stream, buf, ret);
                    break;
                case "w":
                    //err = parse_xlogdata_message(stream, buf, ret);
                    break;
                default:
                    //repl_error(stream, "Unknown streaming message type: \"%c\"", buf[0]);
                    //err = EIO;
            }*/
            } else {
                stream.status = 0;
            }

        }

        //Periodically let the server know up to which point we've consumed the stream.
        //if (err != 0) err = replication_stream_keepalive(stream);

        //if (buf) PQfreemem(buf);
        //return err;

        //return 0;
    }


    /*int replication_stream_keepalive(ReplicationStream stream)
    {
        int err = 0;
        if (stream->recvd_lsn != InvalidXLogRecPtr) {
            int64 now = current_time();
            if (now - stream->last_checkpoint > CHECKPOINT_INTERVAL_SEC * USECS_PER_SEC) {
                err = send_checkpoint(stream, now);
            }
        }
        return err;

        return 0;
    }
    *//* Parses a "Primary keepalive message" received from the server. It is packed binary
    * with the following structure:
    *
    *   - Byte1('k'): Identifies the message as a sender keepalive.
    *   - Int64: The current end of WAL on the server.
    *   - Int64: The server's system clock at the time of transmission, as microseconds
    *            since midnight on 2000-01-01.
    *   - Byte1: 1 means that the client should reply to this message as soon as possible,
    *            to avoid a timeout disconnect. 0 otherwise.
    *//*

    int parse_keepalive_messages(ReplicationStream stream)
    {
        if (buflen < 1 + 8 + 8 + 1) {
            repl_error(stream, "Keepalive message too small: %d bytes", buflen);
            return EIO;
        }

        int offset = 1; // start with 1 to skip the initial 'k' byte

        XLogRecPtr wal_pos = recvint64(&buf[offset]); offset += 8;
        *//* skip server clock timestamp *//*             offset += 8;
        bool reply_requested = buf[offset];           offset += 1;

        *//* Not 100% sure whether it's semantically correct to update our LSN position here --
        * the keepalive message indicates the latest position on the server, which might not
        * necessarily correspond to the latest position on the client. But this is what
        * pg_recvlogical does, so it's probably ok. *//*
        stream->recvd_lsn = Max(wal_pos, stream->recvd_lsn);

        #ifdef DEBUG
        fprintf(stderr, "Keepalive: wal_pos %X/%X, reply_requested %d\n",
                (uint32) (wal_pos >> 32), (uint32) wal_pos, reply_requested);
        #endif

        if (reply_requested) {
            return send_checkpoint(stream, current_time());
        }
        return 0;

        return 0;
    }
    *//* Parses a XLogData message received from the server. It is packed binary with the
    * following structure:
    *
    *   - Byte1('w'): Identifies the message as replication data.
    *   - Int64: The starting point of the WAL data in this message.
    *   - Int64: The current end of WAL on the server.
    *   - Int64: The server's system clock at the time of transmission, as microseconds
    *            since midnight on 2000-01-01.
    *   - Byte(n): The output from the logical replication output plugin.
    *//*

    int parse_xlogdata_messages(ReplicationStream stream)
    {
        int hdrlen = 1 + 8 + 8 + 8;

        if (buflen < hdrlen + 1) {
            repl_error(stream, "XLogData header too small: %d bytes", buflen);
            return EIO;
        }

        XLogRecPtr wal_pos = recvint64(&buf[1]);

        #ifdef DEBUG
        fprintf(stderr, "XLogData: wal_pos %X/%X\n", (uint32) (wal_pos >> 32), (uint32) wal_pos);
        #endif

        int err = parse_frame(stream->frame_reader, wal_pos, buf + hdrlen, buflen - hdrlen);
        if (err) {
            repl_error(stream, "Error parsing frame data: %s", avro_strerror());
        }

        stream->recvd_lsn = Max(wal_pos, stream->recvd_lsn);
        return err;
        return 0;
    }
    int send_checkpoint(ReplicationStream stream)
    {
        char buf[1 + 8 + 8 + 8 + 8 + 1];
        int offset = 0;

        buf[offset] = 'r';                          offset += 1;
        sendint64(stream->recvd_lsn, &buf[offset]); offset += 8;
        sendint64(stream->fsync_lsn, &buf[offset]); offset += 8;
        sendint64(InvalidXLogRecPtr, &buf[offset]); offset += 8; // only used by physical replication
        sendint64(now,               &buf[offset]); offset += 8;
        buf[offset] = 0;                            offset += 1;

        if (PQputCopyData(stream->conn, buf, offset) <= 0 || PQflush(stream->conn)) {
            repl_error(stream, "Could not send checkpoint to server: %s",
                    PQerrorMessage(stream->conn));
            return EIO;
        }

        #ifdef DEBUG
        fprintf(stderr, "Checkpoint: recvd_lsn %X/%X, fsync_lsn %X/%X\n",
                (uint32) (stream->recvd_lsn >> 32), (uint32) stream->recvd_lsn,
                (uint32) (stream->fsync_lsn >> 32), (uint32) stream->fsync_lsn);
        #endif

        stream->last_checkpoint = now;
        return 0;

        return 0;
    }*/
    public static void main(String[] args)
    {
        System.out.println(System.getProperty("java.library.path"));
        System.loadLibrary("pqswig");
        System.out.println("PQ Library Version: " + pq.PQlibVersion());
        ReplicationStream stream = new ReplicationStream();
        stream.connString = "postgresql://postgres@localhost/jtest?connect_timeout=10&replication=database";
        stream.conn = connectToPostgres(stream);
        replication_stream_check(stream);
        if (replication_stream_start(stream) == 0 )
        {
            replication_stream_poll(stream);

        }
        System.out.println("test is done");
    }

}
