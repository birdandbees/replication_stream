package com.avant.data.services.replication;

import org.junit.Before;
import org.junit.Test;
import java.sql.Connection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import static org.junit.Assert.*;

/**
 * Created by jing on 4/6/16.
 */
public class ReplicationSQLTest {

    @Before
    public void setup() throws java.sql.SQLException, java.lang.ClassNotFoundException
    {
        Connection db = Postgres.connectDB("jdbc:postgresql://localhost:5432/jtest", "postgres", "");
        String sql = "create table if not exists test_table( key integer primary key, value text )";
        Postgres.execUpdate(db, sql);
        sql = "delete from test_table";
        Postgres.execUpdate(db, sql);
        db.close();

    }

    @Test
    public void stressTestReplicationStream() throws Exception
    {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        //Executors.newFixedThreadPool(10);
        executorService.execute(new Runnable() {
            public void run() {
                try {
                    Connection db = Postgres.connectDB("jdbc:postgresql://localhost:5432/jtest", "postgres", "");
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < 100000; i++) {
                        sb.delete(0, sb.length());
                        sb.append("insert into test_table (key, value) values (");
                        sb.append(i);
                        sb.append(" , \' testing \' )");
                        Postgres.execUpdate(db, sb.toString());

                    }
                    db.close();
                }catch (Exception e)
                {
                    e.printStackTrace();
                }
            }
        });


        ReplicationSQL rSQL = new ReplicationSQL("jdbc:postgresql://localhost:5432/jtest", "postgres", null, "jtest2");
        rSQL.register(new PrintAlert());
        int counter = 0;
        int loop = 0;
        while(true) {
            counter += rSQL.getChanges(8);
            loop++;
            if( counter == 100000 ) break;
            if( loop > 20000000 ) break; // set an easy timeout
        }
        executorService.shutdown();
        assertEquals(counter, 100000);
    }

}
