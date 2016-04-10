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

    public class DB_DDL implements Runnable
    {
        private int part_start;
        private int part_end;
        public DB_DDL(int start, int end)
        {
            this.part_start = start;
            this.part_end = end;
        }
        public void run()
        {
            try {
                Connection db = Postgres.connectDB("jdbc:postgresql://localhost:5432/jtest", "postgres", "");
                StringBuilder sb = new StringBuilder();
                for (int i = this.part_start; i <= this.part_end; i++) {
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

    }

    @Before
    public void setup() throws java.sql.SQLException, java.lang.ClassNotFoundException
    {
        Connection db = Postgres.connectDB("jdbc:postgresql://localhost:5432/jtest", "postgres", "");
        String sql = "drop table test_table";
        Postgres.execUpdate(db, sql);
        sql = "create table if not exists test_table( key integer primary key, value text )";
        Postgres.execUpdate(db, sql);
        db.close();
    }

    @Test
    public void stressTestReplicationStream() throws Exception
    {
        ExecutorService executorService = Executors.newFixedThreadPool(100);
        for (int j = 1; j <= 10000; j= j+100)
        {
            Runnable worker = new DB_DDL(j, j + 100);
            executorService.execute(worker);
        }


        ReplicationSQL rSQL = new ReplicationSQL("jdbc:postgresql://localhost:5432/jtest", "postgres", null, "jtest2");
        rSQL.register(new PrintAlert());
        int counter = 0;
        int loop = 0;
        while(true) {
            counter += rSQL.getChanges(8, false);
            loop++;
            if( counter == 100000 ) break;
            if( loop > 20000000 ) break; // set an easy timeout
        }
        executorService.shutdown();
        while (!executorService.isTerminated()) {};
        assertEquals(counter, 100000);
    }

}
