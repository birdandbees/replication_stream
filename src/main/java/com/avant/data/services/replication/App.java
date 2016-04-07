package com.avant.data.services.replication;
import javax.xml.transform.Result;
import com.benfante.experiments.libpqwrapper.swig.*;
import org.apache.log4j.Logger;

public class App {

    final static Logger logger = Logger.getLogger(App.class);
    public static void main(String[] args)
    {
        /*System.out.println(System.getProperty("java.library.path"));
        System.loadLibrary("pqswig");
        System.out.println("PQ Library Version: " + pq.PQlibVersion());
        SWIGTYPE_p_pg_conn connection = connectToPostgres("localhost", "5432", "test", "test", "test");

        SWIGTYPE_p_pg_result res = selectAllPerson(connection);

        printTuples(res);

        pq.PQclear(res);

        pq.PQfinish(connection);*/

        try {

            ChangeCaptureAdapter postgresAdapter = new ReplicationSQL("url", "user", "pass", "test");
            postgresAdapter.connect();
            AlertListener printAlert = new PrintAlert();
            postgresAdapter.register(printAlert);

        }
        catch (ServerInterruptedException e)
        {
            logger.fatal("Server can not start, exiting... ");
            logger.fatal(e.getMessage());
            System.exit(1);
        }
        catch (java.sql.SQLException e)
        {
            logger.error(e.getMessage());
        }
        catch (java.lang.Exception e)
        {
            logger.error(e.getMessage());
        }



    }

    private static SWIGTYPE_p_pg_conn connectToPostgres(String host, String port, String db, String username, String password) {
        SWIGTYPE_p_pg_conn connection = pq.PQconnectdb("postgresql://" + username + ":" + password + "@" + host + ":" + port + "/" + db);
        if (pq.PQstatus(connection) != ConnStatusType.CONNECTION_OK) {
            System.out.println("Connection to database failed: " + pq.PQerrorMessage(connection));
            exitNicely(connection);
        }
        return connection;
    }

    private static SWIGTYPE_p_pg_result selectAllPerson(SWIGTYPE_p_pg_conn connection) {
        SWIGTYPE_p_pg_result res = pq.PQexec(connection, "SELECT * FROM person");
        if (pq.PQresultStatus(res) != ExecStatusType.PGRES_TUPLES_OK) {
            System.out.println("SELECT failed: " + pq.PQerrorMessage(connection));
            pq.PQclear(res);
            exitNicely(connection);
        }
        return res;
    }

    private static void exitNicely(SWIGTYPE_p_pg_conn connection) {
        pq.PQfinish(connection);
        System.exit(0);
    }

    private static void printTuples(SWIGTYPE_p_pg_result res) {
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
    }
}
