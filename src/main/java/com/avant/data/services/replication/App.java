package com.avant.data.services.replication;

import org.apache.log4j.Logger;

public class App {

    final static Logger logger = Logger.getLogger(App.class);

    public static void main(String[] args) {
        //TO-DO: signal handling
        try {
            ChangeCaptureAdapter postgresAdapter = new ReplicationSQL("jdbc:postgresql://localhost:5432/jtest", "postgres", null, "jjjtest");
            postgresAdapter.connect();
            AlertListener printAlert = new PrintAlert();
            postgresAdapter.register(printAlert);
            AvantProducer producer = new KafkaJsonProducer("test", null);
            int counter = 0;
            while (true) {

                int changes = postgresAdapter.getChanges(8, false);
                counter += changes;
                if (counter % 100 == 0) {
                    producer.flush();

                }
                if (changes > 0) {
                    postgresAdapter.pushChanges(postgresAdapter.getStream(), producer);
                }
            }

            //producer.close();

        } catch (ServerInterruptedException e) {
            logger.fatal("Server can not start, exiting... ");
            logger.fatal(e.getMessage());
            System.exit(1);
        } catch (java.sql.SQLException e) {
            logger.error(e.getMessage());
        } catch (java.lang.Exception e) {
            logger.error(e.getMessage());
        }


    }
}
