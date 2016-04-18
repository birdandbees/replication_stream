package com.avant.data.services.replication;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by jing on 4/4/16.
 */
public class Postgres {

    public static Connection connectDB(String url, String username, String pass) throws java.lang.ClassNotFoundException, java.sql.SQLException {
        Class.forName("org.postgresql.Driver");
        Connection db = DriverManager.getConnection(url, username, pass);
        return db;
    }

    public static ResultSet execQuery(Connection db, String sql) throws java.sql.SQLException {
        Statement st = db.createStatement();
        ResultSet rs = st.executeQuery(sql);
        return rs;
    }

    public static int execUpdate(Connection db, String sql) throws java.sql.SQLException {
        Statement st = db.createStatement();
        //st.close();
        return st.executeUpdate(sql);
    }

    public static void closeResultSet(ResultSet result) throws java.sql.SQLException {
        result.close();
    }


    public static void main(String[] args) {

    }

}
