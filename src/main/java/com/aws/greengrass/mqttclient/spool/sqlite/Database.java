package com.aws.greengrass.mqttclient.spool.sqlite;

import com.aws.greengrass.mqttclient.spool.SpoolMessage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.sql.Blob;
import java.sql.Connection;

class Database {

    // Database name
    private static final String DATABASE_NAME = "spool.db";
    // Table name
    private static final String TABLE_NAME = "messages";

    // Columns
    private static final String KEY_ID = "id";
    private static final String KEY_MESSAGE = "message";

    private Connection db;

    public Database(String databaseLocation) {
        // Create a database connection
        String fullDatabaseName = databaseLocation + DATABASE_NAME;
        Class.forName("org.sqlite.JDBC");
        db = java.sql.DriverManager.getConnection("jdbc:sqlite:" + fullDatabaseName);
        // Create table
        createTable();
    }

    public void add(long id, SpoolMessage message) {
        // Insert a new row into the database
        Blob blob = serialize(message);
        String INSERT_ITEM = "INSERT INTO " + TABLE_NAME + " (" + KEY_ID + ", " + KEY_MESSAGE + ") VALUES (" + id + ","
                + blob + ")";
        db.createStatement().executeUpdate(INSERT_ITEM);
    }

    private void createTable() {
        String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + "(" + KEY_ID + " INTEGER PRIMARY KEY,"
                + KEY_MESSAGE
                + " TEXT)";
        db.createStatement().execute(CREATE_TABLE);
    }

    public void deleteById(Long id) {
        String DELETE_ITEM_BY_ID = "DELETE FROM " + TABLE_NAME + " WHERE " + KEY_ID + " = " + id;
        db.createStatement().executeUpdate(DELETE_ITEM_BY_ID);
    }

    public SpoolMesage getById(Long id) {
        String GET_ITEM_BY_ID = "SELECT " + KEY_MESSAGE + " FROM " + TABLE_NAME + " WHERE " + KEY_ID + " = " + id;
        return deserialize(db.createStatement().executeQuery(GET_ITEM_BY_ID).getBlob(KEY_MESSAGE));
    }

    private byte[] serialize(SpoolMessage message) {
        // Convert into bytes
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(message);
            byte[] asBytes = baos.toByteArray();
            ByteArrayInputStream bais = new ByteArrayInputStream(asBytes);
            return asBytes;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    private SpoolMessage deserialize(byte[] bytes) {
        // Convert from bytes
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bais);
            return (SpoolMessage) ois.readObject();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }
}