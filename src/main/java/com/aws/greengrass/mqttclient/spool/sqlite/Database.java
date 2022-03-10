/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttclient.spool.sqlite;

import com.aws.greengrass.mqttclient.spool.SpoolMessage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.InputStream;
import java.lang.ClassNotFoundException;
import java.sql.SQLException;

public class Database {

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
        try {
            Class.forName("org.sqlite.JDBC");
            db = java.sql.DriverManager.getConnection("jdbc:sqlite:" + fullDatabaseName);
            // Create table
            createTable();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public void add(long id, SpoolMessage message) throws SQLException {
        // Insert a new row into the database
        byte[] blob = serialize(message);
        String sql = "INSERT INTO " + TABLE_NAME + " (" + KEY_ID + ", " + KEY_MESSAGE + ") VALUES (" + id + ","
                + blob + ")";
        db.createStatement().executeUpdate(sql);
    }

    private void createTable() throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + "(" + KEY_ID + " INTEGER PRIMARY KEY,"
                + KEY_MESSAGE
                + " TEXT)";
        db.createStatement().execute(sql);
    }

    public void removeMessageById(Long id) throws SQLException {
        String sql = "DELETE FROM " + TABLE_NAME + " WHERE " + KEY_ID + " = " + id;
        db.createStatement().executeUpdate(sql);
    }

    public SpoolMessage getMessageById(Long id) throws SQLException {
        String sql = "SELECT " + KEY_MESSAGE + " FROM " + TABLE_NAME + " WHERE " + KEY_ID + " = " + id;
        return deserialize(db.createStatement().executeQuery(sql).getBlob(KEY_MESSAGE).getBinaryStream());
    }

    public SpoolMessage[] getAllMessages() throws SQLException {
        ArrayList<SpoolMessage> messages = new ArrayList<SpoolMessage>();
        String sql = "SELECT " + KEY_MESSAGE + " FROM " + TABLE_NAME;
        java.sql.ResultSet rs = db.createStatement().executeQuery(sql);
        while (rs.next()) {
            messages.add(deserialize(rs.getBlob(KEY_MESSAGE).getBinaryStream()));
        }
        return messages.toArray(new SpoolMessage[messages.size()]);
    }

    public Long[] getAllMessageIds() throws SQLException {
        ArrayList<Long> ids = new ArrayList<Long>();
        String sql = "SELECT " + KEY_ID + " FROM " + TABLE_NAME + " ORDER BY " + KEY_ID + " ASC";
        java.sql.ResultSet rs = db.createStatement().executeQuery(sql);
        while (rs.next()) {
            ids.add(rs.getLong(KEY_ID));
        }
        return ids.toArray(new Long[ids.size()]);
    }

    private byte[] serialize(SpoolMessage message) {
        // Convert into bytes
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(message);
            oos.close();
            byte[] asBytes = baos.toByteArray();
            return asBytes;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    private SpoolMessage deserialize(InputStream stream) {
        // Convert from bytes
        ObjectInputStream ois = null;
        try {
            ois = new ObjectInputStream(stream);
            return (SpoolMessage) ois.readObject();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            if (ois != null) {
                try {
                    ois.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }
}