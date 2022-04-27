/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttclient.spool.sqlite;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.mqttclient.spool.SpoolMessage;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

public class Database {

    // Database name
    private static final String DATABASE_NAME = "spool.db";
    // Table name
    private static final String TABLE_NAME = "messages";

    // Columns
    private static final String KEY_ID = "id";
    private static final String KEY_MESSAGE = "message";

    private static final Logger logger = LogManager.getLogger(Database.class);
    private Connection db;

    /**
     * Constructor.
     *
     * @param databaseLocation path to database
     */
    public Database(String databaseLocation) {
        // Create a database connection
        String fullDatabaseName = databaseLocation + DATABASE_NAME;
        try {
            Class.forName("org.sqlite.JDBC");
            db = java.sql.DriverManager.getConnection("jdbc:sqlite:" + fullDatabaseName);
            // Create table
            createTable();
        } catch (ClassNotFoundException e) {
            logger.atError().setCause(e).log("Sqlite class is not configured!");
        } catch (SQLException e) {
            logger.atError().setCause(e).log("SQLite related problem!");
        }
    }

    /**
     * Add message.
     *
     * @param id      id of the message
     * @param message message
     * @throws SQLException if something goes wrong
     */
    public void add(long id, SpoolMessage message) throws SQLException {
        // Insert a new row into the database
        byte[] blob = serialize(message);
        String sql = String.format("INSERT INTO %s (%s, %s) VALUES (?, ?);", TABLE_NAME, KEY_ID, KEY_MESSAGE);
        try (java.sql.PreparedStatement ps = db.prepareStatement(sql)) {
            ps.setLong(1, id);
            ps.setBytes(2, blob);
            ps.executeUpdate();
        }
    }

    /**
     * Create table.
     *
     * @throws SQLException if something goes wrong
     */
    private void createTable() throws SQLException {
        String sql = String.format("CREATE TABLE IF NOT EXISTS %s(%s INTEGER PRIMARY KEY, %s BLOB);", TABLE_NAME,
                KEY_ID, KEY_MESSAGE);
        try (java.sql.Statement statement = db.createStatement()) {
            statement.execute(sql);
        }
    }

    /**
     * Remove message by id.
     *
     * @param id id of the message
     * @throws SQLException if something goes wrong
     */
    public void removeMessageById(long id) throws SQLException {
        String sql = String.format("DELETE FROM %s WHERE %s = %s;", TABLE_NAME, KEY_ID, id);

        try (java.sql.Statement statement = db.createStatement()) {
            statement.executeUpdate(sql);
        }
    }

    /**
     * Get all message ids.
     *
     * @param id id of the message
     * @return SpoolMessage
     * @throws SQLException if something goes wrong
     */
    public SpoolMessage getMessageById(long id) throws SQLException {
        String sql = String.format("SELECT %s FROM %s WHERE %s = %s;", KEY_MESSAGE, TABLE_NAME, KEY_ID, id);
        try (java.sql.Statement statement = db.createStatement(); java.sql.ResultSet rs = statement.executeQuery(sql)) {
            return deserialize(rs.getBinaryStream(KEY_MESSAGE));
        }
    }

    /**
     * Get all messages.
     *
     * @return List of spool messages
     * @throws SQLException if something goes wrong
     */
    public SpoolMessage[] getAllMessages() throws SQLException {
        ArrayList<SpoolMessage> messages = new ArrayList<>();
        String sql = String.format("SELECT %s FROM %s;", KEY_MESSAGE, TABLE_NAME);
        try (java.sql.Statement statement = db.createStatement(); java.sql.ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                messages.add(deserialize(rs.getBinaryStream(KEY_MESSAGE)));
            }
        }
        return messages.toArray(new SpoolMessage[0]);
    }

    /**
     * Get all message ids.
     *
     * @return List of the Message ids
     * @throws SQLException if something goes wrong
     */
    public ArrayList<Long> getAllMessageIds() throws SQLException {
        ArrayList<Long> ids = new ArrayList<>();
        String sql = String.format("SELECT %s FROM %s ORDER BY %s ASC", KEY_ID, TABLE_NAME, KEY_ID);
        try (java.sql.Statement statement = db.createStatement(); java.sql.ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                ids.add(rs.getLong(KEY_ID));
            }
        }
        return ids;
    }

    /**
     * Serialize message.
     *
     * @param message message to serialize
     * @return byte array
     */
    private byte[] serialize(SpoolMessage message) {
        // Convert into bytes
        // TODO(Aitemir): better serialization!
        MessageDTO messageDto = new MessageDTO(message);
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); ObjectOutputStream oos =
                new ObjectOutputStream(bos)) {
            oos.writeObject(messageDto);
            oos.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            logger.atError().setCause(e).kv("message", message.getRequest()).kv("error", e).log("Failed to serialize "
                    + "message!");
        }
        return null;
    }

    /**
     * Deserialize stream.
     *
     * @param stream stream to deserialize from
     * @return spool message
     */
    private SpoolMessage deserialize(InputStream stream) {
        // Convert from bytes
        try (ObjectInputStream ois = new ObjectInputStream(stream)) {
            MessageDTO messageDto = (MessageDTO) ois.readObject();
            return messageDto.getSpoolMessage();
        } catch (IOException e) {
            logger.atError().setCause(e).log("Failed to deserialize message!");
        } catch (ClassNotFoundException e) {
            logger.atError().setCause(e).log("Class not found!");
        }
        return null;
    }

    public void deleteTable() throws SQLException {
        String sql = String.format("DROP TABLE %s;", TABLE_NAME);
        try (java.sql.Statement statement = db.createStatement()) {
            statement.execute(sql);
        }
    }
}