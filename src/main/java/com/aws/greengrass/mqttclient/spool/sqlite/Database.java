package com.aws.greengrass.mqttclient.spool.sqlite;

Class.forName("org.sqlite.JDBC");

class Database {
    // Database name
    private static final String DATABASE_NAME = "spool.db";
    // Table name
    private static final String TABLE_NAME = "messages";

    // Columns
    private static final String KEY_ID = "id";
    private static final String KEY_MESSAGE = "message";
    

    private 

    public Database(String databaseLocation) {
        // Create a database connection
        String fullDatabaseName = databaseLocation + DATABASE_NAME;

        createTable();
    }

    public void add(long id, SpoolMessage message) {
    
    }

    private void createTable() {
    
    }

    public void delete() {
    
    }

    public byte[] get() {
        
    }
}