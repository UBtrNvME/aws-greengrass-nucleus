package com.aws.greengrass.mqttclient.spool;

import com.aws.greengrass.mqttclient.spool.sqlite;

public class SqliteSpool implements CloudMessageSpool {
    private final Database messages = new Database()

    @Override
    public SpoolMessage getMessageById(long messageId) {
        return messages.getMessageById(messageId);
    }
}