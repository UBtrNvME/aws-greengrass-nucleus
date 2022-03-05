package com.aws.greengrass.mqttclient.spool;

import com.aws.greengrass.mqttclient.spool.sqlite;

public class SqliteSpool implements CloudMessageSpool {
    private final Database messages = new Database();

    @Override
    public SpoolMessage getMessageById(long messageId) {
        return messages.getById(messageId);
    }

    @Override
    public void addMessage(long messageId, SpoolMessage message) {
        messages.add(messageId, message);
    }

    @Override
    public void deleteMessageById(long messageId) {
        messages.deleteById(messageId);
    }
}