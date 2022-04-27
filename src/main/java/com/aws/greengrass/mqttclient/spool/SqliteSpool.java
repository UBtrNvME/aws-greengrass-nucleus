/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttclient.spool;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.mqttclient.spool.sqlite.Database;

import java.sql.SQLException;
import java.util.ArrayList;

public class SqliteSpool implements CloudMessageSpool {
    private final Database messages;
    private static final Logger logger = LogManager.getLogger(SqliteSpool.class);

    public SqliteSpool(String databaseLocation) {
        messages = new Database(databaseLocation);
    }

    @Override
    public SpoolMessage getMessageById(long messageId) {
        try {
            return messages.getMessageById(messageId);
        } catch (SQLException e) {
            logger.atDebug().kv("messageId", messageId).log("Message was not found!");
        }
        return null;
    }

    @Override
    public void add(long messageId, SpoolMessage message) {
        try {
            messages.add(messageId, message);
        } catch (SQLException e) {
            logger.atError().kv("error", e).log("Problem adding message!");
        }
    }

    @Override
    public void removeMessageById(long messageId) {
        try {
            messages.removeMessageById(messageId);
        } catch (SQLException e) {
            logger.atError().kv("error", e).log("Problem removing message by id!");
        }
    }

    /**
     * Get all message ids.
     *
     * @return Message ids
     */
    public ArrayList<Long> getAllMessageIds() {
        try {
            return messages.getAllMessageIds();
        } catch (SQLException e) {
            logger.atError().kv("error", e).log("Problem loading all message ids!");
        }
        return null;
    }

    /**
     * Get the size of the Queue.
     *
     * @return Size of the message queue
     */
    public Long getMessageQueueSizeInBytes() {
        long size = 0L;
        try {
            SpoolMessage[] data = messages.getAllMessages();
            for (SpoolMessage message : data) {
                size += (long) message.getRequest().getPayload().length;
            }
        } catch (SQLException e) {
            logger.atError().kv("error", e).log("Problem loading all messages!");
        }
        return size;
    }
}