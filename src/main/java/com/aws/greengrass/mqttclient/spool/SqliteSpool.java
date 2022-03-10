/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttclient.spool;

import com.aws.greengrass.mqttclient.spool.sqlite.Database;
import java.sql.SQLException;

public class SqliteSpool implements CloudMessageSpool {
    private final Database messages = new Database("/tmp/spool/");

    @Override
    public SpoolMessage getMessageById(long messageId) {
        try {
            return messages.getMessageById(messageId);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void add(long messageId, SpoolMessage message) {
        try {
            messages.add(messageId, message);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void removeMessageById(long messageId) {
        try {
            messages.removeMessageById(messageId);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public Long[] getAllMessageIds() {
        try {
            return messages.getAllMessageIds();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Long getMessageQueueSizeInBytes() {
        Long size = 0;
        try {
            SpoolMessage[] data = messages.getAllMessages();
            for (SpoolMessage message : data) {
                size += message.getRequest().getPayload().length;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return size;
    }
}