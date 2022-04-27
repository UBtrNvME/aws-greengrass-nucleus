/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttclient.spool.sqlite;

import com.aws.greengrass.mqttclient.PublishRequest;
import com.aws.greengrass.mqttclient.spool.SpoolMessage;
import software.amazon.awssdk.crt.mqtt.QualityOfService;

import java.util.concurrent.atomic.AtomicInteger;

class PublishRequestDTO implements java.io.Serializable {
    private final String topic;
    private final QualityOfService qos;
    private final boolean retain;
    private final byte[] payload;

    public PublishRequestDTO(String topic, software.amazon.awssdk.crt.mqtt.QualityOfService qos, boolean retain,
                             byte[] payload) {
        this.topic = topic;
        this.qos = qos;
        this.retain = retain;
        this.payload = payload;
    }

    public PublishRequestDTO(PublishRequest request) {
        this.topic = request.getTopic();
        this.qos = request.getQos();
        this.retain = request.isRetain();
        this.payload = request.getPayload();
    }

    public PublishRequest getPublishRequest() {
        return PublishRequest.builder().topic(topic).qos(qos).retain(retain).payload(payload).build();
    }
}

public class MessageDTO implements java.io.Serializable {
    private final long id;
    private final AtomicInteger retried;
    private final PublishRequestDTO request;

    public MessageDTO(long id, int retried, PublishRequestDTO request) {
        this.id = id;
        this.retried = new AtomicInteger(retried);
        this.request = request;
    }

    public MessageDTO(SpoolMessage spoolMessage) {
        this.id = spoolMessage.getId();
        this.retried = spoolMessage.getRetried();
        this.request = new PublishRequestDTO(spoolMessage.getRequest());
    }

    public SpoolMessage getSpoolMessage() {
        return SpoolMessage.builder().id(id).retried(retried).request(request.getPublishRequest()).build();
    }
}
