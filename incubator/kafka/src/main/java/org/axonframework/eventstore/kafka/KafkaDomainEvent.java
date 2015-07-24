/*
 * Copyright (c) 2010-2014. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.axonframework.eventstore.kafka;

import org.axonframework.serializer.SerializedDomainEventData;
import org.axonframework.serializer.SerializedMetaData;
import org.axonframework.serializer.SerializedObject;
import org.axonframework.serializer.SerializedType;
import org.axonframework.serializer.SimpleSerializedObject;
import org.joda.time.DateTime;

import java.io.DataInput;
import java.io.IOException;
import java.io.Serializable;

/**
 * @author <a href="mailto:brent.n.douglas@gmail.com">Brent Douglas</a>
 * @since 2.5
 */
public class KafkaDomainEvent implements Serializable, SerializedDomainEventData<String>, SerializedType {
    long sequenceNumber;
    final String type;
    String aggregateIdentifier;
    String payload;
    String payloadType;
    String payloadRevision;
    String timestamp;
    String metadata;
    String eventIdentifier;

    public KafkaDomainEvent(final String type) {
        this.type = type;
    }

    transient SerializedObject<String> _metadata;
    transient SerializedObject<String> _payload;

    public void readKey(final DataInput in) throws IOException {
        sequenceNumber = in.readLong();
        aggregateIdentifier = in.readUTF();
    }

    public void readValue(final DataInput in) throws IOException {
        eventIdentifier = in.readUTF();
        timestamp = in.readUTF();
        payloadRevision = in.readUTF();
        payloadType = in.readUTF();
        metadata = in.readUTF();
        payload = in.readUTF();
    }

    @Override
    public String getEventIdentifier() {
        return eventIdentifier;
    }

    @Override
    public String getAggregateIdentifier() {
        return aggregateIdentifier;
    }

    @Override
    public long getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public DateTime getTimestamp() {
        return DateTime.parse(timestamp);
    }

    @Override
    public SerializedObject<String> getMetaData() {
        return _metadata != null ? _metadata : (_metadata = new SerializedMetaData<String>(metadata, String.class));
    }

    @Override
    public SerializedObject<String> getPayload() {
        return _payload != null ? _payload : (_payload = new SimpleSerializedObject<String>(payload, String.class, payloadType, payloadRevision));
    }

    @Override
    public String getName() {
        return payloadType;
    }

    @Override
    public String getRevision() {
        return payloadRevision;
    }
}
