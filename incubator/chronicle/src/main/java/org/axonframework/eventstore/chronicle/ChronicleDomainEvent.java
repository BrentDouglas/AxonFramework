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
package org.axonframework.eventstore.chronicle;

import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshallable;
import org.axonframework.serializer.SerializedDomainEventData;
import org.axonframework.serializer.SerializedMetaData;
import org.axonframework.serializer.SerializedObject;
import org.axonframework.serializer.SerializedType;
import org.axonframework.serializer.SimpleSerializedObject;
import org.joda.time.DateTime;

import java.io.Serializable;

/**
 * @author <a href="mailto:brent.n.douglas@gmail.com">Brent Douglas</a>
 * @since 2.5
 */
public class ChronicleDomainEvent implements Serializable, BytesMarshallable, SerializedDomainEventData<String>, SerializedType {
    long sequenceNumber;
    String type;
    String aggregateIdentifier;
    String payload;
    String payloadType;
    String payloadRevision;
    String timestamp;
    String metadata;
    String eventIdentifier;

    transient SerializedObject<String> _metadata;
    transient SerializedObject<String> _payload;

    @Override
    public void readMarshallable(final Bytes in) {
        sequenceNumber = in.readLong();
        type = in.readUTFΔ();
        aggregateIdentifier = in.readUTFΔ();
        eventIdentifier = in.readUTFΔ();
        timestamp = in.readUTFΔ();
        payloadRevision = in.readUTFΔ();
        payloadType = in.readUTFΔ();
        metadata = in.readUTFΔ();
        payload = in.readUTFΔ();
    }

    @Override
    public void writeMarshallable(final Bytes da) {
        da.writeLong(sequenceNumber);
        da.writeUTFΔ(type);
        da.writeUTFΔ(aggregateIdentifier);
        da.writeUTFΔ(eventIdentifier);
        da.writeUTFΔ(timestamp);
        da.writeUTFΔ(payloadRevision);
        da.writeUTFΔ(payloadType);
        da.writeUTFΔ(metadata);
        da.writeUTFΔ(payload);
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
