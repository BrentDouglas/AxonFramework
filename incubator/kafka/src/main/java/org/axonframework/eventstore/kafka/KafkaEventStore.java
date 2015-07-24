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

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.Whitelist;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.axonframework.domain.DomainEventMessage;
import org.axonframework.domain.DomainEventStream;
import org.axonframework.domain.GenericDomainEventMessage;
import org.axonframework.domain.MetaData;
import org.axonframework.eventstore.EventVisitor;
import org.axonframework.eventstore.PartialStreamSupport;
import org.axonframework.eventstore.SnapshotEventStore;
import org.axonframework.eventstore.management.Criteria;
import org.axonframework.eventstore.management.CriteriaBuilder;
import org.axonframework.eventstore.management.EventStoreManagement;
import org.axonframework.serializer.Externalizer;
import org.axonframework.serializer.SerializedObject;
import org.axonframework.serializer.Serializer;
import org.axonframework.upcasting.SimpleUpcasterChain;
import org.axonframework.upcasting.UpcasterAware;
import org.axonframework.upcasting.UpcasterChain;
import org.joda.time.DateTime;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import static org.axonframework.serializer.MessageSerializer.serializeMetaData;
import static org.axonframework.serializer.MessageSerializer.serializePayload;
import static org.axonframework.upcasting.UpcastUtils.upcastAndDeserialize;

/**
 * An event store based on Apache Kafka. Each aggregate type is a separate partition.
 * Messages are composed of a key containing the aggregate identifier and sequence
 * number and the payload being the rest of the data. Snapshot events are written to
 * separate partitions based on the name of the aggregate with a configurable postfix.
 *
 * Supports storing events using Axon's {@link Serializer} which are then fed though
 * the supplied {@link UpcasterChain} as expected. Also supports using an
 * {@link Externalizer} where the burden of serialization, deserialization and
 * version management is delegated back to the consumer.
 *
 * Never removes events or snapshots.
 *
 * @author <a href="mailto:brent.n.douglas@gmail.com">Brent Douglas</a>
 * @since 2.5
 */
public class KafkaEventStore implements SnapshotEventStore, EventStoreManagement, UpcasterAware, PartialStreamSupport {

    /**
     * The postfix to use to identify snapshot partitions.
     */
    private static final String SNAPSHOT_POSTFIX = System.getProperty("org.axonframework.eventstore.kafka.snapshot.postfix", "__Snapshot");

    private UpcasterChain upcasterChain = SimpleUpcasterChain.EMPTY;
    final boolean skipUnknown;

    final ConsumerConfig consumerConfig;
    final Producer<byte[], byte[]> producer;

    final Serializer serializer;
    final Externalizer externalizer;


    private KafkaEventStore(final Properties consumerProps, final Properties producerProps, final boolean skipUnknown,
                            final Serializer serializer, final Externalizer externalizer) {
        if (externalizer == null && serializer == null) {
            throw new IllegalArgumentException("A Serializer or Externalizer must be provided.");
        }
        this.skipUnknown = skipUnknown;
        this.serializer = serializer;
        this.externalizer = externalizer;
        consumerProps.put("autooffset.reset", "smallest");
        this.consumerConfig = new ConsumerConfig(consumerProps);
        this.producer = new Producer<byte[], byte[]>(new kafka.producer.Producer<byte[], byte[]>(new ProducerConfig(producerProps)));
    }

    /**
     * Create a store with a {@link Serializer}.
     *
     * @param consumerProps Configuration for the consumer.
     * @param producerProps Configuration for the producer.
     * @param skipUnknown If unknown events should be ignored by the serializer.
     * @param serializer The serialized to read and write events with.
     */
    public KafkaEventStore(final Properties consumerProps, final Properties producerProps,
                           final boolean skipUnknown, final Serializer serializer) {
        this(consumerProps, producerProps, skipUnknown, serializer, null);
    }

    /**
     * Create am event store with an {@link Externalizer}.
     *
     * @param consumerProps Configuration for the consumer.
     * @param producerProps Configuration for the producer.
     * @param externalizer The externalizer to read and write events with.
     */
    public KafkaEventStore(final Properties consumerProps, final Properties producerProps, final Externalizer externalizer) {
        this(consumerProps, producerProps, false, null, externalizer);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void setUpcasterChain(final UpcasterChain upcasterChain) {
        this.upcasterChain = upcasterChain;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void appendEvents(final String type, final DomainEventStream events) {
        try {
            final Externalizer externalizer = this.externalizer;
            final List<KeyedMessage<byte[], byte[]>> msgs = new ArrayList<KeyedMessage<byte[], byte[]>>();
            final ByteArrayOutputStream _key = new ByteArrayOutputStream();
            final ByteArrayOutputStream _value = new ByteArrayOutputStream();
            final ReusableDataOutputStream key = new ReusableDataOutputStream(_key);
            final ReusableDataOutputStream value = new ReusableDataOutputStream(_value);
            final DataOutput k = key;
            final DataOutput v = value;
            while (events.hasNext()) {
                final DomainEventMessage event = events.next();
                final long sequenceNumber = event.getSequenceNumber();
                final long timestamp = event.getTimestamp().getMillis();
                final String eventIdentifier = event.getIdentifier();
                if (externalizer != null) {
                    final Object aggId = event.getAggregateIdentifier();
                    final Object payload = event.getPayload();
                    final MetaData metadata = event.getMetaData();

                    k.writeLong(sequenceNumber);
                    externalizer.writeTo(aggId, k);
                    v.writeUTF(eventIdentifier);
                    v.writeLong(timestamp);
                    externalizer.writeTo(metadata, v);
                    externalizer.writeTo(payload, v);
                } else {
                    final String aggregateIdentifier = event.getAggregateIdentifier().toString();
                    final SerializedObject<String> serializedPayloadObject = serializePayload(event, serializer, String.class);
                    final SerializedObject<String> serializedMetaDataObject = serializeMetaData(event, serializer, String.class);
                    final String payload = serializedPayloadObject.getData();
                    final String payloadType = serializedPayloadObject.getType().getName();
                    final String payloadRevision = serializedPayloadObject.getType().getRevision();
                    final String metadata = serializedMetaDataObject.getData();

                    k.writeLong(sequenceNumber);
                    k.writeUTF(aggregateIdentifier);
                    v.writeUTF(eventIdentifier);
                    v.writeLong(timestamp);
                    v.writeUTF(payloadRevision);
                    v.writeUTF(payloadType);
                    v.writeUTF(metadata);
                    v.writeUTF(payload);
                }
                key.flush();
                value.flush();
                msgs.add(new KeyedMessage<byte[], byte[]>(type, _key.toByteArray(), _value.toByteArray()));
                key.reset();
                value.reset();
                _key.reset();
                _value.reset();
            }
            producer.send(msgs);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void appendSnapshotEvent(final String type, final DomainEventMessage snapshotEvent) {
        appendEvents(type + SNAPSHOT_POSTFIX, new DomainEventStream() {
            boolean next = true;
            @Override
            public boolean hasNext() {
                return next;
            }

            @Override
            public DomainEventMessage next() {
                if (next) {
                    next = false;
                    return snapshotEvent;
                } else {
                    return null;
                }
            }

            @Override
            public DomainEventMessage peek() {
                return next ? snapshotEvent : null;
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DomainEventStream readEvents(final String type, final Object identifier) {
        try {
            return new KakfaEventStream(type, identifier, -1, -1);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DomainEventStream readEvents(final String type, final Object identifier, final long firstSequenceNumber) {
        try {
            return new KakfaEventStream(type, identifier, firstSequenceNumber, -1);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DomainEventStream readEvents(final String type, final Object identifier, final long firstSequenceNumber, final long lastSequenceNumber) {
        try {
            return new KakfaEventStream(type, identifier, firstSequenceNumber, lastSequenceNumber);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visitEvents(final EventVisitor visitor) {
        final KakfaEventStream stream;
        try {
            stream = new KakfaEventStream(null, null, -1, -1);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        while (stream.hasNext()) {
            visitor.doWithEvent(stream.next());
        }
    }

    /**
     * Not supported
     */
    @Override
    public void visitEvents(final Criteria criteria, final EventVisitor visitor) {
        throw new UnsupportedOperationException();
    }

    /**
     * Not supported
     */
    @Override
    public CriteriaBuilder newCriteriaBuilder() {
        throw new UnsupportedOperationException();
    }

    private class KakfaEventStream implements DomainEventStream {

        final boolean _any;
        final String _type;
        final String _aggregateIdentifier;
        final long _firstSequenceNumber;
        final long _lastSequenceNumber;

        final Iterator<KafkaStream<byte[], byte[]>> streams;
        ConsumerIterator<byte[], byte[]> stream = null;

        boolean finished = false;
        Object aggregateIdentifier;
        final KafkaDomainEvent event;
        DomainEventMessage<?> message;
        int messageIndex = 0;
        List<DomainEventMessage> messages;
        final ReusableByteArrayInputStream _key = new ReusableByteArrayInputStream(new byte[0]);
        final ReusableByteArrayInputStream _value = new ReusableByteArrayInputStream(new byte[0]);
        final DataInput key = new DataInputStream(_key);
        final DataInput value = new DataInputStream(_value);

        private KakfaEventStream(final String type, final Object aggregateIdentifier, final long firstSequenceNumber, final long lastSequenceNumber) throws IOException {
            this._firstSequenceNumber = firstSequenceNumber;
            this._lastSequenceNumber = lastSequenceNumber;
            if (type == null || aggregateIdentifier == null) {
                this._any = true;
                this._type = null;
                this._aggregateIdentifier = null;
            } else {
                this._any = false;
                this._type = type;
                this._aggregateIdentifier = aggregateIdentifier.toString();
            }
            this.event = new KafkaDomainEvent(type);
            this.streams = Consumer.createJavaConsumerConnector(consumerConfig)
                    .createMessageStreamsByFilter(type == null ? new Whitelist(".*") : new Whitelist(type))
                    .iterator();
            _advanceMessages();

        }

        @Override
        public boolean hasNext() {
            return !finished;
        }

        @Override
        public DomainEventMessage next() {
            _advanceMessages();
            if (this.finished) {
                return null;
            }
            if (externalizer == null) {
                return this.messages.get(this.messageIndex++);
            } else {
                final DomainEventMessage message = this.message;
                this.message = null;
                return message;
            }
        }

        @Override
        public DomainEventMessage peek() {
            _advanceMessages();
            if (this.finished) {
                return null;
            }
            if (externalizer == null) {
                return this.messages.get(this.messageIndex);
            } else {
                return this.message;
            }
        }

        /**
         * @param sequenceNumber The sequence number of the message.
         * @param aggregateIdentifier The aggregate identifier of the message.
         * @param msg The contents of the message.
         * @param externalizer The externalized to use if set.
         */
        private void _readEvent(final long sequenceNumber, final Object aggregateIdentifier, final byte[] msg,
                                final Externalizer externalizer) throws IOException {
            _value.setBuf(msg);
            final DataInput value = this.value;
            if (externalizer == null) {
                this.event.readValue(value);
                this.aggregateIdentifier = aggregateIdentifier;
            } else {
                try {
                    final String eventIdentifier = value.readUTF();
                    final DateTime timestamp = new DateTime(value.readLong());
                    final MetaData metaData = externalizer.readFrom(value);
                    final Object payload = externalizer.readFrom(value);
                    this.message = new GenericDomainEventMessage<Object>(
                            eventIdentifier,
                            timestamp,
                            aggregateIdentifier,
                            sequenceNumber,
                            payload,
                            metaData
                    );
                    this.aggregateIdentifier = aggregateIdentifier;
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        /**
         * @return true if a stream is set after this event is called.
         */
        private boolean _advanceStream() {
            if (stream == null || !stream.hasNext()) {
                if (streams.hasNext()) {
                    stream = streams.next().iterator();
                    return true;
                } else {
                    stream = null;
                    return false;
                }
            }
            return true;
        }

        /**
         * @return True if an event was read from the stream.
         */
        private boolean _advanceEvents() {
            final KafkaDomainEvent event = this.event;
            final String id = this._aggregateIdentifier;
            final Externalizer externalizer = KafkaEventStore.this.externalizer;
            if (!_advanceStream()) {
                return false;
            }
            final ConsumerIterator<byte[], byte[]> stream = this.stream;
            try {
                if (this._any) {
                    if (stream.hasNext()) {
                        final MessageAndMetadata<byte[], byte[]> msg = stream.next();
                        _key.setBuf(msg.key());
                        if (externalizer == null) {
                            event.readKey(key);
                            _readEvent(event.sequenceNumber, event.aggregateIdentifier, msg.message(), externalizer);
                        } else {
                            final long sn = key.readLong();
                            final Object aggId = externalizer.readFrom(key);
                            _readEvent(sn, aggId, msg.message(), externalizer);
                        }
                        return true;
                    } else {
                        this.stream = null;
                        this.aggregateIdentifier = null;
                        return false;
                    }
                }
                final long first = this._firstSequenceNumber;
                final long last = this._lastSequenceNumber;
                while (stream.hasNext()) {
                    final MessageAndMetadata<byte[], byte[]> msg = stream.next();
                    _key.setBuf(msg.key());
                    final long sn;
                    final Object aggId;
                    if (externalizer == null) {
                        event.readKey(key);
                        sn = event.sequenceNumber;
                        aggId = event.aggregateIdentifier;
                    } else {
                        sn = key.readLong();
                        aggId = externalizer.readFrom(key);
                    }
                    if ((first != -1 && sn < first) || (last != -1 && sn > last) || !id.equals(aggId)) {
                        continue;
                    }
                    _readEvent(sn, aggId, msg.message(), externalizer);
                    return true;
                }
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            this.aggregateIdentifier = null;
            return false;
        }

        private void _advanceMessages() {
            while (!finished) {
                if (externalizer == null) {
                    if (this.messages != null && this.messageIndex < this.messages.size()) {
                        return;
                    }
                } else {
                    if (this.message != null) {
                        return;
                    }
                }
                if (!_advanceEvents()) {
                    this.finished = true;
                    return;
                }
                if (externalizer == null) {
                    this.messageIndex = 0;
                    this.messages = upcastAndDeserialize(this.event, this.aggregateIdentifier, serializer, upcasterChain, skipUnknown);
                    return;
                }
            }
        }
    }
}
