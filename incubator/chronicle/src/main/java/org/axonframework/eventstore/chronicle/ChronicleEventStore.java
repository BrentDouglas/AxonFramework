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

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
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
import org.axonframework.serializer.SerializedObject;
import org.axonframework.serializer.Serializer;
import org.axonframework.serializer.Externalizer;
import org.axonframework.upcasting.SimpleUpcasterChain;
import org.axonframework.upcasting.UpcasterAware;
import org.axonframework.upcasting.UpcasterChain;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.List;

import static org.axonframework.eventstore.chronicle.Util.hash;
import static org.axonframework.eventstore.chronicle.Util.len;
import static org.axonframework.serializer.MessageSerializer.serializeMetaData;
import static org.axonframework.serializer.MessageSerializer.serializePayload;
import static org.axonframework.upcasting.UpcastUtils.upcastAndDeserialize;

/**
 * An event store based on OpenHFT's Chronicle Queue. Stores event in four queue's,
 * one each for events and snapshots and and index queue for each. Indices are used
 * to check if an event is probably from the queried aggregate.
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
public class ChronicleEventStore implements SnapshotEventStore, EventStoreManagement, UpcasterAware, PartialStreamSupport {

    private UpcasterChain upcasterChain = SimpleUpcasterChain.EMPTY;
    final boolean skipUnknown;

    final Chronicle events;
    final ExcerptAppender eventsAppender;

    final Chronicle eventsIndex;
    final ExcerptAppender eventsIndexAppender;

    final Chronicle snapshots;
    final ExcerptAppender snapshotsAppender;

    final Chronicle snapshotsIndex;
    final ExcerptAppender snapshotsIndexAppender;

    final Serializer serializer;
    final Externalizer externalizer;


    private ChronicleEventStore(final Chronicle events, final Chronicle eventsIndex, final Chronicle snapshots,
                                final Chronicle snapshotsIndex, final boolean skipUnknown, final Serializer serializer,
                                final Externalizer externalizer) throws IOException {
        if (externalizer == null && serializer == null) {
            throw new IllegalArgumentException("A Serializer or Externalizer must be provided.");
        }
        this.skipUnknown = skipUnknown;
        this.serializer = serializer;
        this.externalizer = externalizer;
        this.events = events;
        this.eventsIndex = eventsIndex;
        this.snapshots = snapshots;
        this.snapshotsIndex = snapshotsIndex;
        this.eventsAppender = events.createAppender();
        this.eventsIndexAppender = eventsIndex.createAppender();
        this.snapshotsAppender = snapshots.createAppender();
        this.snapshotsIndexAppender = snapshotsIndex.createAppender();
    }

    /**
     * Create a store with a {@link Serializer}.
     *
     * @param events The queue to store events.
     * @param eventsIndex The queue to index events.
     * @param snapshots The queue to store snapshots.
     * @param snapshotsIndex The queue to index snapshots.
     * @param skipUnknown If unknown events should be ignored by the serializer.
     * @param serializer The serialized to read and write events with.
     * @throws IOException If the queues cannot be created.
     */
    public ChronicleEventStore(final Chronicle events, final Chronicle eventsIndex, final Chronicle snapshots,
                                final Chronicle snapshotsIndex, final boolean skipUnknown, final Serializer serializer) throws IOException {
        this(events, eventsIndex, snapshots, snapshotsIndex, skipUnknown, serializer, null);
    }

    /**
     * Create am event store with an {@link Externalizer}.
     *
     * @param events The queue to store events.
     * @param eventsIndex The queue to index events.
     * @param snapshots The queue to store snapshots.
     * @param snapshotsIndex The queue to index snapshots.
     * @param externalizer The externalizer to read and write events with.
     * @throws IOException If the queues cannot be created.
     */
    public ChronicleEventStore(final Chronicle events, final Chronicle eventsIndex, final Chronicle snapshots,
                               final Chronicle snapshotsIndex, final Externalizer externalizer) throws IOException {
        this(events, eventsIndex, snapshots, snapshotsIndex, false, null, externalizer);
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
        final ExcerptAppender da = this.eventsAppender;
        final ExcerptAppender ia = this.eventsIndexAppender;
        final Externalizer externalizer = this.externalizer;
        while (events.hasNext()) {
            _writeEvent(type, da, ia, externalizer, events.next());
        }
    }

    private void _writeEvent(final String type, final ExcerptAppender da, final ExcerptAppender ia, final Externalizer externalizer,
                             final DomainEventMessage event) {
        String aggregateIdentifier;
        final long sequenceNumber = event.getSequenceNumber();
        final long timestamp = event.getTimestamp().getMillis();
        final String eventIdentifier = event.getIdentifier();
        if (externalizer != null) {
            final Object aggId = event.getAggregateIdentifier();
            final Object payload = event.getPayload();
            final MetaData metadata = event.getMetaData();

            final int size =  8 //sequenceNumber
                    + len(type)
                    + externalizer.size(aggId)
                    + len(eventIdentifier)
                    + 8 //timestamp
                    + externalizer.size(metadata)
                    + externalizer.size(payload);

            da.startExcerpt(size);
            da.writeLong(sequenceNumber);
            da.writeUTFΔ(type);
            try {
                aggregateIdentifier = aggId.toString();
                externalizer.writeTo(aggId, da);
                da.writeUTFΔ(eventIdentifier);
                da.writeLong(timestamp);
                externalizer.writeTo(metadata, da);
                externalizer.writeTo(payload, da);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            da.finish();
        } else {
            aggregateIdentifier = event.getAggregateIdentifier().toString();
            final SerializedObject<String> serializedPayloadObject = serializePayload(event, serializer, String.class);
            final SerializedObject<String> serializedMetaDataObject = serializeMetaData(event, serializer, String.class);
            final String payload = serializedPayloadObject.getData();
            final String payloadType = serializedPayloadObject.getType().getName();
            final String payloadRevision = serializedPayloadObject.getType().getRevision();
            final String metadata = serializedMetaDataObject.getData();

            final int size = 8 //sequenceNumber
                    + len(type)
                    + len(aggregateIdentifier)
                    + len(eventIdentifier)
                    + 8 //timestamp
                    + len(payloadRevision)
                    + len(payloadType)
                    + len(metadata)
                    + len(payload);

            da.startExcerpt(size);
            da.writeLong(sequenceNumber);
            da.writeUTFΔ(type);
            da.writeUTFΔ(aggregateIdentifier);
            da.writeUTFΔ(eventIdentifier);
            da.writeLong(timestamp);
            da.writeUTFΔ(payloadRevision);
            da.writeUTFΔ(payloadType);
            da.writeUTFΔ(metadata);
            da.writeUTFΔ(payload);
            da.finish();
        }

        /**
         * The index stores the hash code of the aggregate + id to allow
         * quickly checking if the event probably matches the query.
         * Proper checking is then performed on the actual event for matches.
         */
        final int hc = hash(type, aggregateIdentifier);
        ia.startExcerpt(20); // 4 + 8 + 8
        ia.writeInt(hc);
        ia.writeLong(sequenceNumber);
        ia.writeLong(da.lastWrittenIndex());
        ia.finish();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void appendSnapshotEvent(final String type, final DomainEventMessage snapshotEvent) {
        _writeEvent(type, this.snapshotsAppender, this.snapshotsIndexAppender, externalizer, snapshotEvent);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DomainEventStream readEvents(final String type, final Object aggregateIdentifier) {
        try {
            return new ChronicleEventStream(type, aggregateIdentifier, -1, -1);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DomainEventStream readEvents(final String type, final Object aggregateIdentifier, final long firstSequenceNumber) {
        try {
            return new ChronicleEventStream(type, aggregateIdentifier, firstSequenceNumber, -1);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DomainEventStream readEvents(final String type, final Object aggregateIdentifier, final long firstSequenceNumber, final long lastSequenceNumber) {
        try {
            return new ChronicleEventStream(type, aggregateIdentifier, firstSequenceNumber, lastSequenceNumber);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visitEvents(final EventVisitor visitor) {
        final ChronicleEventStream stream;
        try {
            stream = new ChronicleEventStream(null, null, -1, -1);
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

    private class ChronicleEventStream implements DomainEventStream {
        final boolean _any;
        final int _hc;
        final String _type;
        final String _aggregateIdentifier;
        final long _firstSequenceNumber;
        final long _lastSequenceNumber;
        final ExcerptTailer _indexTailer;
        final ExcerptTailer _databaseTailer;
        boolean finished = false;

        Object aggregateIdentifier;
        final ChronicleDomainEvent event = new ChronicleDomainEvent();
        DomainEventMessage<?> message;
        int messageIndex = 0;
        List<DomainEventMessage> messages;

        private ChronicleEventStream(final String type, final Object aggregateIdentifier, final long firstSequenceNumber, final long lastSequenceNumber) throws IOException {
            this._indexTailer = ChronicleEventStore.this.eventsIndex.createTailer().toStart();
            this._databaseTailer = ChronicleEventStore.this.events.createTailer().toStart();
            this._firstSequenceNumber = firstSequenceNumber;
            this._lastSequenceNumber = lastSequenceNumber;
            if (type == null || aggregateIdentifier == null) {
                this._any = true;
                this._hc = -1;
                this._type = null;
                this._aggregateIdentifier = null;
            } else {
                this._any = false;
                this._type = type;
                this._aggregateIdentifier = aggregateIdentifier.toString();
                this._hc = hash(this._type, this._aggregateIdentifier);
            }
            _advanceMessages();
        }

        @Override
        public boolean hasNext() {
            return !finished;
        }

        /**
         * @param id null if matchAny is true
         * @param type The type to lok for
         * @param event The event to load non-binary events into
         * @param dt The tailer to read from
         * @param externalizer The externalized to use if set.
         * @param matchAny {@code true} if visiting every event
         * @return True if a matching event was found and this.aggregateIdentifier was set
         */
        private boolean _readEvent(final String id, final String type, final ChronicleDomainEvent event, final ExcerptTailer dt, final Externalizer externalizer, final boolean matchAny) {
            if (externalizer == null) {
                this.event.readMarshallable(dt); //TODO Should read type and aggId first and skip if wrong
                dt.finish();
                if (!matchAny && (!type.equals(event.type) || !id.equals(event.aggregateIdentifier))) {
                    return false;
                }
                this.aggregateIdentifier = event.aggregateIdentifier;
                return true;
            } else {
                final long sequenceNumber = dt.readLong();
                final String _type = dt.readUTFΔ();
                try {
                    final Object aggregateIdentifier = externalizer.readFrom(dt);
                    if (!matchAny && (!type.equals(_type) || !id.equals(aggregateIdentifier))) {
                        dt.finish();
                        return false;
                    }
                    final String eventIdentifier = dt.readUTFΔ();
                    final DateTime timestamp = new DateTime(dt.readLong());
                    final MetaData metaData = externalizer.readFrom(dt);
                    final Object payload = externalizer.readFrom(dt);
                    this.message = new GenericDomainEventMessage<Object>(
                            eventIdentifier,
                            timestamp,
                            aggregateIdentifier,
                            sequenceNumber,
                            payload,
                            metaData
                    );
                    dt.finish();
                    this.aggregateIdentifier = aggregateIdentifier;
                    return true;
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        private void _advanceEvents() {
            final ChronicleDomainEvent event = this.event;
            final String type = this._type;
            final String id = this._aggregateIdentifier;
            final int hc = this._hc;
            final ExcerptTailer dt = this._databaseTailer;
            final Externalizer externalizer = ChronicleEventStore.this.externalizer;
            if (this._any) {
                if (dt.nextIndex()) {
                    _readEvent(id, type, event, dt, externalizer, true);
                } else {
                    this.aggregateIdentifier = null;
                }
                return;
            }
            final long first = this._firstSequenceNumber;
            final long last = this._lastSequenceNumber;
            final ExcerptTailer it = this._indexTailer;
            while (it.nextIndex()) {
                final int theirHc = it.readInt();
                if (theirHc != hc) {
                    continue;
                }
                final long sn = it.readLong();
                if ((first != -1 && sn < first) || (last != -1 && sn > last)) {
                    it.finish();
                    continue;
                }
                // Found
                final long index = it.readLong();
                it.finish();

                dt.index(index);
                if (_readEvent(id, type, event, dt, externalizer, false)) {
                    return;
                }
            }
            this.aggregateIdentifier = null;
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
                _advanceEvents();
                if (this.aggregateIdentifier == null) {
                    this.finished = true;
                    return;
                }
                if (externalizer == null) {
                    this.messageIndex = 0;
                    this.messages = upcastAndDeserialize(this.event, this.aggregateIdentifier, serializer, upcasterChain, skipUnknown);
                }
            }
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
    }
}
