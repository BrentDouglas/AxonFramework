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
package org.axonframework.serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;

/**
 * Used to manually manage serialization and deserialization of values without the assistance
 * of Axon's regular serialization process.
 *
 * Values managed by an Externalizer are not applicable for use with
 * {@link org.axonframework.upcasting.Upcaster}'s and must manually manage
 * field versions. In practical terms, consider using this with serialization frameworks
 * such as Protocol Buffers, Thrift, etc.
 *
 * @author <a href="mailto:brent.n.douglas@gmail.com">Brent Douglas</a>
 * @since 2.5
 */
public interface Externalizer {

    /**
     * @return The number of bytes that will be written to the stream when externalizing the value.
     * @throws IllegalArgumentException If the value is not externalizable by this instance.
     */
    int size(final Object val) throws IllegalArgumentException;

    /**
     * Write the bytes representing this value to the stream.
     *
     * @param out The stream to write to.
     * @throws IOException If there is a error writing to the stream.
     * @throws IllegalArgumentException If the value is not externalizable by this instance.
     */
    void writeTo(final Object val, final OutputStream out) throws IOException, IllegalArgumentException;

    /**
     * Write the bytes representing this value to the output.
     *
     * @param out The output to write to.
     * @throws IOException If there is a error writing to the stream.
     * @throws IllegalArgumentException If the value is not externalizable by this instance.
     */
    void writeTo(final Object val, final DataOutput out) throws IOException, IllegalArgumentException;

    /**
     * Read a value from the stream.
     *
     * @param in The stream to read from.
     * @param <T> The type of object that will be returned.
     * @return An object from the stream.
     * @throws IOException If a value can not be read from the stream.
     */
    <T> T readFrom(final InputStream in) throws IOException;

    /**
     * Read a value from the input.
     *
     * @param in The input to read from.
     * @param <T> The type of object that will be returned.
     * @return An object from the input.
     * @throws IOException If a value can not be read from the input.
     */
    <T> T readFrom(final DataInput in) throws IOException;
}
