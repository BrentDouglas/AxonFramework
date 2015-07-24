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

import java.io.ByteArrayInputStream;

/**
 * A {@link ByteArrayInputStream} that allows switching the underlying array.
 *
 * @author <a href="mailto:brent.n.douglas@gmail.com">Brent Douglas</a>
 * @since 2.5
 */
public class ReusableByteArrayInputStream extends ByteArrayInputStream {

    /**
     * {@inheritDoc}
     */
    public ReusableByteArrayInputStream(final byte[] buf) {
        super(buf);
    }

    /**
     * {@inheritDoc}
     */
    public ReusableByteArrayInputStream(final byte[] buf, final int offset, final int length) {
        super(buf, offset, length);
    }

    /**
     * Set the underlying array to the supplied input buffer.
     *
     * @param buf The input buffer.
     */
    public void setBuf(final byte[] buf) {
        this.buf = buf;
        this.pos = 0;
        this.mark = 0;
        this.count = buf.length;
    }

    /**
     * Set the underlying array to a specified slice of the supplied input buffer.
     *
     * @param buf The input buffer.
     * @param offset The offset in the buffer of the first byte to read.
     * @param length The maximum number of bytes to read from the buffer.
     */
    public void setBuf(final byte[] buf, final int offset, final int length) {
        this.buf = buf;
        this.pos = offset;
        this.mark = offset;
        this.count = Math.min(offset + length, buf.length);
    }
}
