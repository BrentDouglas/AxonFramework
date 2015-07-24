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

import java.io.DataOutputStream;
import java.io.OutputStream;

/**
 * A {@link DataOutputStream} that allows reseting the written counter.
 *
 * @author <a href="mailto:brent.n.douglas@gmail.com">Brent Douglas</a>
 * @since 2.5
 */
public class ReusableDataOutputStream extends DataOutputStream {

    /**
     * {@inheritDoc}
     */
    public ReusableDataOutputStream(final OutputStream out) {
        super(out);
    }

    /**
     * Reset the bytes written counter.
     */
    public void reset() {
        this.written = 0;
    }
}
