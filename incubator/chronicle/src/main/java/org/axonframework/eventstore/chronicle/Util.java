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

/**
 * @author <a href="mailto:brent.n.douglas@gmail.com">Brent Douglas</a>
 * @since 2.5
 */
public class Util {

    public static int hash(final String a, final String b) {
        int ret = 1;
        ret = 31 * ret + (a == null ? 0 : a.hashCode());
        ret = 31 * ret + (b == null ? 0 : b.hashCode());
        return ret;
    }

    public static int len(final String s) {
        return s == null
                ? 3
                : 3 + (3 * s.length());
    }

    public static int len(final byte[] s) {
        return s == null
                ? 4
                : 4 + s.length;
    }
}