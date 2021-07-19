/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gigaspaces.internal.serialization.primitives;

import com.gigaspaces.internal.serialization.IClassSerializer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class LongPrimitiveClassSerializer implements IClassSerializer<Long> {
    private static final Long DEFAULT_VALUE = 0L;

    public static final LongPrimitiveClassSerializer instance = new LongPrimitiveClassSerializer();

    private LongPrimitiveClassSerializer() {
    }

    public byte getCode() {
        return CODE_LONG;
    }

    public Long read(ObjectInput in)
            throws IOException, ClassNotFoundException {
        return in.readLong();
    }

    public void write(ObjectOutput out, Long obj)
            throws IOException {
        out.writeLong(obj.longValue());
    }

    @Override
    public Long getDefaultValue() {
        return DEFAULT_VALUE;
    }
}
