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
package com.gigaspaces.internal.serialization.compressed;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.io.PooledObjectConverter;
import com.gigaspaces.internal.serialization.IClassSerializer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Niv Ingberg
 * @since 15.8
 */
@com.gigaspaces.api.InternalApi
public class DefaultCompressedClassSerializer implements IClassSerializer<Object> {
    public static final DefaultCompressedClassSerializer instance = new DefaultCompressedClassSerializer();

    @Override
    public byte getCode() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(ObjectOutput out, Object obj) throws IOException {
        byte[] bytes = PooledObjectConverter.zip(obj);
        IOUtils.writeInt(out, bytes.length);
        out.write(bytes);
    }

    @Override
    public Object read(ObjectInput in) throws IOException, ClassNotFoundException {
        int length = IOUtils.readInt(in);
        byte[] bytes = new byte[length];
        in.read(bytes);
        return PooledObjectConverter.unzip(bytes);
    }
}