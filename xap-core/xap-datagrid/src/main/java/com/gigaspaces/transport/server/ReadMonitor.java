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
package com.gigaspaces.transport.server;

import com.gigaspaces.internal.remoting.RemoteOperationRequest;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.lrmi.LRMIRuntime;
import com.gigaspaces.transport.NioChannel;
import com.gigaspaces.transport.PocSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

public class ReadMonitor extends SelectMonitor {
    private final SpaceImpl space;
    private final Executor executorService = PocSettings.serverLrmiExecutor ? LRMIRuntime.getRuntime().getThreadPool() : null;

    public ReadMonitor(NioServer server, int id) throws IOException {
        super("read-" + id);
        this.space = server.getSpace();
    }

    @Override
    protected void onReady(SelectionKey key) throws IOException {
        NioChannel nioChannel = (NioChannel) key.attachment();
        ByteBuffer requestBuffer = nioChannel.readNonBlocking();
        if (requestBuffer != null) {
            SpaceOperationTask task = new SpaceOperationTask(requestBuffer, space, nioChannel);
            if (executorService != null)
                executorService.execute(task);
            else
                task.run();
        }
    }

    private static class SpaceOperationTask implements Runnable {
        private static final Logger logger = LoggerFactory.getLogger(SpaceOperationTask.class);
        private final ByteBuffer requestBuffer;
        private final SpaceImpl space;
        private final NioChannel channel;

        private SpaceOperationTask(ByteBuffer buffer, SpaceImpl space, NioChannel channel) {
            this.requestBuffer = buffer;
            this.space = space;
            this.channel = channel;
        }

        @Override
        public void run() {
            Object response;
            try {
                RemoteOperationRequest<?> request = (RemoteOperationRequest<?>) channel.deserialize(requestBuffer);
                response = space.executeOperation(request);
            } catch (Exception e) {
                response = e;
            }
            try {
                ByteBuffer responseBuffer = channel.serialize(response);
                channel.writeBlocking(responseBuffer);
            } catch (IOException e) {
                logger.error("Failed to write result to channel", e);
            }
        }
    }
}
