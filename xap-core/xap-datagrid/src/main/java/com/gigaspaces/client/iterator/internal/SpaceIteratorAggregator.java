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

package com.gigaspaces.client.iterator.internal;

import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.query.aggregators.SpaceEntriesAggregatorContext;

import java.io.Externalizable;
import java.util.ArrayList;

/**
 * @author Niv Ingberg
 * @since 10.1
 */
@com.gigaspaces.api.InternalApi
public class SpaceIteratorAggregator extends AbstractSpaceIteratorAggregator
        implements Externalizable {

    private static final long serialVersionUID = 2L;
    private transient ISpaceIteratorResult finalResult;
    private transient SpaceIteratorAggregatorPartitionResult result;

    @Override
    public String getDefaultAlias() {
        return null;
    }

    @Override
    public void aggregate(SpaceEntriesAggregatorContext context) {
        if (result == null)
            result = new SpaceIteratorAggregatorPartitionResult(context.getPartitionId());
        if (result.getEntries().size() < getBatchSize())
            result.getEntries().add((IEntryPacket) context.getRawEntry());
        else {
            if (result.getUids() == null)
                result.setUids(new ArrayList<String>());
            result.getUids().add(context.getEntryUid());
        }
    }

    @Override
    public ISpaceIteratorAggregatorPartitionResult getIntermediateResult() {
        return result;
    }

    @Override
    public void aggregateIntermediateResult(ISpaceIteratorAggregatorPartitionResult partitionResult) {
        if (finalResult == null)
            finalResult = new SpaceIteratorResult();
        finalResult.addPartition(partitionResult);
    }

    @Override
    public Object getFinalResult() {
        if (result != null)
            aggregateIntermediateResult(result);
        return finalResult != null ? finalResult : new SpaceIteratorResult();
    }
}
