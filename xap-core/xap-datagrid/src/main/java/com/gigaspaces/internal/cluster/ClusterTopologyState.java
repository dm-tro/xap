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
package com.gigaspaces.internal.cluster;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;

public class ClusterTopologyState implements Externalizable {
    private static final long serialVersionUID = 1L;

    private Map <Integer,Integer> partitionGenerationMap;
    private int currentGeneration;

    public ClusterTopologyState() {
    }

    public ClusterTopologyState(int generation, int numOfPartitions) {
        this.currentGeneration = generation;
        partitionGenerationMap = new HashMap<>();
        addGeneration(generation, numOfPartitions,1);
    }

    public ClusterTopologyState(ClusterTopologyState other) {
        this.partitionGenerationMap = new HashMap<>(other.partitionGenerationMap);
        this.currentGeneration = other.currentGeneration;
    }

    public void addGeneration(int generation, int numOfPartitions, int basePartitionToUpdate) {
        updatePartitionsGeneration(basePartitionToUpdate, numOfPartitions, generation);
    }

    public void updatePartitionsGeneration(int from, int to, int generation) {
        for (int partitionId = from ; partitionId <= to; partitionId++) {
            partitionGenerationMap.put(partitionId, generation);
        }
    }

    public int getCurrentGeneration() {
        return currentGeneration;
    }

    public int getGenerationForPartition(int partitionId) {
        return this.partitionGenerationMap.get(partitionId);
    }

    public void deletePartitionGeneration(int partitionId){
        this.partitionGenerationMap.remove(partitionId);
    }

    public void updateGeneration(int currentGeneration){
        this.currentGeneration = currentGeneration;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeShort(currentGeneration);
        out.writeShort(partitionGenerationMap.size());
        for (Map.Entry<Integer, Integer> entry : partitionGenerationMap.entrySet()) {
            out.writeShort(entry.getKey());
            out.writeShort(entry.getValue());
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.currentGeneration = in.readShort();
        int size = in.readShort();
        partitionGenerationMap = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            partitionGenerationMap.put((int) in.readShort(), (int)in.readShort());
        }
    }

    @Override
    public String toString() {
        return "ClusterTopologyState{" +
                "partitionGenerationMap=" + partitionGenerationMap +
                ", currentGeneration=" + currentGeneration +
                '}';
    }
}
