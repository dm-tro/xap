package com.gigaspaces.internal.server.space.repartitioning;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.j_spaces.core.client.Modifiers;
import net.jini.core.lease.Lease;
import net.jini.core.transaction.TransactionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class CopyChunksConsumer implements Runnable {

    public static Logger logger = LoggerFactory.getLogger("org.openspaces.admin.internal.pu.scale_horizontal.ScaleManager");

    private Map<Integer, ISpaceProxy> proxyMap;
    private BlockingQueue<Batch> batchQueue;
    private CopyChunksResponseInfo responseInfo;

    CopyChunksConsumer(Map<Integer, ISpaceProxy> proxyMap, BlockingQueue<Batch> batchQueue, CopyChunksResponseInfo responseInfo) {
        this.proxyMap = proxyMap;
        this.batchQueue = batchQueue;
        this.responseInfo = responseInfo;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            WriteBatch writeBatch = null;
            try {
                Batch batch = batchQueue.poll(5, TimeUnit.SECONDS);
                if (batch == Batch.EMPTY_BATCH) {
                    return;
                }
                if (batch != null) {
                    writeBatch = ((WriteBatch) batch);
                    ISpaceProxy spaceProxy = proxyMap.get(writeBatch.getPartitionId());
                    spaceProxy.writeMultiple(writeBatch.getEntries().toArray(), null, Lease.FOREVER, Modifiers.BACKUP_ONLY);
                    responseInfo.getMovedToPartition().get((short) writeBatch.getPartitionId()).addAndGet(writeBatch.getEntries().size());

                }
            } catch (InterruptedException e) {
                responseInfo.setException(new IOException("Copy chunks consumer thread was interrupted", e));
                Thread.currentThread().interrupt();
                return;
            } catch (RemoteException | TransactionException e) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Consumer thread caught exception");
                    e.printStackTrace();
                }
                responseInfo.setException(new IOException("Caught exception while trying to write to partition " +
                        writeBatch.getPartitionId(), e));
                return;
            }
        }
    }
}