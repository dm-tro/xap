package com.gigaspaces.internal.server.space.iterator;

import com.gigaspaces.logger.Constants;
import com.j_spaces.core.GetBatchForIteratorException;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ServerIteratorsManager {
    private final Logger _logger;
    private final Map<UUID, ServerIteratorInfo> serverIteratorInfoMap = new ConcurrentHashMap<>();

    public ServerIteratorsManager() {
        _logger = Logger.getLogger(Constants.LOGGER_SERVER_GSITERATOR);
    }

    public ServerIteratorInfo getOrCreateServerIteratorInfo(ServerIteratorRequestInfo serverIteratorRequestInfo) throws GetBatchForIteratorException {
        UUID uuid = serverIteratorRequestInfo.getUuid();
        boolean containsUuid = serverIteratorInfoMap.containsKey(uuid);
        boolean firstTime = serverIteratorRequestInfo.isFirstTime();
        boolean createNew = !containsUuid && firstTime;
        boolean foundActive = containsUuid && !firstTime;
        if(createNew){
            ServerIteratorInfo result = new ServerIteratorInfo(serverIteratorRequestInfo.getUuid(), serverIteratorRequestInfo.getBatchSize(), serverIteratorRequestInfo.getMaxInactiveDuration());
            serverIteratorInfoMap.put(uuid, result);
            if(_logger.isLoggable(Level.FINE))
                _logger.fine("Space iterator " + uuid + " was created in server");
            return result;
        }
        if(foundActive) {
            ServerIteratorInfo serverIteratorInfo = tryRenewServerIteratorLease(uuid);
            if(serverIteratorInfo != null)
                return serverIteratorInfo;
            throw new GetBatchForIteratorException("Space iterator " + uuid + " was not found in space");
        }
        if(containsUuid && firstTime) {
            throw new GetBatchForIteratorException("Space iterator " + uuid + " was already created in space");
        }
        throw new GetBatchForIteratorException("Space iterator " + uuid + " was not found in space");
    }

    public void closeServerIterator(UUID uuid){
        ServerIteratorInfo serverIteratorInfo = serverIteratorInfoMap.get(uuid);
        if(serverIteratorInfo != null){
            if(serverIteratorInfo.tryDeactivateIterator()){
                if (_logger.isLoggable(Level.FINE))
                    _logger.fine("Space iterator " + uuid + " was closed in server");
                serverIteratorInfoMap.remove(uuid, serverIteratorInfo);
            }
        }
    }

    public ServerIteratorInfo tryRenewServerIteratorLease(UUID uuid){
        ServerIteratorInfo serverIteratorInfo = serverIteratorInfoMap.get(uuid);
        if(serverIteratorInfo == null)
            return null;
        if(serverIteratorInfo.tryRenewLease())
            return serverIteratorInfo;
        return null;
    }

    public Map<UUID, ServerIteratorInfo> getServerIteratorInfoMap() {
        return serverIteratorInfoMap;
    }

    public int purgeExpiredIterators() {
        int reapCount = 0;
        for(Map.Entry<UUID, ServerIteratorInfo> entry: serverIteratorInfoMap.entrySet()){
            ServerIteratorInfo serverIteratorInfo = entry.getValue();
            if(serverIteratorInfo.tryExpireIterator()) {
                if (_logger.isLoggable(Level.FINE))
                    _logger.fine("Space iterator " + serverIteratorInfo.getUuid() + " was inactive for more than " + TimeUnit.MILLISECONDS.toSeconds(serverIteratorInfo.getMaxInactiveDuration()) + " seconds, expiring it.");
                if (serverIteratorInfoMap.remove(entry.getKey(), entry.getValue())) {
                    reapCount++;
                }
            }
        }
        return reapCount;
    }
}
