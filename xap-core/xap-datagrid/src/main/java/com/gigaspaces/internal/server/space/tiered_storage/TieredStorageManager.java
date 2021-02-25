package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.j_spaces.core.cache.context.TieredState;

public interface TieredStorageManager {

    CachePredicate getCacheRule(String typeName); // get cache rule for a specific type

    TimePredicate getRetentionRule(String typeName); // get retention rule for a specific type

    void setCacheRule(String typeName, CachePredicate newRule); // dynamically change rule

    InternalRDBMS getInternalStorage();

    TieredState getEntryTieredState(IEntryData entryData);

    TieredState guessEntryTieredState(String typeName);

    TieredState guessTemplateTier(ITemplateHolder templateHolder);

    // For the future when we would want to support warm layer
    //    CachePredicate getCacheRule(String typeName, String tier);
    //    Map<String,CachePredicate> getCacheRulesForTiers(String typeName);
}
