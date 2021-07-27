package com.gigaspaces.sql.datagateway.netty.query;

import com.gigaspaces.sql.datagateway.netty.utils.PgType;

class TypeAware {
    protected final PgType type;

    protected TypeAware(PgType type) {
        this.type = type;
    }

    public int getTypeId() {
        return type.getId();
    }

    public PgType getType() {
        return type;
    }
}
