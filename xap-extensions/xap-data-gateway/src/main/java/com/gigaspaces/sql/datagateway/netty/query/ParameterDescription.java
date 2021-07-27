package com.gigaspaces.sql.datagateway.netty.query;

import com.gigaspaces.sql.datagateway.netty.exception.ProtocolException;
import com.gigaspaces.sql.datagateway.netty.utils.PgType;
import com.gigaspaces.sql.datagateway.netty.utils.TypeUtils;
import io.netty.buffer.ByteBuf;

public class ParameterDescription extends TypeAware {
    public ParameterDescription(PgType type) {
        super(type);
    }

    public <T> T read(Session session, ByteBuf buf, int format) throws ProtocolException {
        return TypeUtils.readParameter(session, buf, this, format);
    }
}
