package com.gigaspaces.sql.datagateway.netty.utils;

import com.gigaspaces.jdbc.calcite.pg.PgTypeDescriptor;
import com.gigaspaces.sql.datagateway.netty.exception.ProtocolException;
import com.gigaspaces.sql.datagateway.netty.query.Session;
import io.netty.buffer.ByteBuf;

public class TypeInt4 extends PgType {
    public static final PgType INSTANCE = new TypeInt4();

    public TypeInt4() {
        super(PgTypeDescriptor.INT4);
    }

    @Override
    protected void asTextInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
        TypeUtils.checkType(value, Integer.class);
        TypeUtils.writeText(session, dst, value.toString());
    }

    @Override
    protected <T> T fromTextInternal(Session session, ByteBuf src) {
        return (T) Integer.valueOf(TypeUtils.readText(session, src));
    }

    @Override
    protected void asBinaryInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
        TypeUtils.checkType(value, Integer.class);
        dst.writeInt(4).writeInt((Integer) value);
    }

    @Override
    protected <T> T fromBinaryInternal(Session session, ByteBuf src) throws ProtocolException {
        TypeUtils.checkLen(src, 4);
        return (T) Integer.valueOf(src.readInt());
    }
}
