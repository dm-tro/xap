package com.gigaspaces.sql.datagateway.netty.utils;

import com.gigaspaces.jdbc.calcite.pg.PgTypeDescriptor;
import com.gigaspaces.sql.datagateway.netty.exception.NonBreakingException;
import com.gigaspaces.sql.datagateway.netty.exception.ProtocolException;
import com.gigaspaces.sql.datagateway.netty.query.Session;
import io.netty.buffer.ByteBuf;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

@SuppressWarnings("unchecked")
public class TypeNumeric extends PgType {
    public static final PgType INSTANCE = new TypeNumeric();

    private static final BigInteger BI_TEN_THOUSAND = BigInteger.valueOf(10000);
    private static final BigInteger[] BI_TEN_POWERS = new BigInteger[32];
    private static final BigInteger BI_MAX_LONG = BigInteger.valueOf(Long.MAX_VALUE);
    private static final int[] INT_TEN_POWERS = new int[6];
    private static final short SIGN_POS = 0x0000;
    private static final short SIGN_NEG = 0x4000;
    private static final short SIGN_NAN = (short) 0xC000;
    private static final int SCALE_MASK = 0x00003FFF;

    static {
        for (int i = 0; i < INT_TEN_POWERS.length; ++i) {
            INT_TEN_POWERS[i] = (int) Math.pow(10, i);
        }
        for (int i = 0; i < BI_TEN_POWERS.length; ++i) {
            BI_TEN_POWERS[i] = BigInteger.TEN.pow(i);
        }
    }

    private static final class ShortsStack {
        private short[] array = new short[8];
        private int size = 0;

        public void push(short s) {
            assert s >= 0;

            if (size == array.length)
                array = Arrays.copyOf(array, array.length << 1);

            array[size++] = s;
        }

        public short pop() {
            return size > 0 ? array[--size] : -1;
        }

        public int size() {
            return size;
        }

        public boolean isEmpty() {
            return size == 0;
        }
    }

    public TypeNumeric() {
        super(PgTypeDescriptor.NUMERIC);
    }

    @Override
    protected void asTextInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
        TypeUtils.writeText(session, dst, value.toString());
    }

    @Override
    protected <T> T fromTextInternal(Session session, ByteBuf src) throws ProtocolException {
        return (T) new BigDecimal(TypeUtils.readText(session, src).trim());
    }

    @Override
    protected void asBinaryInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
        TypeUtils.checkType(value, BigDecimal.class);

        dst.writeInt(0); // placeholder for data length
        int offset = dst.writerIndex();

        BigDecimal value0 = (BigDecimal) value;
        BigInteger unscaled = value0.unscaledValue().abs();
        int scale = value0.scale();
        if (unscaled.equals(BigInteger.ZERO)) {
            // header only
            dst.writeShort(0) // length
                    .writeShort(-1) // wight
                    .writeShort(SIGN_POS) // sign
                    .writeShort(Math.max(0, scale)); // scale
        } else {
            ShortsStack shorts = new ShortsStack();
            int weight = -1;
            if (scale <= 0) {
                //this means we have an integer
                //adjust unscaled and weight
                if (scale < 0) {
                    scale = Math.abs(scale);
                    //weight value covers 4 digits
                    weight += scale / 4;
                    //whatever remains needs to be incorporated to the unscaled value
                    int mod = scale % 4;
                    unscaled = unscaled.multiply(tenPowerBI(mod));
                    scale = 0;
                }

                while (unscaled.compareTo(BI_MAX_LONG) > 0) {
                    final BigInteger[] pair = unscaled.divideAndRemainder(BI_TEN_THOUSAND);
                    unscaled = pair[0];
                    final short shortValue = pair[1].shortValue();
                    if (shortValue != 0 || !shorts.isEmpty()) {
                        shorts.push(shortValue);
                    }
                    ++weight;
                }
                long unscaledLong = unscaled.longValueExact();
                do {
                    final short shortValue = (short) (unscaledLong % 10000);
                    if (shortValue != 0 || !shorts.isEmpty()) {
                        shorts.push(shortValue);
                    }
                    unscaledLong = unscaledLong / 10000L;
                    ++weight;
                } while (unscaledLong != 0);
            } else {
                final BigInteger[] split = unscaled.divideAndRemainder(tenPowerBI(scale));
                BigInteger decimal = split[1];
                BigInteger wholes = split[0];
                weight = -1;
                if (!BigInteger.ZERO.equals(decimal)) {
                    int mod = scale % 4;
                    int segments = scale / 4;
                    if (mod != 0) {
                        decimal = decimal.multiply(tenPowerBI(4 - mod));
                        ++segments;
                    }
                    do {
                        final BigInteger[] pair = decimal.divideAndRemainder(BI_TEN_THOUSAND);
                        decimal = pair[0];
                        final short shortValue = pair[1].shortValue();
                        if (shortValue != 0 || !shorts.isEmpty()) {
                            shorts.push(shortValue);
                        }
                        --segments;
                    } while (!BigInteger.ZERO.equals(decimal));

                    //for the leading 0 shorts we either adjust weight (if no wholes)
                    // or push shorts
                    if (BigInteger.ZERO.equals(wholes)) {
                        weight -= segments;
                    } else {
                        //now add leading 0 shorts
                        for (int i = 0; i < segments; ++i) {
                            shorts.push((short) 0);
                        }
                    }
                }

                while (!BigInteger.ZERO.equals(wholes)) {
                    ++weight;
                    final BigInteger[] pair = wholes.divideAndRemainder(BI_TEN_THOUSAND);
                    wholes = pair[0];
                    final short shortValue = pair[1].shortValue();
                    if (shortValue != 0 || !shorts.isEmpty()) {
                        shorts.push(shortValue);
                    }
                }
            }

            //number of 2-byte shorts representing 4 decimal digits
            dst.writeShort(shorts.size());
            //0 based number of 4 decimal digits (i.e. 2-byte shorts) before the decimal
            dst.writeShort(weight);
            //indicates positive, negative or NaN
            dst.writeShort(value0.signum() == -1 ? SIGN_NEG : SIGN_POS);
            //number of digits after the decimal
            dst.writeShort(scale);
            //shorts
            while (!shorts.isEmpty()) {
                dst.writeShort(shorts.pop());
            }
        }

        dst.setInt(offset - 4, dst.writerIndex() - offset);
    }

    @Override
    protected <T> T fromBinaryInternal(Session session, ByteBuf src) throws ProtocolException {
        int numBytes = src.readInt();
        if (numBytes < 8) {
            throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "number of bytes should be at-least 8");
        }

        //number of 2-byte shorts representing 4 decimal digits
        short len = src.readShort();
        //0 based number of 4 decimal digits (i.e. 2-byte shorts) before the decimal
        //a value <= 0 indicates an absolute value < 1.
        short weight = src.readShort();
        //indicates positive, negative or NaN
        short sign = src.readShort();
        //number of digits after the decimal. This must be >= 0.
        //a value of 0 indicates a whole number (integer).
        short scale = src.readShort();

        //An integer should be built from the len number of 2 byte shorts, treating each
        //as 4 digits.
        //The weight, if > 0, indicates how many of those 4 digit chunks should be to the
        //"left" of the decimal. If the weight is 0, then all 4 digit chunks start immediately
        //to the "right" of the decimal. If the weight is < 0, the absolute distance from 0
        //indicates 4 leading "0" digits to the immediate "right" of the decimal, prior to the
        //digits from "len".
        //A weight which is positive, can be a number larger than what len defines. This means
        //there are trailing 0s after the "len" integer and before the decimal.
        //The scale indicates how many significant digits there are to the right of the decimal.
        //A value of 0 indicates a whole number (integer).
        //The combination of weight, len, and scale can result in either trimming digits provided
        //by len (only to the right of the decimal) or adding significant 0 values to the right
        //of len (on either side of the decimal).

        if (numBytes != (len * 2 + 8)) {
            throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "invalid length of bytes \"numeric\" value");
        }
        
        switch (sign) {
            case SIGN_NAN:
                throw new NonBreakingException(ErrorCodes.UNSUPPORTED_FEATURE, "NaN values are unsupported at the moment");
            case SIGN_NEG:
            case SIGN_POS:
                break;
            default:
                throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "invalid sign in \"numeric\" value");
        }

        if ((scale & SCALE_MASK) != scale) {
            throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "invalid scale in \"numeric\" value");
        }

        if (len == 0) {
            return (T) new BigDecimal(BigInteger.ZERO, scale);
        }
        
        short d = src.readShort();

        //if the absolute value is (0, 1), then leading '0' values
        //do not matter for the unscaledInt, but trailing 0s do
        if (weight < 0) {
            assert scale > 0;
            int effectiveScale = scale;
            ++weight;
            if (weight < 0) {
                effectiveScale += (4 * weight);
            }

            int i = 1;
            //typically there should not be leading 0 short values, as it is more
            //efficient to represent that in the weight value
            for ( ; i < len && d == 0; ++i) {
                effectiveScale -= 4;
                d = src.readShort();
            }

            BigInteger unscaledBI = null;
            assert effectiveScale > 0;
            if (effectiveScale >= 4) {
                effectiveScale -= 4;
            } else {
                d = (short) (d / tenPowerInt(4 - effectiveScale));
                effectiveScale = 0;
            }
            long unscaledInt = d;
            for ( ; i < len; ++i) {
                if (i == 4 && effectiveScale > 2) {
                    unscaledBI = BigInteger.valueOf(unscaledInt);
                }
                d = src.readShort();
                if (effectiveScale >= 4) {
                    if (unscaledBI == null) {
                        unscaledInt *= 10000;
                    } else {
                        unscaledBI = unscaledBI.multiply(BI_TEN_THOUSAND);
                    }
                    effectiveScale -= 4;
                } else {
                    if (unscaledBI == null) {
                        unscaledInt *= tenPowerInt(effectiveScale);
                    } else {
                        unscaledBI = unscaledBI.multiply(tenPowerBI(effectiveScale));
                    }
                    d = (short) (d / tenPowerInt(4 - effectiveScale));
                    effectiveScale = 0;
                }
                if (unscaledBI == null) {
                    unscaledInt += d;
                } else {
                    if (d != 0) {
                        unscaledBI = unscaledBI.add(BigInteger.valueOf(d));
                    }
                }
            }
            if (unscaledBI == null) {
                unscaledBI = BigInteger.valueOf(unscaledInt);
            }
            if (effectiveScale > 0) {
                unscaledBI = unscaledBI.multiply(tenPowerBI(effectiveScale));
            }
            if (sign == SIGN_NEG) {
                unscaledBI = unscaledBI.negate();
            }

            return (T) new BigDecimal(unscaledBI, scale);
        }

        //if there is no scale, then shorts are the unscaled int
        if (scale == 0) {
            BigInteger unscaledBI = null;
            long unscaledInt = d;
            for (int i = 1; i < len; ++i) {
                if (i == 4) {
                    unscaledBI = BigInteger.valueOf(unscaledInt);
                }
                d = src.readShort();
                if (unscaledBI == null) {
                    unscaledInt *= 10000;
                    unscaledInt += d;
                } else {
                    unscaledBI = unscaledBI.multiply(BI_TEN_THOUSAND);
                    if (d != 0) {
                        unscaledBI = unscaledBI.add(BigInteger.valueOf(d));
                    }
                }
            }
            if (unscaledBI == null) {
                unscaledBI = BigInteger.valueOf(unscaledInt);
            }
            if (sign == SIGN_NEG) {
                unscaledBI = unscaledBI.negate();
            }
            final int bigDecScale = (len - (weight + 1)) * 4;
            //string representation always results in a BigDecimal with scale of 0
            //the binary representation, where weight and len can infer trailing 0s, can result in a negative scale
            //to produce a consistent BigDecimal, we return the equivalent object with scale set to 0
            return (T) (bigDecScale == 0 ? new BigDecimal(unscaledBI)
                                         : new BigDecimal(unscaledBI, bigDecScale).setScale(0));
        }

        BigInteger unscaledBI = null;
        long unscaledInt = d;
        int effectiveWeight = weight;
        int effectiveScale = scale;
        for (int i = 1 ; i < len; ++i) {
            if (i == 4) {
                unscaledBI = BigInteger.valueOf(unscaledInt);
            }
            d = src.readShort();
            if (effectiveWeight > 0) {
                --effectiveWeight;
                if (unscaledBI == null) {
                    unscaledInt *= 10000;
                } else {
                    unscaledBI = unscaledBI.multiply(BI_TEN_THOUSAND);
                }
            } else if (effectiveScale >= 4) {
                effectiveScale -= 4;
                if (unscaledBI == null) {
                    unscaledInt *= 10000;
                } else {
                    unscaledBI = unscaledBI.multiply(BI_TEN_THOUSAND);
                }
            } else {
                if (unscaledBI == null) {
                    unscaledInt *= tenPowerInt(effectiveScale);
                } else {
                    unscaledBI = unscaledBI.multiply(tenPowerBI(effectiveScale));
                }
                d = (short) (d / tenPowerInt(4 - effectiveScale));
                effectiveScale = 0;
            }
            if (unscaledBI == null) {
                unscaledInt += d;
            } else {
                if (d != 0) {
                    unscaledBI = unscaledBI.add(BigInteger.valueOf(d));
                }
            }
        }

        if (unscaledBI == null) {
            unscaledBI = BigInteger.valueOf(unscaledInt);
        }
        if (effectiveScale > 0) {
            unscaledBI = unscaledBI.multiply(tenPowerBI(effectiveScale));
        }
        if (sign == SIGN_NEG) {
            unscaledBI = unscaledBI.negate();
        }
        return (T) new BigDecimal(unscaledBI, scale);
    }

    private static BigInteger tenPowerBI(int exponent) {
        return BI_TEN_POWERS.length > exponent ? BI_TEN_POWERS[exponent] : BigInteger.TEN.pow(exponent);
    }

    private int tenPowerInt(int exponent) {
        return INT_TEN_POWERS[exponent];
    }
}
