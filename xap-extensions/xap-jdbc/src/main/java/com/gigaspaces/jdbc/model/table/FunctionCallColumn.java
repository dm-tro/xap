package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.query.sql.functions.CastSqlFunction;
import com.gigaspaces.query.sql.functions.SqlFunction;
import com.gigaspaces.query.sql.functions.SqlFunctionExecutionContext;
import com.gigaspaces.query.sql.functions.extended.ExtractSqlFunction;
import com.gigaspaces.query.sql.functions.extended.LocalSession;
import com.j_spaces.jdbc.SQLFunctions;
import org.apache.calcite.avatica.util.TimeUnitRange;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.*;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.WeekFields;
import java.util.Calendar;
import java.util.List;
import java.util.stream.Collectors;

public class FunctionCallColumn implements IQueryColumn {
    protected final List<IQueryColumn> params;
    protected final String columnName;
    protected final String functionName;
    protected final String columnAlias;
    protected final boolean isVisible;
    protected final int columnOrdinal;
    private final LocalSession session;
    protected final String type;

    public FunctionCallColumn(LocalSession session, List<IQueryColumn> params, String functionName, String columnName, String columnAlias, boolean isVisible, int columnOrdinal) {
        this.params = params;
        this.columnName = columnName;
        this.functionName = functionName;
        this.columnAlias = columnAlias;
        this.isVisible = isVisible;
        this.columnOrdinal = columnOrdinal;
        this.session = session;
        type = null;
    }

    public FunctionCallColumn(LocalSession session, List<IQueryColumn> params, String functionName, String columnName, String columnAlias, boolean isVisible, int columnOrdinal, String type) {
        this.params = params;
        this.columnName = columnName;
        this.functionName = functionName;
        this.columnAlias = columnAlias;
        this.isVisible = isVisible;
        this.columnOrdinal = columnOrdinal;
        this.type = type;
        this.session = session;
    }

    @Override
    public int getColumnOrdinal() {
        return columnOrdinal;
    }

    @Override
    public String getName() {
        return functionName + "(" + String.join(", ", params.stream().map(IQueryColumn::getName).collect(Collectors.toList())) + ")";
    }

    @Override
    public String getAlias() {
        return columnAlias == null ? getName() : columnAlias;
    }

    @Override
    public boolean isVisible() {
        return isVisible;
    }

    @Override
    public boolean isUUID() {
        return false;
    }

    @Override
    public TableContainer getTableContainer() {
        throw new UnsupportedOperationException("Unsupported method getName");
    }

    @Override
    public Object getCurrentValue() {
        return getValue(null);
    }

    @Override
    public Class<?> getReturnType() {
        return Object.class;
    }

    @Override
    public IQueryColumn create(String columnName, String columnAlias, boolean isVisible, int columnOrdinal) {
        return null;
    }

    @Override
    public int compareTo(IQueryColumn o) {
        return 0;
    }

    public String getFunctionName() {
        return functionName;
    }

    @Override
    public Object getValue(IEntryPacket entryPacket) {
        SqlFunction sqlFunction = SQLFunctions.getBuiltInFunction(getFunctionName());
        if (sqlFunction != null) {
            if (sqlFunction instanceof ExtractSqlFunction) {
                return getValueFromExtract(entryPacket);
            }

            return sqlFunction.apply(new SqlFunctionExecutionContext() {
                @Override
                public int getNumberOfArguments() {
                    return params.size();
                }

                @Override
                public Object getArgument(int index) {
                    return params.get(index).getValue(entryPacket);
                }

                @Override
                public LocalSession getSession() {
                    return session;
                }

                @Override
                public String getType() {
                    return type;
                }
            });
        }
        throw new RuntimeException("Unknown function [" + getFunctionName() + "]");
    }

    private double getValueFromExtract(IEntryPacket entryPacket) {
        SqlFunctionExecutionContext extractContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return params.size();
            }

            @Override
            public Object getArgument(int index) {
                return params.get(index).getValue(entryPacket);
            }

            @Override
            public String getType() {
                return type;
            }
        };
        if (extractContext.getNumberOfArguments() != 2) {
            throw new RuntimeException("Extract function - Missing arguments");
        }
        Object symbolObject = extractContext.getArgument(0);
        Object date = extractContext.getArgument(1);
        if (!(symbolObject instanceof TimeUnitRange)) {
            throw new RuntimeException("Extract function - first argument isn't of type TimeUnitRange");
        }
        TimeUnitRange symbol = ((TimeUnitRange) symbolObject);

        if (date instanceof TemporalAccessor) {
            return extractLocalDateTime(((TemporalAccessor) date), symbol);
        } else if (date instanceof java.util.Date) {
            return extractDate(((java.util.Date) date), symbol);
        } else { // date instanceof String, requires conversion
            SqlFunctionExecutionContext castContext = new SqlFunctionExecutionContext() {
                @Override
                public int getNumberOfArguments() {
                    return 1;
                }

                @Override
                public Object getArgument(int index) {
                    if (index != 0) {
                        return null;
                    }
                    return date;
                }

                @Override
                public String getType() {
                    return type;
                }
            };

            TemporalAccessor castedDate = ((TemporalAccessor) new CastSqlFunction().apply(castContext)); // TemporalObject because cast can only cast date to TemporalObject
            return extractLocalDateTime(castedDate, symbol);
        }
    }

    @SuppressWarnings("IntegerDivisionInFloatingPointContext")
    private double extractLocalDateTime(TemporalAccessor temporalAccessor, TimeUnitRange timeunit) {
        LocalDateTime localDateTime;
        if (temporalAccessor instanceof LocalDateTime) {
            localDateTime = ((LocalDateTime) temporalAccessor);
        } else if (temporalAccessor instanceof LocalDate) {
            localDateTime = ((LocalDate) temporalAccessor).atStartOfDay();
        } else if (temporalAccessor instanceof LocalTime) {
            localDateTime = LocalDateTime.of(LocalDate.now(), ((LocalTime) temporalAccessor));
        } else if (temporalAccessor instanceof Instant) {
            localDateTime = LocalDateTime.ofInstant(((Instant) temporalAccessor), ZoneOffset.systemDefault());
        } else {
            throw new UnsupportedOperationException("Unsupported TemporalAccessor object: " + temporalAccessor.getClass());
        }
        
        switch (timeunit) {
            case SECOND:
                return localDateTime.getSecond();
            case MILLISECOND:
                return localDateTime.getSecond() * 1000D + BigDecimal.valueOf(localDateTime.getNano() / 1000000D).setScale(3, RoundingMode.HALF_EVEN).doubleValue();
            case MICROSECOND:
                return localDateTime.getSecond() * 1000000D + BigDecimal.valueOf(localDateTime.getNano() / 1000D).setScale(0, RoundingMode.HALF_EVEN).doubleValue();
            case MINUTE:
                return localDateTime.getMinute();
            case HOUR:
                return localDateTime.getHour();
            case DAY:
                return localDateTime.getDayOfMonth();
            case DOW:
                return localDateTime.getDayOfWeek().getValue() % 7;
            case DOY:
                return localDateTime.getDayOfYear();
            case MONTH:
                return localDateTime.getMonthValue();
            case MILLENNIUM:
                return ((localDateTime.getYear() - 1) / 1000) + 1;
            case QUARTER:
                int month = localDateTime.getMonthValue();
                return ((month - 1) / 3) + 1;
            case YEAR:
                return localDateTime.getYear();
            case WEEK:
                return localDateTime.get(WeekFields.ISO.weekOfWeekBasedYear());
            case CENTURY: {
                int year = localDateTime.getYear();
                return year % 100 == 0 ? (year / 100) : (year / 100) + 1;
            }
            case DECADE: {
                int year = localDateTime.getYear();
                return year / 10;
            }
            case EPOCH: {
                return localDateTime.atZone(ZoneOffset.UTC).toEpochSecond();
            }
            default:
                return 0;
        }
    }

    @SuppressWarnings("IntegerDivisionInFloatingPointContext")
    private double extractDate(java.util.Date date, TimeUnitRange timeunit) {
        Calendar calendar = Calendar.getInstance();
        calendar.clear();
        calendar.setTime(date);

        
        switch (timeunit) {
            case SECOND:
                return calendar.get(Calendar.SECOND);
            case MILLISECOND:
                return new BigDecimal(calendar.get(Calendar.MILLISECOND)).setScale(3, RoundingMode.HALF_EVEN).doubleValue();
            case MICROSECOND:
                return calendar.get(Calendar.MILLISECOND) * 1000;
            case MINUTE:
                return calendar.get(Calendar.MINUTE);
            case HOUR:
                return calendar.get(Calendar.HOUR_OF_DAY);
            case DAY:
                return calendar.get(Calendar.DAY_OF_MONTH);
            case DOW:
                return calendar.get(Calendar.DAY_OF_WEEK) - 1;
            case DOY:
                return calendar.get(Calendar.DAY_OF_YEAR);
            case MONTH:
                return calendar.get(Calendar.MONTH) + 1;
            case MILLENNIUM:
                return ((calendar.get(Calendar.YEAR) - 1) / 1000) + 1;
            case QUARTER:
                int month = calendar.get(Calendar.MONTH) + 1;
                return ((month - 1) / 3) + 1;
            case YEAR:
                return calendar.get(Calendar.YEAR);
            case WEEK:
                return calendar.get(Calendar.WEEK_OF_YEAR);
            case CENTURY: {
                int year = calendar.get(Calendar.YEAR);
                return year % 100 == 0 ? year / 100 : (year / 100) + 1;
            }
            case DECADE: {
                int year = calendar.get(Calendar.YEAR);
                return year / 10;
            }
            case EPOCH:
                return calendar.getTimeInMillis() / 1000;
            default:
                return 0;
        }
    }
}
