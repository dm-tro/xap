package com.gigaspaces.jdbc.model.result;

import com.gigaspaces.internal.utils.math.MutableNumber;
import com.gigaspaces.jdbc.model.table.AggregationColumn;
import com.gigaspaces.jdbc.model.table.AggregationFunctionType;
import com.gigaspaces.jdbc.model.table.IQueryColumn;
import com.gigaspaces.jdbc.model.table.OrderColumn;
import com.gigaspaces.metadata.StorageType;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class TableRowUtils {

    public static TableRow aggregate(List<TableRow> tableRows, List<IQueryColumn> selectedColumns,
                                     List<AggregationColumn> aggregationColumns, List<IQueryColumn> visibleColumns) {
        if (tableRows.isEmpty()) {
            return new TableRow((IQueryColumn[]) null, null);
        }
        IQueryColumn[] rowsColumns = selectedColumns.toArray(new IQueryColumn[0]);
        OrderColumn[] firstRowOrderColumns = tableRows.get(0).getOrderColumns();
        Object[] firstRowOrderValues = tableRows.get(0).getOrderValues();
        IQueryColumn[] firstRowGroupByColumns = tableRows.get(0).getGroupByColumns();
        Object[] firstRowGroupByValues = tableRows.get(0).getGroupByValues();
        Object[] values = new Object[rowsColumns.length];
        for (IQueryColumn visibleColumn : visibleColumns) {
            values[visibleColumn.getColumnOrdinal()] = tableRows.get(0).getPropertyValue( visibleColumn );
        }
        for (AggregationColumn aggregationColumn : aggregationColumns) {
            values[aggregationColumn.getColumnOrdinal()] = tableRows.get(0).hasColumn(aggregationColumn) ?
                    tableRows.get(0).getPropertyValue(aggregationColumn) // if this column already exists by reference.
                    : aggregate(aggregationColumn, tableRows);
        }
        return new TableRow(rowsColumns, values, firstRowOrderColumns, firstRowOrderValues,
                firstRowGroupByColumns, firstRowGroupByValues);
    }

    public static TableRow aggregate(List<TableRow> tableRows, List<IQueryColumn> selectedColumns) {
        if (tableRows.isEmpty()) {
            return new TableRow((IQueryColumn[]) null, null);
        }
        IQueryColumn[] rowsColumns = selectedColumns.toArray(new IQueryColumn[0]);
        OrderColumn[] firstRowOrderColumns = tableRows.get(0).getOrderColumns();
        Object[] firstRowOrderValues = tableRows.get(0).getOrderValues();
        IQueryColumn[] firstRowGroupByColumns = tableRows.get(0).getGroupByColumns();
        Object[] firstRowGroupByValues = tableRows.get(0).getGroupByValues();
        Object[] values = new Object[rowsColumns.length];
        for (int i = 0; i < rowsColumns.length; i++) {
            IQueryColumn qc = rowsColumns[i];
            if(qc.isAggregate()){
                values[i] = aggregate((AggregationColumn) qc, tableRows);
            }
            else if (qc.isCaseColumn()) {
                values[i] = null;
            }
            else{
                values[i] = tableRows.get(0).getPropertyValue(qc);
            }
        }
        return new TableRow(rowsColumns, values, firstRowOrderColumns, firstRowOrderValues,
                firstRowGroupByColumns, firstRowGroupByValues);
    }

    private static Object aggregate(AggregationColumn aggregationColumn, List<TableRow> tableRows){
        Object value = null;
        Class<?> classType = aggregationColumn.getReturnType();
        AggregationFunctionType type = aggregationColumn.getType();
        String columnName = aggregationColumn.getColumnName();
        switch (type) {
            case COUNT:
                boolean isAllColumn = aggregationColumn.isAllColumns();
                if (isAllColumn) {
                    value = tableRows.size();
                } else {
                    value = tableRows.stream().map(tr -> tr.getPropertyValue(columnName))
                            .filter(Objects::nonNull).count();
                }
                break;
            case MAX:
                value = tableRows.stream().map(tr -> tr.getPropertyValue(columnName))
                        .filter(Objects::nonNull).max(getObjectComparator()).orElse(null);
                break;
            case MIN:
                value = tableRows.stream().map(tr -> tr.getPropertyValue(columnName))
                        .filter(Objects::nonNull).min(getObjectComparator()).orElse(null);
                break;
            case AVG:
                if (!Number.class.isAssignableFrom(classType)) {
                    throw new UnsupportedOperationException("Can't perform AVG aggregation function on type " +
                            "[" + classType.getTypeName() + "], AVG supports only types of " + Number.class);
                }
                MutableNumber sum = null;
                long count = 0;
                for (TableRow tableRow : tableRows) {
                    Number number = (Number) tableRow.getPropertyValue(columnName);
                    if (number == null) continue;
                    if (sum == null) {
                        sum = MutableNumber.fromClass(number.getClass(), true);
                    }
                    sum.add(number);
                    count++;
                }
                value = count == 0 ? 0 : sum.calcDivision(count);
                break;
            case SUM:
                if (!Number.class.isAssignableFrom(classType)) {
                    throw new UnsupportedOperationException("Can't perform SUM aggregation function on type " +
                            "[" + classType.getTypeName() + "], SUM supports only types of " + Number.class);
                }
                sum = null;
                for (TableRow tableRow : tableRows) {
                    Number number = (Number) tableRow.getPropertyValue(columnName);
                    if (number == null) continue;
                    if (sum == null) {
                        sum = MutableNumber.fromClass(number.getClass(), true);
                    }
                    sum.add(number);
                }
                value = sum == null ? null : sum.toNumber();
                break;
        }
        return value;
    }

    private static Comparator<Object> getObjectComparator() {
        return (o1, o2) -> {
            Comparable first = castToComparable(o1);
            Comparable second = castToComparable(o2);
            return first.compareTo(second);
        };
    }

    /**
     * Cast the object to Comparable otherwise throws an IllegalArgumentException exception
     */
    public static Comparable castToComparable(Object obj) {
        try {
            return (Comparable) obj;
        } catch (ClassCastException cce) {
            throw new IllegalArgumentException("Type " + obj.getClass() +
                    " doesn't implement Comparable, Serialization mode might be different than " + StorageType.OBJECT + ".", cce);
        }
    }
}