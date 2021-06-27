package com.gigaspaces.jdbc.calcite.schema;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.gigaspaces.jdbc.model.result.TableRow;
import com.gigaspaces.jdbc.model.result.TempQueryResult;
import com.gigaspaces.jdbc.model.table.ConcreteColumn;
import com.gigaspaces.jdbc.model.table.SchemaTableContainer;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.admin.IRemoteJSpaceAdmin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.List;

public class GSSchemaTable extends AbstractTable {
    private final PGSystemTable systemTable;

    public GSSchemaTable(PGSystemTable systemTable) {
        this.systemTable = systemTable;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
        for (SchemaProperty schema : this.systemTable.getProperties()) {
            builder.add(schema.getPropertyName(), schema.getSqlTypeName());
        }
        return builder.build();
    }

    public String getName() {
        return systemTable.name();
    }

    public SchemaProperty[] getSchemas() {
        return this.systemTable.getProperties();
    }

    public QueryResult execute(SchemaTableContainer schemaTableContainer, IJSpace space, List<ConcreteColumn> queryColumns) throws SQLException {
        QueryResult queryResult = new TempQueryResult(schemaTableContainer);
        ConcreteColumn[] arr = queryColumns.toArray(new ConcreteColumn[0]);
        switch (systemTable) {
            case pg_am:
            case pg_attrdef:
                break; // will return empty result
            case pg_tables:
                getSpaceTables(space).forEach(table -> {
                    ITypeDesc typeDesc = space.getDirectProxy().getTypeManager().getTypeDescByName(table);
                    queryResult.addRow(new TableRow(arr, table, !typeDesc.getIndexes().isEmpty()));
                });
            default:
                throw new UnsupportedOperationException("Unhandled system table " + systemTable.name());
        }
        return queryResult;
    }

    private List<String> getSpaceTables(IJSpace space) throws SQLException {
        try {
            return ((IRemoteJSpaceAdmin) space.getAdmin()).getRuntimeInfo().m_ClassNames;
        } catch (RemoteException e) {
            throw new SQLException("Failed to get runtime info from space", e);
        }
    }


    public static class SchemaProperty {

        private final String propertyName;
        private final SqlTypeName sqlTypeName;

        public SchemaProperty(String propertyName, SqlTypeName sqlTypeName) {
            this.propertyName = propertyName;
            this.sqlTypeName = sqlTypeName;
        }

        public String getPropertyName() {
            return propertyName;
        }

        public SqlTypeName getSqlTypeName() {
            return sqlTypeName;
        }

        public static SchemaProperty of(String propertyName, SqlTypeName typeName) {
            return new SchemaProperty(propertyName, typeName);
        }
    }
}
