package com.gigaspaces.sql.datagateway.netty.server;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.text.SimpleDateFormat;

public class OdbcMetadataQueryTest extends AbstractServerTest {
    private static final String SET_DATE_STYLE_ISO = "SET DateStyle = 'ISO';";
    private static final String SET_EXTRA_FLOAT_DIGITS_2 = "SET extra_float_digits = 2;";
    private static final String SHOW_TRANSACTION_ISOLATION = "show transaction_isolation;";
    private static final String SELECT_TYPE_WHERE_TYPNAME_LO = "select oid, typbasetype from pg_type where typname = 'lo';";
    private static final String SHOW_MAX_IDENTIFIER_LENGTH = "show max_identifier_length;";
    private static final String SELECT_NULL = "select NULL, NULL, NULL";
    private static final String SELECT_TABLES = "select relname, nspname, relkind from pg_catalog.pg_class c, pg_catalog.pg_namespace n where relkind in ('r', 'v', 'm', 'f', 'p') and nspname not in ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1') and n.oid = relnamespace order by nspname, relname";
    private static final String SELECT_ATTRIBUTES = "select n.nspname, c.relname, a.attname, a.atttypid, t.typname, a.attnum, a.attlen, a.atttypmod, a.attnotnull, c.relhasrules, c.relkind, c.oid, pg_get_expr(d.adbin, d.adrelid), case t.typtype when 'd' then t.typbasetype else 0 end, t.typtypmod, c.relhasoids, '', c.relhassubclass from (((pg_catalog.pg_class c inner join pg_catalog.pg_namespace n on n.oid = c.relnamespace and c.relname like E'com.gigaspaces.sql.datagateway.netty.server.MyPojo' and n.nspname like E'public') inner join pg_catalog.pg_attribute a on (not a.attisdropped) and a.attnum > 0 and a.attrelid = c.oid) inner join pg_catalog.pg_type t on t.oid = a.atttypid) left outer join pg_attrdef d on a.atthasdef and d.adrelid = a.attrelid and d.adnum = a.attnum order by n.nspname, c.relname, attnum";
    private static final String SELECT_ATTRIBUTES_ORG_ORDERED = "" +
            "select n.nspname, c.relname, a.attname, a.atttypid, t.typname, a.attnum, a.attlen, a.atttypmod, a.attnotnull, c.relhasrules, c.relkind, c.oid, " +
            "pg_get_expr(d.adbin, d.adrelid), case t.typtype when 'd' then t.typbasetype else 0 end, t.typtypmod, c.relhasoids, '', c.relhassubclass " +
            "from " +
            "   (" +
            "       (" +
            "           (pg_catalog.pg_class c inner join pg_catalog.pg_namespace n on n.oid = c.relnamespace and c.relname like E'com.gigaspaces.sql.datagateway.netty.server.MyPojo' and n.nspname like E'public')" +
            "           inner join pg_catalog.pg_attribute a on (not a.attisdropped) and a.attnum > 0 and a.attrelid = c.oid" +
            "       ) inner join pg_catalog.pg_type t on t.oid = a.atttypid" +
            "   ) left outer join pg_attrdef d on a.atthasdef and d.adrelid = a.attrelid and d.adnum = a.attnum" +
            " order by n.nspname, c.relname, attnum";
    private static final String SELECT_ATTRIBUTESzz = "" +
            "select n.nspname, c.relname, a.attname, a.atttypid, t.typname, a.attnum, a.attlen, a.atttypmod, a.attnotnull, c.relhasrules, c.relkind, c.oid, " +
            "pg_get_expr(d.adbin, d.adrelid), case t.typtype when 'd' then t.typbasetype else 0 end, t.typtypmod, c.relhasoids, '', c.relhassubclass " +
            "from " +
            "   (" +
            "       (" +
            "           (pg_catalog.pg_class c inner join pg_catalog.pg_namespace n on n.oid = c.relnamespace and c.relname like E'com.gigaspaces.sql.datagateway.netty.server.MyPojo' and n.nspname like E'public')" +
            "           inner join pg_catalog.pg_attribute a on (not a.attisdropped) and a.attnum > 0 and a.attrelid = c.oid" +
            "       ) inner join pg_catalog.pg_type t on t.oid = a.atttypid" +
            "   ) left outer join pg_attrdef d on a.atthasdef and d.adrelid = a.attrelid and d.adnum = a.attnum" +
            " order by n.nspname, c.relname, attnum";

    private static final String SELECT_INDEXES = "select ta.attname, ia.attnum, ic.relname, n.nspname, tc.relname from pg_catalog.pg_attribute ta, pg_catalog.pg_attribute ia, pg_catalog.pg_class tc, pg_catalog.pg_index i, pg_catalog.pg_namespace n, pg_catalog.pg_class ic where tc.relname = E'test_table' AND n.nspname = E'public' AND tc.oid = i.indrelid AND n.oid = tc.relnamespace AND i.indisprimary = 't' AND ia.attrelid = i.indexrelid AND ta.attrelid = i.indrelid AND ta.attnum = i.indkey[ia.attnum-1] AND (NOT ta.attisdropped) AND (NOT ia.attisdropped) AND ic.oid = i.indexrelid order by ia.attnum";
    private static final String SELECT_CONSTRAINTS = "select\t'mySpace'::name as \"PKTABLE_CAT\",\n" +
            "\tn2.nspname as \"PKTABLE_SCHEM\",\n" +
            "\tc2.relname as \"PKTABLE_NAME\",\n" +
            "\ta2.attname as \"PKCOLUMN_NAME\",\n" +
            "\t'mySpace'::name as \"FKTABLE_CAT\",\n" +
            "\tn1.nspname as \"FKTABLE_SCHEM\",\n" +
            "\tc1.relname as \"FKTABLE_NAME\",\n" +
            "\ta1.attname as \"FKCOLUMN_NAME\",\n" +
            "\ti::int2 as \"KEY_SEQ\",\n" +
            "\tcase ref.confupdtype\n" +
            "\t\twhen 'c' then 0::int2\n" +
            "\t\twhen 'n' then 2::int2\n" +
            "\t\twhen 'd' then 4::int2\n" +
            "\t\twhen 'r' then 1::int2\n" +
            "\t\telse 3::int2\n" +
            "\tend as \"UPDATE_RULE\",\n" +
            "\tcase ref.confdeltype\n" +
            "\t\twhen 'c' then 0::int2\n" +
            "\t\twhen 'n' then 2::int2\n" +
            "\t\twhen 'd' then 4::int2\n" +
            "\t\twhen 'r' then 1::int2\n" +
            "\t\telse 3::int2\n" +
            "\tend as \"DELETE_RULE\",\n" +
            "\tref.conname as \"FK_NAME\",\n" +
            "\tcn.conname as \"PK_NAME\",\n" +
            "\tcase\n" +
            "\t\twhen ref.condeferrable then\n" +
            "\t\t\tcase\n" +
            "\t\t\twhen ref.condeferred then 5::int2\n" +
            "\t\t\telse 6::int2\n" +
            "\t\t\tend\n" +
            "\t\telse 7::int2\n" +
            "\tend as \"DEFERRABILITY\"\n" +
            " from\n" +
            " ((((((( (select cn.oid, conrelid, conkey, confrelid, confkey,\n" +
            "\t generate_series(array_lower(conkey, 1), array_upper(conkey, 1)) as i,\n" +
            "\t confupdtype, confdeltype, conname,\n" +
            "\t condeferrable, condeferred\n" +
            "  from pg_catalog.pg_constraint cn,\n" +
            "\tpg_catalog.pg_class c,\n" +
            "\tpg_catalog.pg_namespace n\n" +
            "  where contype = 'f' \n" +
            "   and  conrelid = c.oid\n" +
            "   and  relname = E'com.gigaspaces.sql.datagateway.netty.server.MyPojo'\n" +
            "   and  n.oid = c.relnamespace\n" +
            "   and  n.nspname = E'public'\n" +
            " ) ref\n" +
            " inner join pg_catalog.pg_class c1\n" +
            "  on c1.oid = ref.conrelid)\n" +
            " inner join pg_catalog.pg_namespace n1\n" +
            "  on  n1.oid = c1.relnamespace)\n" +
            " inner join pg_catalog.pg_attribute a1\n" +
            "  on  a1.attrelid = c1.oid\n" +
            "  and  a1.attnum = conkey[i])\n" +
            " inner join pg_catalog.pg_class c2\n" +
            "  on  c2.oid = ref.confrelid)\n" +
            " inner join pg_catalog.pg_namespace n2\n" +
            "  on  n2.oid = c2.relnamespace)\n" +
            " inner join pg_catalog.pg_attribute a2\n" +
            "  on  a2.attrelid = c2.oid\n" +
            "  and  a2.attnum = confkey[i])\n" +
            " left outer join pg_catalog.pg_constraint cn\n" +
            "  on cn.conrelid = ref.confrelid\n" +
            "  and cn.contype = 'p')\n" +
            "  order by ref.oid, ref.i";

    @BeforeAll
    public static void setUp() throws Exception {
        Class.forName("org.postgresql.Driver");
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SSS");
        java.util.Date date1 = simpleDateFormat.parse("10/09/2001 05:20:00.231");
        java.util.Date date2 = simpleDateFormat.parse("11/09/2001 10:20:00.250");
        java.util.Date date3 = simpleDateFormat.parse("12/09/2001 15:20:00.100");
        java.util.Date date4 = simpleDateFormat.parse("13/09/2001 20:20:00.300");
        gigaSpace.write(new MyPojo("Adler Aa", 20, "Israel", date1, new Time(date1.getTime()), new Timestamp(date1.getTime())));
        gigaSpace.write(new MyPojo("Adam Bb", 30, "Israel", date2, new Time(date2.getTime()), new Timestamp(date2.getTime())));
        gigaSpace.write(new MyPojo("Eve Cc", 35, "UK", date3, new Time(date3.getTime()), new Timestamp(date3.getTime())));
        gigaSpace.write(new MyPojo("NoCountry Dd", 40, null, date4, new Time(date4.getTime()), new Timestamp(date4.getTime())));
    }

    // Queries executed while creating a datasource with a user query.

    @Test
    public void testSetDateStyleIso() throws Exception {
        checkQuery(SET_DATE_STYLE_ISO);
    }

    @Test
    public void testSetExtraFloatDigits2() throws Exception {
        checkQuery(SET_EXTRA_FLOAT_DIGITS_2);
    }

    @Test
    public void testShowTransactionIsolation() throws Exception {
        checkQuery(SHOW_TRANSACTION_ISOLATION);
    }

    @Test
    public void testSelectTypeWhereTypnameLo() throws Exception {
        checkQuery(SELECT_TYPE_WHERE_TYPNAME_LO);
    }

    @Test
    public void testShowMaxIdentifierLength() throws Exception {
        checkQuery(SHOW_MAX_IDENTIFIER_LENGTH);
    }

    // Queries executed while creating a datasource using UI.

    @Test
    public void testSelectNull() throws Exception {
        checkQuery(SELECT_NULL);
    }

    @Test
    public void testSelectTables() throws Exception {
        checkQuery(SELECT_TABLES);
    }

    @Test
    public void testSelectAttributes() throws Exception {
        checkQuery(SELECT_ATTRIBUTES);
    }

    @Test
    public void testSelectIndexes() throws Exception {
        checkQuery(SELECT_INDEXES);
    }

    @Test
    public void testSelectConstraints() throws Exception {
        checkQuery(SELECT_CONSTRAINTS);
    }

    private void checkQuery(String query) throws Exception {
        try (Connection connection = connect(true)) {
            Statement statement = connection.createStatement();
            System.out.println("Executing: " + query);
            if (query.startsWith("select")) {
                ResultSet res = statement.executeQuery(query);
                DumpUtils.dump(res);
            } else {
                statement.execute(query);
            }
        }
    }
}
