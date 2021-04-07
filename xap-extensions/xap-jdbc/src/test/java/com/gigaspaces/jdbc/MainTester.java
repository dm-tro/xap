/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gigaspaces.jdbc;

import com.gigaspaces.internal.server.space.tiered_storage.TieredStorageTableConfig;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.config.TieredStorageConfigurer;
import org.openspaces.core.space.AbstractSpaceConfigurer;
import org.openspaces.core.space.EmbeddedSpaceConfigurer;
import org.openspaces.core.space.SpaceProxyConfigurer;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;

public class MainTester {
    public static void main(String[] args) throws SQLException, ParseException {
        boolean newDriver = Boolean.getBoolean("useNewDriver");
        GigaSpace space = createAndFillSpace(newDriver, true);

        Properties properties = new Properties();
//                try (Connection connection = GSConnection.getInstance(space.getSpace(), properties)) {
        properties.put("com.gs.embeddedQP.enabled", "true");

        try {
            Class.forName("com.j_spaces.jdbc.driver.GDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
//        try (Connection connection = DriverManager.getConnection("jdbc:gigaspaces:url:jini://*/*/"+space.getSpaceName(), properties)) {
        try (Connection connection = DriverManager.getConnection(newDriver ? "jdbc:gigaspaces:v3://localhost:4174/" + space.getSpaceName() : "jdbc:gigaspaces:url:jini://*/*/" + space.getSpaceName(), properties)) {
//            PreparedStatement st = connection.prepareStatement("EXPLAIN SELECT * FROM com.gigaspaces.jdbc.MyPojo where age = ?");
//            st.setInt(1, 30);
//
//
//            ResultSet rs = st.executeQuery();
//            DumpUtils.dump(rs);

            Statement statement = connection.createStatement();
//            execute(statement, "SELECT * FROM com.gigaspaces.jdbc.MyPojo where timestamp > '2001-09-10 05:20:00'");// WHERE rowNum <= 10");
            execute(statement, "explain SELECT *, birthLong FROM com.gigaspaces.jdbc.MyPojo where birthLong = 1000192800000");// WHERE rowNum <= 10");

//            execute(statement, "SELECT UID,* FROM com.gigaspaces.jdbc.MyPojo");// WHERE rowNum <= 10");
//            execute(statement, "SELECT UID,* FROM com.gigaspaces.jdbc.MyPojo WHERE country like '%a%'");
//            execute(statement, "SELECT UID,* FROM com.gigaspaces.jdbc.MyPojo WHERE age NOT BETWEEN 10 and 20");
//
//            execute(statement, "select name,age from com.gigaspaces.jdbc.MyPojo where name='Adler' and age=20");
//
//            execute(statement, "explain select name,age from com.gigaspaces.jdbc.MyPojo where name='Adler' and age=20");
//            execute(statement, "explain verbose select name,age from com.gigaspaces.jdbc.MyPojo where name='Adler' and age=20");

//            String sqlQuery = "select name, name, id from com.gigaspaces.jdbc.MyPojo";
//            String sqlQuery = "select * from com.gigaspaces.jdbc.MyPojo";
//                String sqlQuery = "explain select name,age from com.gigaspaces.jdbc.MyPojo where name='Adler' and age=20";
//            String sqlQuery = "select * from com.gigaspaces.jdbc.MyPojo AS A";
//            String sqlQuery = "select name from (select A.name AS NAME_A, B.name as NAME_B, A.age from com.gigaspaces.jdbc.MyPojo AS A inner join com.gigaspaces.jdbc.MyPojo AS B ON A.name = B.name)";
//            String sqlQuery = "select name AS name2 from (select name from com.gigaspaces.jdbc.MyPojo)";
//            String sqlQuery = "select A.name AS NAME_A, B.name as NAME_B, A.age from com.gigaspaces.jdbc.MyPojo AS A inner join com.gigaspaces.jdbc.MyPojo AS B ON A.name = B.name";
//            String sqlQuery = "explain select A.name AS NAME_A, B.name as NAME_B, A.age from com.gigaspaces.jdbc.MyPojo AS A inner join com.gigaspaces.jdbc.MyPojo AS B ON A.name = B.name";
//                ResultSet res = statement.executeQuery(sqlQuery);
//                DumpUtils.dump(res);

        }
    }

    private static void execute(Statement statement, String sql) throws SQLException {
        ResultSet res = statement.executeQuery(sql);
        System.out.println();
        System.out.println("Executing: " + sql);
        DumpUtils.dump(res);

    }

    private static GigaSpace createAndFillSpace(boolean newDriver, boolean embedded) throws ParseException {
        String spaceName = "demo" + (newDriver ? "new" : "old");
        AbstractSpaceConfigurer configurer = embedded ? new EmbeddedSpaceConfigurer(spaceName)
//                .tieredStorage(new TieredStorageConfigurer().addTable(new TieredStorageTableConfig().setName(MyPojo.class.getName()).setCriteria("age > 20")))
                : new SpaceProxyConfigurer(spaceName);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        GigaSpace gigaSpace = new GigaSpaceConfigurer(configurer).gigaSpace();
        if (embedded || gigaSpace.count(null) == 0) {
            java.util.Date date1 = simpleDateFormat.parse("10/09/2001 05:20:00");
            java.util.Date date2 = simpleDateFormat.parse("11/09/2001 10:20:00");
            java.util.Date date3 = simpleDateFormat.parse("12/09/2001 15:20:00");
            java.util.Date date4 = simpleDateFormat.parse("13/09/2001 20:20:00");
            gigaSpace.write(new MyPojo("Adler", 20, "Israel", date1, new Time(date1.getTime()), new Timestamp(date1.getTime())));
            gigaSpace.write(new MyPojo("Adam", 30, "Israel", date2, new Time(date2.getTime()), new Timestamp(date2.getTime())));
            gigaSpace.write(new MyPojo("Eve", 35, "UK", date3, new Time(date3.getTime()), new Timestamp(date3.getTime())));
            gigaSpace.write(new MyPojo("NoCountry", 40, null, date4, new Time(date4.getTime()), new Timestamp(date4.getTime())));
        }
        return gigaSpace;
    }
}