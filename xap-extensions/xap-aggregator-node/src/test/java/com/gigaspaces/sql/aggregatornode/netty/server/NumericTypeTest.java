package com.gigaspaces.sql.aggregatornode.netty.server;

import com.gigaspaces.annotation.pojo.SpaceId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class NumericTypeTest extends AbstractServerTest {
    @BeforeAll
    static void setUp() {
        gigaSpace.write(new TestPojo(new BigDecimal("0.10")));
        gigaSpace.write(new TestPojo(new BigDecimal("-0.10")));
        gigaSpace.write(new TestPojo(new BigDecimal("-10")));
        gigaSpace.write(new TestPojo(new BigDecimal("10")));
        gigaSpace.write(new TestPojo(new BigDecimal("-10000000000000000000000000000000000")));
        gigaSpace.write(new TestPojo(new BigDecimal("10000000000000000000000000000000000")));
        gigaSpace.write(new TestPojo(new BigDecimal("-.00000000000000000000000000000000001")));
        gigaSpace.write(new TestPojo(new BigDecimal(".00000000000000000000000000000000001")));
        gigaSpace.write(new TestPojo(new BigDecimal("10000000000000000000000000000000000.00000000000000000000000000000000001")));
    }

    public static class TestPojo {
        private String id;
        private BigDecimal decimal;

        public TestPojo() {
        }

        public TestPojo(BigDecimal decimal) {
            this.decimal = decimal;
        }

        @SpaceId(autoGenerate = true)
        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public BigDecimal getDecimal() {
            return decimal;
        }

        public void setDecimal(BigDecimal decimal) {
            this.decimal = decimal;
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSelect(boolean simple) throws Exception {
        try (Connection connect = connect(simple)){
            final PreparedStatement statement = connect.prepareStatement("SELECT \"decimal\" FROM \"" + TestPojo.class.getName() + "\"");

            Assertions.assertTrue(statement.execute());
            final ResultSet resultSet = statement.getResultSet();

            String expected = "" +
"| decimal                                                                 |\n" +
"| ----------------------------------------------------------------------- |\n" +
"| 0.10                                                                    |\n" +
"| -0.10                                                                   |\n" +
"| -10                                                                     |\n" +
"| 10                                                                      |\n" +
"| -10000000000000000000000000000000000                                    |\n" +
"| 10000000000000000000000000000000000                                     |\n" +
"| -1E-35                                                                  |\n" +
"| 1E-35                                                                   |\n" +
"| 10000000000000000000000000000000000.00000000000000000000000000000000001 |";
            DumpUtils.checkResult(resultSet, expected);
        }
    }
}
