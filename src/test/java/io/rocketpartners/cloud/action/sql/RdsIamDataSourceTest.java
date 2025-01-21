package io.rocketpartners.cloud.action.sql;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RdsIamDataSourceTest {

    @Test
    public void determineHostname() {
        testHostnames("jdbc:mysql://cirk-dev.cluster-uniquename.us-east-1.rds.amazonaws.com:3306/lift?autoReconnect=true&useUnicode=yes&characterEncoding=UTF-8", "cirk-dev.cluster-uniquename.us-east-1.rds.amazonaws.com", false);
        testHostnames("jdbc:mysql://localhost:3306/mysql", "localhost", false);
        testHostnames("jdbc:mysql://localhost:3306", "localhost", false);
        testHostnames("jdbc:redshift://examplecluster.abc123xyz789.us-west-2.redshift.amazonaws.com:5439/dev", "examplecluster.abc123xyz789.us-west-2.redshift.amazonaws.com", false);
        testHostnames("jdbc:mysql://localhost/mysql", "", true); // missing port number
        testHostnames("jdbc:mysql:localhost:3306/mysql", "", true); // missing separator between connection type and hostname
    }

    private void testHostnames(String givenJdbcUrl, String expectedHostname, boolean expectException) {
        if (expectException) {
            try {
                RdsIamDataSource.determineHostname(givenJdbcUrl);
                fail("Expected an exception, but did not see an exception");
            } catch (IllegalArgumentException exception) {
                assertTrue(StringUtils.isNotEmpty(exception.getMessage()));
            } catch (Exception exception) {
                fail(exception.getMessage());
            }
        } else {
            assertEquals(expectedHostname, RdsIamDataSource.determineHostname(givenJdbcUrl));
        }
    }

    @Test
    public void determinePorts() {
        testPorts("jdbc:mysql://cirk-dev.cluster-uniquename.us-east-1.rds.amazonaws.com:3306/lift?autoReconnect=true&useUnicode=yes&characterEncoding=UTF-8", 3306, false);
        testPorts("jdbc:mysql://localhost:3306/mysql", 3306, false);
        testPorts("jdbc:mysql://localhost:44444", 44444, false);
        testPorts("jdbc:redshift://examplecluster.abc123xyz789.us-west-2.redshift.amazonaws.com:5439/dev", 5439, false);
        testPorts("jdbc:mysql://localhost/mysql", -1, true); // missing port number
        testPorts("jdbc:mysql:localhost:3306/mysql", -1, true); // missing separator between connection type and hostname
    }

    private void testPorts(String givenJdbcUrl, int expectedPort, boolean expectException) {
        if (expectException) {
            try {
                RdsIamDataSource.determinePort(givenJdbcUrl);
                fail("Expected an exception, but did not see an exception");
            } catch (IllegalArgumentException exception) {
                assertTrue(StringUtils.isNotEmpty(exception.getMessage()));
            } catch (Exception exception) {
                fail(exception.getMessage());
            }
        } else {
            assertEquals(expectedPort, RdsIamDataSource.determinePort(givenJdbcUrl));
        }
    }
}