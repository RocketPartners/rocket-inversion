package io.rcktapp.api.handler.sql;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RdsIamDataSourceTest {

    @ParameterizedTest
    @MethodSource("provideHostnames")
    void determineHostname(String givenJdbcUrl, String expectedHostname, boolean expectException) {
        if (expectException) {
            assertThrows(IllegalArgumentException.class, () -> RdsIamDataSource.determineHostname(givenJdbcUrl));
        } else {
            assertEquals(expectedHostname, RdsIamDataSource.determineHostname(givenJdbcUrl));
        }
    }

    static Stream<Arguments> provideHostnames() {
        return Stream.of(
                Arguments.of("jdbc:mysql://cirk-dev.cluster-uniquename.us-east-1.rds.amazonaws.com:3306/lift?autoReconnect=true&useUnicode=yes&characterEncoding=UTF-8", "cirk-dev.cluster-uniquename.us-east-1.rds.amazonaws.com", false),
                Arguments.of("jdbc:mysql://localhost:3306/mysql", "localhost", false),
                Arguments.of("jdbc:mysql://localhost:3306", "localhost", false),
                Arguments.of("jdbc:redshift://examplecluster.abc123xyz789.us-west-2.redshift.amazonaws.com:5439/dev", "examplecluster.abc123xyz789.us-west-2.redshift.amazonaws.com", false),
                Arguments.of("jdbc:mysql://localhost/mysql", "", true), // missing port number
                Arguments.of("jdbc:mysql:localhost:3306/mysql", "", true) // missing separator between connection type and hostname
        );
    }

    @ParameterizedTest
    @MethodSource("providePorts")
    void determinePort(String givenJdbcUrl, int expectedPort, boolean expectException) {
        if (expectException) {
            assertThrows(IllegalArgumentException.class, () -> RdsIamDataSource.determinePort(givenJdbcUrl));
        } else {
            assertEquals(expectedPort, RdsIamDataSource.determinePort(givenJdbcUrl));
        }
    }

    static Stream<Arguments> providePorts() {
        return Stream.of(
                Arguments.of("jdbc:mysql://cirk-dev.cluster-uniquename.us-east-1.rds.amazonaws.com:3306/lift?autoReconnect=true&useUnicode=yes&characterEncoding=UTF-8", 3306, false),
                Arguments.of("jdbc:mysql://localhost:3306/mysql", 3306, false),
                Arguments.of("jdbc:mysql://localhost:44444", 44444, false),
                Arguments.of("jdbc:redshift://examplecluster.abc123xyz789.us-west-2.redshift.amazonaws.com:5439/dev", 5439, false),
                Arguments.of("jdbc:mysql://localhost/mysql", -1, true), // missing port number
                Arguments.of("jdbc:mysql:localhost:3306/mysql", -1, true) // missing separator between connection type and hostname
        );
    }
}