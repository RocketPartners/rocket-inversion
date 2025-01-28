package io.rcktapp.api.handler.sql;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.services.rds.auth.GetIamAuthTokenRequest;
import com.amazonaws.services.rds.auth.RdsIamAuthTokenGenerator;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class RdsIamDataSource extends HikariDataSource {

    public RdsIamDataSource(HikariConfig config) {
        super(config);
    }

    @Override
    public String getPassword() {
        return generateAuthToken(getJdbcUrl(), getUsername());
    }

    private static String generateAuthToken(String jdbcUrl, String username) {
        RdsIamAuthTokenGenerator generator = RdsIamAuthTokenGenerator.builder()
                .credentials(new DefaultAWSCredentialsProviderChain())
                .region(new DefaultAwsRegionProviderChain().getRegion())
                .build();

        return generator.getAuthToken(GetIamAuthTokenRequest.builder()
                .hostname(determineHostname(jdbcUrl))
                .port(determinePort(jdbcUrl))
                .userName(username)
                .build());
    }

    protected static String determineHostname(String jdbcUrl) {
        validateUrl(jdbcUrl);
        return jdbcUrl.substring(jdbcUrl.indexOf("//") + 2, jdbcUrl.lastIndexOf(":"));
    }

    protected static int determinePort(String jdbcUrl) {
        validateUrl(jdbcUrl);
        String portStringStart = jdbcUrl.substring(jdbcUrl.lastIndexOf(":") + 1);
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < portStringStart.length(); i++) {
            if (Character.isDigit(portStringStart.charAt(i))) {
                stringBuilder.append(portStringStart.charAt(i));
            } else {
                break;
            }
        }
        return Integer.parseInt(stringBuilder.toString());
    }

    private static void validateUrl(String jdbcUrl) {
        int separatorIndex = jdbcUrl.indexOf("//");
        int hostnameEndIndex = jdbcUrl.lastIndexOf(":");
        if (separatorIndex == -1 || separatorIndex + 2 >= hostnameEndIndex) {
            throw new IllegalArgumentException("Invalid JDBC URL: " + jdbcUrl);
        }
    }

    public static Connection getSingleConnection(String jdbcUrl, String username) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", username);
        props.setProperty("password", generateAuthToken(jdbcUrl, username));
        return DriverManager.getConnection(jdbcUrl, props);
    }
}