package io.rcktapp.api.handler.sql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.rds.RdsUtilities;
import software.amazon.awssdk.services.rds.model.GenerateAuthenticationTokenRequest;

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
        Region region = new DefaultAwsRegionProviderChain().getRegion();
        RdsUtilities utilities = RdsUtilities.builder()
                .region(region)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        return utilities.generateAuthenticationToken(GenerateAuthenticationTokenRequest.builder()
                .hostname(determineHostname(jdbcUrl))
                .port(determinePort(jdbcUrl))
                .username(username)
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