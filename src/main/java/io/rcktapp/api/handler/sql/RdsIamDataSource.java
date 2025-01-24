package io.rcktapp.api.handler.sql;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.services.rds.auth.GetIamAuthTokenRequest;
import com.amazonaws.services.rds.auth.RdsIamAuthTokenGenerator;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class RdsIamDataSource extends HikariDataSource {

    public RdsIamDataSource(HikariConfig config) {
        super(config);
    }

    @Override
    public String getPassword() {
        return generateAuthToken();
    }

    private String generateAuthToken() {
        RdsIamAuthTokenGenerator generator = RdsIamAuthTokenGenerator.builder()
                .credentials(new DefaultAWSCredentialsProviderChain())
                .region(new DefaultAwsRegionProviderChain().getRegion())
                .build();

        return generator.getAuthToken(GetIamAuthTokenRequest.builder()
                .hostname(determineHostname(getJdbcUrl()))
                .port(determinePort(getJdbcUrl()))
                .userName(getUsername())
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
}