## Unreleased

## [0.3.6.18 - 04/27/26]
- LIFT-2452: Restore async (fire-and-forget) Firehose client to fix Tomcat thread blocking causing player disconnections

## [0.3.6.17 - 04/05/26]
- LIFT-2099: Add relevance scoring to wildcard search queries (w, sw, ew operators)
  - Wildcard queries now include match queries in `should` for TF-IDF/BM25 relevance scoring
  - `_score` added as secondary sort when search terms are present
  - Fixed multi-value wildcard queries restoring OR semantics (e.g. sw(city,Chand,Atl) matches either value)
  - Fixed `_score` sort serialization (removed unsupported `missing` parameter)

## [0.3.6.16 - 03/30/26]
- LIFT-1807: Upgrade RDS IAM auth token generation to AWS SDK v2

## [0.3.6.15 - 03/30/26]
- LIFT-1806: Upgrade Firehose to AWS SDK v2

## [0.3.6.14 - 03/20/26]
- LIFT-1805: Upgrade DynamoDB from AWS SDK v1 to v2

## [0.3.6.13 - 03/19/26]
- LIFT-1395: Migrate off c3p0 to HikariCP
- LIFT-1804: fix etag capitalization from s3 sdk v2 upgrade

## [0.3.6.12 - 02/27/26]
- LIFT-1804: aws s3 sdk v2

## [0.3.6.11 - 12/15/25]
- LIFT-1230: Support ``maxIdleTimeExcessConnections`` configuration for c3p0

## [0.3.6.10 - 3/26/25]
- LIFTBAU-3259: support integration with Spring Boot 3

## [0.3.6.9 - 3/26/25]
- LIFTBAU-3016: Use SqlDB catalog name, if provided, on initial DB load

## [0.3.6.8 - 3/20/25]
- LIFTBAU-3239: Re-add ComboPooledDataSource for non-IAM RDS connections

## [0.3.6.7 - 1/22/25]
- LIFTBAU-2803: Fix incorrect rowCount when querying without `ORDER BY`

## [0.3.6.6 - 1/14/25]
- LIFTBAU-2803: Remove/replace SQL_CALC_FOUND_ROWS and FOUND_ROWS commands for performance

## [0.3.6.5 - 1/7/25]
- LIFTBAU-2988: Add RDS IAM authentication

## [0.3.6.4 - 09/23/24]
- LIFTBAU-2613: fix long casting error

## [0.3.6.3 - 05/02/24]
- LIFTBAU-1812: Fix mysql injection via RQL
