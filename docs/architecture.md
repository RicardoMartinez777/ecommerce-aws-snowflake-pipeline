# Architecture

This project follows an ELT architecture using AWS S3 and Snowflake.

- AWS S3 is used to store raw source files before they are loaded into Snowflake.
- Snowflake RAW schema stores data without transformation.
- STAGING schema contains cleaned and enriched tables.
- ANALYTICS schema provides views and analytical queries.

The architecture is designed to be simple, reproducible, and easy to extend.
