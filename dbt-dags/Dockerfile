FROM fishtownanalytics/dbt:1.0.0

COPY patrojun6040/ /dbt

WORKDIR /dbt

RUN dbt deps --profiles-dir /dbt/profile