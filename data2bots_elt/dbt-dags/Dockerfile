FROM fishtownanalytics/dbt:1.0.0

ENV DB_USER $DB_USER
ENV DB_PASSWORD $DB_PASSWORD
ENV DB_NAME $DB_NAME

COPY patrojun6040/ /dbt

WORKDIR /dbt

RUN dbt deps --profiles-dir /dbt/profile