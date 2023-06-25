## Seeds

> Seeds are CSV files in your dbt project (typically in your data directory), that dbt can load into your data warehouse using the dbt seed command.
> Seeds can be referenced in downstream models the same way as referencing models — by using the [ref function](https://docs.getdbt.com/reference/dbt-jinja-functions/ref).

### Using Seeds

- For working with seeds check out the [docs](https://docs.getdbt.com/docs/building-a-dbt-project/seeds)
- There is additionally the option to [configure seeds](https://docs.getdbt.com/reference/seed-configs)
- The Basic steps are as follows:

1. Add the csv to the `data` directory
2. Add the csv name to the `seeds` key in the `dbt_project.yaml`

### Tips and Pitfalls

- Once the target table has been created from the seed, the table columns are fixed.
- If you want to change the data structure you have to drop the target table first.

### Data Sources
