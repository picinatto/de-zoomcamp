Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### BigQuery: "Dataset was not found in location US"

Se o dataset existe no projeto mas o `dbt run` falha com "Dataset ... was not found in location US", o perfil está usando uma **location** diferente da do dataset. No BigQuery, cada dataset tem uma região (ex.: US, EU, us-central1).

1. No console do BigQuery, clique no dataset `zoomcamp` e veja em **Detalhes** qual é a **Localização**.
2. No seu `~/.dbt/profiles.yml`, no output do BigQuery, defina `location` com esse valor (ex.: `location: EU` ou `location: us-central1`).
3. Use `profiles.example.yml` como referência.

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
