## ElasticSearch Bulk Document Updater Script

This Python script is designed to perform add a field to all Elasticsearch docs with given schema.

It accomplishes the following tasks:
1. Reads document data from a JSON file.
2. Parallelizes document updates.
3. Sends each update operation to Elasticsearch in bulk.
   
### Usage:
- This script reads document data from a JSON file and performs document updates on Elasticsearch.
- The JSON file should have the following format:
```json
{
  "<tenantId>": <fieldOptionId>,
  "mpass": 34
}
  ```
- After the execution, it prints start and end times along with the processing time.

### Requirements:
- Python 3.6 or higher
- Elasticsearch 7.6.2 or higher
- `requests` library for Python
- `result.json` file containing the document data to be updated. *(This file should be in the same directory as the script)*

### Generate `result.json` file:
This query can be used to generate the `result.json` file from the database.

- First execute this SQL command to get `schema_names` for all tenants.
```sql
SELECT 'SELECT jsonb_object_agg(foo.tenantid, foo.json) FROM(' || string_agg(
        '(select ''' || mt.schema_name || ''' as tenantId, fdeo.id as json from ' || mt.schema_name ||
        '.field_definition fd LEFT JOIN ' || mt.schema_name ||
        '.field_definition_entity_options fdeo on fd.id = fdeo.field_definition_entity_id WHERE fd.key = ''ts.scope'' AND fdeo.label = ''TICKET'')',
        ' UNION ') || ')as foo' || ';'
from main.tenant mt;
```

- Copy generated SQL query string and execute with `\gexec` command using `psql`

> SELECT 'SELECT jsonb_object_agg(foo ................. from main.tenant mt **\gexec**

- Then copy result and paste it to `result.json` file

### How to run:
- Install the `requests` and `argparse` libraries for Python.
  - or run the following command:
  - `pip install -r requirements.txt`
- Run the script with the following command:
  - **env**: The environment to be updated.
```bash
python script.py --env local
```

---

#### Don't forget to update `environment_addresses` for each environment address.
