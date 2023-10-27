## ElasticSearch Bulk Document Updater Script

This Python script is designed to perform bulk Elasticsearch data update operations.
It accomplishes the following tasks:
1. Reads document data from a JSON file.
2. Parallelizes document updates.
3. Sends each update operation to Elasticsearch in bulk.
   
### Usage:
- This script reads document data from a JSON file and performs document updates on Elasticsearch.
- The JSON file should have the following format:
```json
  {
      "field_key1": ["ticket_key1", "ticket_key2", ...],
      "field_key2": ["ticket_key3", "ticket_key4", ...],
      ...
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
```sql
SELECT json_agg(json_build_array(field_key, ticket_keys))
FROM (SELECT fd.key AS field_key, jsonb_agg(cf.ticket_key) AS ticket_keys
      FROM field_definition fd
               LEFT JOIN custom_field cf ON fd.id = cf.definition_id
      WHERE fd.type = 'NUMBER_DECIMAL' AND cf.ticket_key IS NOT NULL
      GROUP BY fd.key) subquery
```

### How to run:
- Install the `requests` and `argparse` libraries for Python.
  - or run the following command:
  - `pip install -r requirements.txt`
- Run the script with the following command:
  - **tenantId**: The tenantId to be updated.
  - **env**: The environment to be updated.
```bash
python script.py --tenantId develop --env local
```

---

#### Don't forget to update `environment_addresses` for each environment address.
