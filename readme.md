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
    "organizationId1": ["requesterId1", "requesterId2", ...],
    "organizationId2": ["requesterId3", "requesterId4", ...],
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
```postgresql
SELECT json_agg(json_build_array(organization_id, user_ids))
FROM (SELECT au.organization_id, jsonb_agg(DISTINCT au.id) AS user_ids
      FROM ticket t
               LEFT JOIN app_user au ON t.requester_id = au.id
      WHERE au.organization_id IS NOT NULL
      GROUP BY au.organization_id
      ) subquery;
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