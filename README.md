# TODOs
- add logging
- handle exceptions
    - do not use exists_ok=True in create_bq_dataset > create_dataset, but log existence
    - do not use exists_ok=True in create_bq_table > create_table, but log existence
    - do not use not_found_ok=True in create_bq_table > delete_table, but log inexistence
- extract authorization into private function e.g. _authorize_gcloud_services