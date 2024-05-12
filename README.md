# i190622_MLOPS_Ass2
Tasks Performed:

1. *Data Extraction:*
   - Utilize dawn.com and BBC.com as your data sources.
   - Extract links on the landing page
   - Extract the titles and descriptions from articles displayed on their homepages.

2. *Data Transformation:*
   - Preprocess the extracted text data. Ensure to clean and format the text appropriately for further analysis.

3. *Data Storage and Version Control:*
   - Store the processed data on Google Drive.
   - Implement Data Version Control (DVC) to track versions of the data. Ensure each version is accurately recorded as changes are made.
   - Try to version the metadata against each dvc push to github repo.

4. *Apache Airflow DAG Development:*
   - Write an Airflow DAG to automate the processes of extraction, transformation, and storage.
   - Ensure the DAG handles task dependencies and error management effectively.
