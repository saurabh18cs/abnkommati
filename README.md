# PySpark Data Joining Utility

This utility is designed to read, join, and filter data from two CSV files using PySpark. It's a simple yet effective tool for data manipulation, originally developed as part of a job application assignment. The utility reads two CSV files, converts them into Spark DataFrames, and merges them into a single DataFrame. It then renames the columns and filters the rows based on user-specified countries. The final output is a CSV file saved in the **client data** directory.

## How to Use

The utility requires three inputs from the user:
- **fpath_1**: Path to the first CSV file
- **fpath_2**: Path to the second CSV file
- **countries**: One or more countries to filter the data by (e.g., France, Netherlands, United Kingdom, United States)

Here's an example of how to start the utility:

```python
join.py --fpath_1 'resources/dataset_one.csv' --fpath_2 'resources/dataset_two.csv' --countries 'United Kingdom' 'Netherlands'
```
This command will join the data from 'dataset_one.csv' and 'dataset_two.csv', filter for 'United Kingdom' and 'Netherlands', and save the result as a CSV file in the **client data** directory excluding PII data.