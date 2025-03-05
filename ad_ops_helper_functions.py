### Helper functions to automate ad operations tasks.

def generate_new_csv(df, csv=str, name=None):
  """
  Generates a new CSV file with a modified filename.

  Args:
      df (pd.DataFrame): The DataFrame to be saved as a CSV file.
      csv (str): The original CSV filename.
      name (str, optional): The string to be inserted into the filename at position 9.

  Returns:
      None: The function saves the DataFrame as a CSV file with the updated filename.

  The function modifies the filename by inserting `name` at the 9th character position
  and saves the DataFrame as a CSV file with this new name.
  """
  updated_file_name = csv[:9] + name + csv[9:]

  return df.to_csv(updated_file_name, index=False)

def get_dataframes_from_csv_files(csv1, csv2):
  """
    Reads two CSV files and returns their corresponding pandas DataFrames.

    Args:
        csv1 (str): Path to the first CSV file.
        csv2 (str): Path to the second CSV file.

    Returns:
        tuple: A tuple containing two pandas DataFrames (df1, df2) corresponding to csv1 and csv2.

    Prints:
        - The number of rows and columns in each CSV file.
    """
  df1 = pd.read_csv(csv1)
  df2 = pd.read_csv(csv2)

  print(f'CSV1 has {df1.shape[0]} rows and {df1.shape[1]} columns.')
  print(f'CSV1 has {df2.shape[0]} rows and {df2.shape[1]} columns.')

  return df1, df2

def map_creative_name_to_flights(df1, df2, column_to_merge_on):
  """
  Merges two DataFrames on a specified column and selects relevant columns for output.

  Args:
      df1 (pd.DataFrame): The first DataFrame containing flight data.
      df2 (pd.DataFrame): The second DataFrame containing additional data to merge.
      column_to_merge_on (str): The column name on which to perform the merge.

  Returns:
      pd.DataFrame: A new DataFrame with a 'Creative Name' columns after merging df1 and df2.

  Prints:
      - The final column names of the resulting DataFrame after selection and renaming.

  The function performs the following operations:
      1. Merges `df1` and `df2` using a left join on `column_to_merge_on`, 
          adding suffixes `_actual` and `_prediction` to overlapping column names.
      2. Selects a predefined subset of columns from the merged DataFrame.
      3. Renames specific columns by removing the `_actual` suffix.
      4. Prints the final column names before returning the modified DataFrame.
  """
  temp_df = pd.merge(df1, df2, on=[column_to_merge_on], how='left', suffixes=('_actual', '_prediction'))

  temp_columns = ['Flight ID',
                 'Target Name',
                 'Target ID',
                 'Ad Name',
                 'Package Name_actual',
                 'Ad ID_actual',
                 'Target Type',
                 'Ad Type_actual',
                 'Ad Type Name',
                 'Ad Unit_actual',
                 'Ad Unit Name',
                 'Ad Dimensions',
                 'Start Date',
                 'End Date',
                 'Run Time',
                 'Budget',
                 'Paused',
                 'Completed',
                 'Creative Name']

  temp_df = temp_df[temp_columns].copy()

  temp_df = temp_df.rename(columns={
    'Package Name_actual': 'Package Name',
    'Ad ID_actual': 'Ad ID',
    'Ad Type_actual': 'Ad Type',
    'Ad Unit_actual': 'Ad Unit'
  })

  print(f'Column names: {temp_df.columns.to_list()}')
  return temp_df

def update_column_names(df, column_names, new_column_names):
  """
  Renames specified columns in a DataFrame.

  Args:
      df (pd.DataFrame): The DataFrame whose columns need to be renamed.
      column_names (list of str): A list of existing column names to be replaced.
      new_column_names (list of str): A list of new column names corresponding to `column_names`.

  Returns:
      pd.DataFrame: A new DataFrame with updated column names.

  The function creates a mapping dictionary from `column_names` to `new_column_names`
  and applies the renaming operation in a single step.
  """
  rename_mapping = dict(zip(column_names, new_column_names))
  return df.rename(columns=rename_mapping)

def clean_and_convert_budgets(df, columns_to_process):
  """
  Cleans and converts specified budget columns in a DataFrame to numeric format.

  Args:
      df (pd.DataFrame): The DataFrame containing budget columns as strings.
      columns_to_process (list of str): A list of column names to be cleaned and converted.

  Returns:
      pd.DataFrame: The DataFrame with specified columns converted to numeric values.

  The function performs the following operations on each column in `columns_to_process`:
      1. Removes whitespace, dollar signs (`$`), commas (`,`), and hyphens (`-`) from the values.
      2. Converts the cleaned strings to numeric values, coercing errors to NaN.
  """
  for column in columns_to_process:
    df[column] = df[column].str.replace(r'[\s\$-,]', '', regex=True)
    df[column] = pd.to_numeric(df[column], errors='coerce')

  return df

def create_new_budget_columns(df, columns_to_process):
  """
  Creates new budget-related columns in the DataFrame.

  Args:
      df (pd.DataFrame): The DataFrame containing budget columns to be processed.
      columns_to_process (list of str): A list of column names for which new columns will be created.

  Returns:
      pd.DataFrame: The DataFrame with new columns added.

  The function performs the following operations for each column in `columns_to_process`:
      1. Creates a new column `Header_<col>` that stores the original column name as its value.
      2. Creates a new column `Values_<col>` that stores the original column values.
  """
  for col in columns_to_process:
    df[f'Header_{col}'] = col
    df[f'Values_{col}'] = df[col]

  return df

def process_dataframes(df, column_names, new_column_names, columns_to_process):
  """
  Processes a DataFrame by renaming columns, cleaning budget data, and creating new budget-related columns.

  Args:
      df (pd.DataFrame): The input DataFrame to be processed.
      column_names (list of str): A list of existing column names to be replaced.
      new_column_names (list of str): A list of new column names corresponding to `column_names`.
      columns_to_process (list of str): A list of columns that need budget cleaning and transformation.

  Returns:
      pd.DataFrame: The processed DataFrame with renamed columns, cleaned budget values, and additional budget-related columns.

  The function performs the following steps:
      1. Renames columns in `df` using `update_column_names`.
      2. Cleans and converts budget-related columns to numeric format using `clean_and_convert_budgets`.
      3. Creates new budget-related columns using `create_new_budget_columns`.
  """
  df = update_column_names(df, column_names=column_names, new_column_names=new_column_names)
  df = clean_and_convert_budgets(df, columns_to_process=new_column_names)
  df = create_new_budget_columns(df, columns_to_process=new_column_names)

  return df

def get_num_flights(df, package, start_date):
  """
  Returns the number of flights for a given package and start date.

  Args:
      df (pd.DataFrame): The DataFrame containing flight data.
      package (str): The package name to filter flights.
      start_date (str): The start date to filter flights.

  Returns:
      int: The number of flights that match the specified package and start date.

  The function filters the DataFrame based on `Package Name` and `Start Date` 
  and counts the number of occurrences in the `Budget` column.
  """
  num_flights = df.loc[
            (df['Package Name'] == package) &
            (df['Start Date'] == start_date), 'Budget'
          ].count()

  return num_flights

def get_budget(df, package, start_date):
  """
  Calculates the total budget for a given package and start date.

  Args:
      df (pd.DataFrame): The DataFrame containing budget data.
      package (str): The package name to filter the data.
      start_date (str): The start date to filter the data.

  Returns:
      float or int: The total budget for the specified package and start date.

  The function constructs dynamic column names (`Header_<start_date>` and `Values_<start_date>`)
  and filters the DataFrame based on `Package Name` and `Header_<start_date>`. 
  It then sums the values from the corresponding `Values_<start_date>` column.
  """
  header_start_date = 'Header_' + start_date
  value_start_date = 'Values_' + start_date

  budget = df.loc[
    (df['Package Name'] == package) &
    (df[header_start_date] == start_date), value_start_date
  ].sum()

  return budget

def update_flight_level_budgets(df, package, start_date, flight_level_budget):
  """
  Updates the budget for flights that match a given package and start date.

  Args:
      df (pd.DataFrame): The DataFrame containing flight data.
      package (str): The package name to filter flights.
      start_date (str): The start date to filter flights.
      flight_level_budget (float or int): The new budget value to assign.

  Returns:
      pd.DataFrame: The updated DataFrame with modified budget values.

  The function locates rows in `df` where `Package Name` matches `package` 
  and `Start Date` matches `start_date`, then updates the `Budget` column 
  with `flight_level_budget`.
  """
  df.loc[
      (df['Package Name'] == package) &
      (df['Start Date'] == start_date), 'Budget'
    ] = flight_level_budget

  return df

def update_budgets(df1, df2, packages_to_process, start_dates):
  """
  Updates flight-level budgets based on aggregated package-level budgets.

  Args:
      df1 (pd.DataFrame): The DataFrame containing flight-level data where budgets will be updated.
      df2 (pd.DataFrame): The DataFrame containing package-level budget data.
      packages_to_process (list of str): A list of package names to process.
      start_dates (list of str): A list of start dates to process.

  Returns:
      pd.DataFrame: The updated `df1` DataFrame with modified flight-level budgets.

  The function performs the following steps:
      1. Identifies unique packages and start dates from the input lists.
      2. Iterates through each unique package and start date.
      3. Retrieves the number of flights (`get_num_flights`) and total budget (`get_budget`) for each package-start date combination.
      4. Calculates the flight-level budget by dividing the total budget by the number of flights.
      5. Updates the budget values in `df1` using `update_flight_level_budgets`.

  Note:
      - If `num_flights` is zero, a division by zero error may occur.
      - Ensure `get_num_flights` and `get_budget` return valid values before performing the division.
  """
  unique_packages = list(set(packages_to_process))
  unique_start_dates = list(set(start_dates))

  for package in unique_packages:

    for start_date in unique_start_dates:

      num_flights = get_num_flights(df1, package, start_date)
      budget = get_budget(df2, package, start_date)
      flight_level_budget = budget / num_flights

      update_flight_level_budgets(df1, package, start_date, flight_level_budget)

  return df1
