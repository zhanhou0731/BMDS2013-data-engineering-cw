#author: Desmond Oon Kai Quan
from pyspark.sql.functions import col, concat_ws, to_timestamp, dayofweek, hour, when, avg, year, month, dayofmonth

class DataProcessor:
    def __init__(self):
        self.column_renames = {
            "Date": "date", 
            "Time": "time", 
            "COGT": "carbon_monoxide",
            "C6H6GT": "benzene", 
            "NOxGT": "nitrogen_oxides", 
            "NO2GT": "nitrogen_dioxide",
            "NMHCGT": "non_methane_hydrocarbon"
        }
        self.sensor_columns = [
            "carbon_monoxide", 
            "benzene", 
            "nitrogen_oxides", 
            "nitrogen_dioxide", 
            "non_methane_hydrocarbon"
        ]

    def clean_and_transform(self, df):
        print("Starting data processing pipeline...")
        df_cleaned = self._initial_cleaning(df)
        df_renamed = self._rename_columns(df_cleaned)
        df_nulls = self._handle_missing_values(df_renamed)
        df_imputed = self._impute_missing_values(df_nulls)
        df_final = self._enrich_data(df_imputed)
        print("Data processing pipeline complete.")
        return df_final

    def _initial_cleaning(self, df):
        print("- Step 1: Dropping duplicates and irrelevant columns...")
        return df.dropDuplicates().drop(
            "PT08S1CO", 
            "PT08S2NMHC", 
            "PT08S3NOx", 
            "PT08S4NO2", 
            "PT08S5O3", 
            "T", 
            "RH", 
            "AH"
        )

    def _rename_columns(self, df):
        print("- Step 2: Renaming columns...")
        df_renamed = df
        for old_name, new_name in self.column_renames.items():
            df_renamed = df_renamed.withColumnRenamed(old_name, new_name)
        return df_renamed

    def _handle_missing_values(self, df):
        print("- Step 3: Replacing -200 with null...")
        df_nulls = df
        for col_name in self.sensor_columns:
            df_nulls = df_nulls.withColumn(col_name, when(col(col_name) != -200, col(col_name)).otherwise(None))
        return df_nulls

    def _impute_missing_values(self, df):
        print("- Step 4: Imputing nulls with column averages...")
        imputation_values = {}
        for col_name in self.sensor_columns:
            mean_val = df.select(avg(col(col_name))).first()[0]
            imputation_values[col_name] = 0.0 if mean_val is None else mean_val
        return df.fillna(imputation_values)

    def _enrich_data(self, df):
        print("- Step 5: Enriching with timestamp and time features...")
        df_enriched = df.withColumn(
            "event_timestamp",
            to_timestamp(concat_ws(" ", col("date"), col("time")), "dd/MM/yyyy HH.mm.ss")
        ).drop("date", "time")
        
        df_enriched = df_enriched.withColumn("year", year(col("event_timestamp")))
        df_enriched = df_enriched.withColumn("month", month(col("event_timestamp")))
        df_enriched = df_enriched.withColumn("day", dayofmonth(col("event_timestamp")))
        df_enriched = df_enriched.withColumn("day_of_week", dayofweek(col("event_timestamp")))
        df_enriched = df_enriched.withColumn("hour_of_day", hour(col("event_timestamp")))
        return df_enriched
