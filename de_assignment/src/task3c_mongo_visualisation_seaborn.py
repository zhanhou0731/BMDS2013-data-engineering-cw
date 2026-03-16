#author: Phung Jian Thong
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from IPython.display import display

class MongoVisualizer:
    def __init__(self, analyzer):
        self.analyzer = analyzer
        sns.set_theme(style="whitegrid")

    def plot_daily_analysis(self):
        print("--- Visualizations for Query 1: Daily Pollution Levels  ---")
        daily_spark_df = self.analyzer.daily_analysis_df()
        df = daily_spark_df.toPandas()

        if df.empty:
            print("No data returned from the daily analysis query.")
            return

        day_order = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
        df['day_name'] = pd.Categorical(df['day_name'], categories=day_order, ordered=True)
        df = df.sort_values('day_name')
        
        display(df)

        fig, axes = plt.subplots(3, 1, figsize=(12, 18), sharex=True)
        fig.suptitle('Daily Average Pollution Levels', fontsize=16, y=0.92)

        sns.lineplot(ax=axes[0], data=df, x='day_name', y='avg_carbon_monoxide', color='#EF5350', marker='o')
        axes[0].set_title('Average Carbon Monoxide by Day')
        axes[0].set_xlabel('')
        axes[0].set_ylabel('Average CO')

        sns.lineplot(ax=axes[1], data=df, x='day_name', y='avg_benzene', color='#42A5F5', marker='o')
        axes[1].set_title('Average Benzene by Day')
        axes[1].set_xlabel('')
        axes[1].set_ylabel('Average Benzene')

        sns.lineplot(ax=axes[2], data=df, x='day_name', y='avg_nitrogen_dioxide', color='#66BB6A', marker='o')
        axes[2].set_title('Average Nitrogen Dioxide by Day')
        axes[2].set_xlabel('Day of the Week')
        axes[2].set_ylabel('Average NO2')
        
        plt.xticks(rotation=45)
        plt.tight_layout(rect=[0, 0, 1, 0.9])
        plt.show()

    def plot_hourly_analysis(self):
        print("\n--- Visualization for Query 2: Top 5 Most Polluted Hours (Seaborn) ---")
        hourly_spark_df = self.analyzer.hourly_analysis_df()
        df = hourly_spark_df.toPandas()

        if df.empty:
            print("No data returned from the hourly analysis query.")
            return
            
        df_renamed = df.rename(columns={
            "hour_of_day": "Hour of Day",
            "avg_carbon_monoxide": "Avg CO",
            "peak_carbon_monoxide": "Peak CO"
        }).sort_values(by="Hour of Day")

        display(df_renamed)

        df_melted = df_renamed.melt(id_vars="Hour of Day", value_vars=["Avg CO", "Peak CO"],
                                    var_name="Metric", value_name="Level")

        plt.figure(figsize=(10, 6))
        sns.barplot(data=df_melted, x='Hour of Day', y='Level', hue='Metric', palette='viridis')
        
        plt.title('Pollution Hotspots: Top 5 Most Polluted Hours', fontsize=14)
        plt.xlabel('Hour of Day')
        plt.ylabel('Pollution Level')
        plt.xticks(rotation=0)
        plt.legend(title='Metric')
        plt.tight_layout()
        plt.show()