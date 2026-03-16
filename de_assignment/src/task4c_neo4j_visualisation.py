#author: Seow Zhan Hou
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from task4b_neo4j_query import AirQualityQuery


class Neo4jVisualisation():

    def __init__(self):
        self.dashboard = AirQualityQuery()

    def q1_visualisation(self):
        q1_df = self.dashboard.df_query_1()
        pandas_q1_pdf = q1_df.toPandas()
        heatmap_data = pandas_q1_pdf.pivot(index="Pollutant", columns="HourOfDay", values="NormalizedValue")

        plt.figure(figsize=(14,6))
        sns.heatmap(heatmap_data, annot=True, cmap="YlOrRd", cbar_kws={'label': 'Normalized Level'})
        plt.title("Average Pollution Level by Hour")
        plt.xlabel("Hour of Day")
        plt.ylabel("Pollutant")
        plt.show()

    def q2_visualisation(self):
        q2_df = self.dashboard.df_query_2()
        pandas_q2_df = q2_df.toPandas()
        
        plt.figure(figsize=(8,4))
        
        for pollutant in pandas_q2_df["Pollutant"].unique():
            subset = pandas_q2_df[pandas_q2_df["Pollutant"] == pollutant]
            plt.plot(
                subset["DayOfWeek"],
                subset["NormalizedValue"],
                marker="o",
                label=pollutant
            )
        
        plt.title("Normalized Pollution Levels by Day of Week")
        plt.xlabel("Day of Week")
        plt.ylabel("Pollution Level (0–1)")
        plt.legend()
        plt.grid(True)
        plt.show()

