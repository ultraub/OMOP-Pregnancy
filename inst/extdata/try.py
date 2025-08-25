import pandas as pd
concepts = pd.read_csv("./pps_concepts.csv")
print(concepts["min_month"].unique())
print(concepts["min_month"].value_counts())
print(concepts["max_month"].unique())
print(concepts["max_month"].value_counts())