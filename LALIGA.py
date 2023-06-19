from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("LALIGA").getOrCreate()

# Đường dẫn đến các file dữ liệu
data_files = [
    # "data/SP1_2017_2018.csv",
    "data/SP1_2019_2020.csv",
    "data/SP1_2020_2021.csv"
]

# Đọc dữ liệu từ các file và tạo DataFrame
dataframes = []
for file_path in data_files:
    dataframe = spark.read.format("csv").option("header", "true").load(file_path)
    dataframes.append(dataframe)

# Gom tất cả các DataFrame lại với nhau
combined_dataframe = dataframes[0]
for i in range(1, len(dataframes)):
    combined_dataframe = combined_dataframe.unionAll(dataframes[i])

# Chọn các cột cần thiết
selected_columns = ["AwayTeam", "AST", "FTR"]
filtered_dataframe = combined_dataframe.select(selected_columns)

# Lọc các trận thắng trên sân khách và tính số lượng cú sút trúng đích
away_wins_dataframe = filtered_dataframe.filter(col("FTR") == "A")
team_stats_dataframe = away_wins_dataframe.groupBy("AwayTeam").agg({"AST": "sum"}).withColumnRenamed("sum(AST)", "TotalAST")
team_stats_dataframe = team_stats_dataframe.orderBy(col("TotalAST").desc()).limit(3)

# In kết quả
team_stats_dataframe.show()

# Lưu kết quả vào file
team_stats_dataframe.write.mode("overwrite").csv("data/laliga/output.csv")

# Dừng SparkSession
spark.stop()
