import kagglehub
import pandas as pd
import os

# 1. 下载数据集
# path = kagglehub.dataset_download("dhrubangtalukdar/qs-world-university-rankings-2026-top-1500")
# path = kagglehub.dataset_download("akashbommidi/2026-qs-world-university-rankings")
path = kagglehub.dataset_download("zulqarnain11/global-university-rankings-2025-2026")

print("数据集已下载至:", path)

# 2. 找到文件夹里的 csv 文件
files = os.listdir(path)
csv_file = [f for f in files if f.endswith('.csv')][0]
full_path = os.path.join(path, csv_file)

# 3. 读取并打印前 5 行看看
df = pd.read_csv(full_path)
print("\n--- 数据预览 ---")
print(df.head()) 

# 4. (可选) 如果你想把文件挪到你当前的项目目录下，方便以后直接查看：
import shutil
shutil.copy(full_path, "./data/qs_data_architecture.csv")