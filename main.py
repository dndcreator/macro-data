import pandas as pd
import yfinance as yf
from datetime import datetime
import os
import boto3
from io import BytesIO

# 定义需要获取的标的
TICKERS = {
    # 美国国债收益率
    "US 3M Yield": "^IRX",  # 美国3个月国债收益率
    "US 5Y Yield": "^FVX",  # 美国5年国债收益率
    "US 10Y Yield": "^TNX",  # 美国10年国债收益率
    "US 30Y Yield": "^TYX",  # 美国30年国债收益率

    # 大宗商品
    "Gold": "GC=F",  # 黄金
    "Silver": "SI=F",  # 白银
    "Copper": "HG=F",  # 铜
    "Crude Oil": "CL=F",  # 原油
    "Natural Gas": "NG=F",  # 天然气
    "Corn": "ZC=F",  # 玉米
    "Wheat": "ZW=F",  # 小麦
    "Soybean": "ZS=F",  # 大豆

    # 全球主要股票指数
    "S&P 500": "^GSPC",  # 标普500
    "Dow Jones": "^DJI",  # 道琼斯指数
    "NASDAQ": "^IXIC",  # 纳斯达克指数
    "FTSE 100": "^FTSE",  # 富时100指数
    "DAX": "^GDAXI",  # 德国DAX指数
    "Nikkei 225": "^N225",  # 日经225指数
    "Shanghai Composite": "000001.SS",  # 上证指数

    # 波动率指数
    "VIX": "^VIX",  # 恐慌指数
}

# 初始化 S3 客户端
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
)

# S3 配置
S3_BUCKET_NAME = "chian-macro-data"  # 替换为你的 S3 桶名称
S3_FILE_NAME = "macroresult.xlsx"  # S3 中的文件名

# 获取数据
def get_market_data():
    data = {}
    for name, ticker in TICKERS.items():
        try:
            # 从雅虎财经获取数据
            ticker_data = yf.Ticker(ticker)
            latest_price = ticker_data.history(period="1d")["Close"].iloc[-1]
            data[name] = latest_price
        except Exception as e:
            print(f"获取 {name} 数据失败: {e}")
            data[name] = None
    return data

# 保存数据到 S3
def save_to_s3(df, bucket_name, file_name):
    with BytesIO() as buffer:
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            df.to_excel(writer)
        buffer.seek(0)
        s3.upload_fileobj(buffer, bucket_name, file_name)

# 从 S3 读取数据
def read_from_s3(bucket_name, file_name):
    try:
        with BytesIO() as buffer:
            s3.download_fileobj(bucket_name, file_name, buffer)
            buffer.seek(0)
            df = pd.read_excel(buffer, index_col=0)
        return df
    except Exception as e:
        print(f"从 S3 读取数据失败: {e}")
        return None

# 主函数
def main():
    print("开始获取数据...")
    # 获取数据
    market_data = get_market_data()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 整理数据
    new_row = pd.DataFrame([market_data], index=[timestamp])

    # 从 S3 读取旧数据
    existing_data = read_from_s3(S3_BUCKET_NAME, S3_FILE_NAME)
    if existing_data is not None:
        updated_data = pd.concat([existing_data, new_row])
    else:
        updated_data = new_row

    # 保存更新后的数据到 S3
    save_to_s3(updated_data, S3_BUCKET_NAME, S3_FILE_NAME)

    print(f"数据已更新并保存到 S3: s3://{S3_BUCKET_NAME}/{S3_FILE_NAME}")

if __name__ == "__main__":
    main()