from prefect import task, flow, get_run_logger
import pandas as pd
import matplotlib.pyplot as plt


@task
def fetch_data():
    logger = get_run_logger()
    logger.info("Reading data...")

    file_path = "/home/hamna/ai601-data-engineering/labs/lab6/analytics_data.csv"
    df = pd.read_csv(file_path)

    logger.info(f"Data shape: {df.shape}")
    return df


@task
def validate_data(df):
    logger = get_run_logger()
    logger.info("Validating data...")
    missing_values = df.isnull().sum()
    logger.info(f"Missing values:\n{missing_values}")
    df_clean = df.dropna()
    return df_clean

@task
def transform_data(df_clean):
    logger = get_run_logger()
    logger.info("Transforming data...")
    if "sales" in df_clean.columns:
        df_clean["sales_normalized"] = (df_clean["sales"] - df_clean["sales"].mean()) / df_clean["sales"].std()
    return df_clean

@task
def generate_report(df_clean):
    logger = get_run_logger()
    summary = df_clean.describe()
    summary.to_csv("summary.csv")
    logger.info("Summary statistics saved to data summary.csv")

@task
def create_histogram(df_clean):
    logger = get_run_logger()
    if "sales" in df_clean.columns:
        plt.hist(df_clean["sales"], bins=20)
        plt.title("Sales Distribution")
        plt.xlabel("Sales")
        plt.ylabel("Frequency")
        plt.savefig("/home/hamna/ai601-data-engineering/labs/lab6/sales_histogram.png")
        plt.close()
        logger.info("Sales histogram saved to  sales_histogram.png")

@flow
def analytics_pipeline():
    df = fetch_data()
    df_clean = validate_data(df)
    df_transformed = transform_data(df_clean)
    generate_report(df_transformed)
    create_histogram(df_transformed)

if __name__ == "__main__":
    analytics_pipeline()

