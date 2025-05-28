from pyspark.sql import SparkSession
from functools import reduce
import os
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import time
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.api import VAR
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error
from typing import Union
import tensorflow as tf
from tensorflow.keras import layers
from keras.layers import Dense
from tensorflow.keras.callbacks import EarlyStopping
import torch
from torch import nn
import warnings
warnings.filterwarnings('ignore')
import logging
from varnn_model import VARNN

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("Training_VARNN")

SOURCE_CATALOG = "datalake"
SOURCE_NAMESPACE = f"{SOURCE_CATALOG}.gold"
TARGET_CATALOG = "datalake"
TARGET_NAMESPACE = f"{TARGET_CATALOG}.gold"
FINANCIAL_COLUMNS = ["price"]
DATE_COLUMN = "date"
BASE_PREDICT_PATH = '/src/data/predict'
BASE_FUTURE_PATH = '/src/data/future'
BASE_MODEL_PATH = '/src/model'
today = datetime.today().date()
today = today.strftime('%d_%m_%Y')
START_DATE = '05-01-1995'
start_date_dt = datetime.strptime(START_DATE, '%d-%m-%Y')


def create_spark_session(app_name: str) -> SparkSession:
    """Initialize Spark session with required configurations"""
    logger.info("Initializing Spark Session...")
    spark = (
        SparkSession.builder.appName(app_name)
        .enableHiveSupport()
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.avro.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
            "software.amazon.awssdk:s3:2.20.130,"
            "software.amazon.awssdk:auth:2.20.130,"
            "software.amazon.awssdk:regions:2.20.130,"
            "software.amazon.awssdk:sts:2.20.130,"
            "software.amazon.awssdk:kms:2.25.30,"
            "software.amazon.awssdk:glue:2.25.30,"
            "software.amazon.awssdk:dynamodb:2.20.130" 
        )
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access-key-id", "admin")
        .config("spark.hadoop.fs.s3a.secret-access-key", "admin123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.sql.catalog.datalake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.datalake.type", "rest")
        .config("spark.sql.catalog.datalake.uri", "http://nessie:19120/iceberg")
        .config("spark.sql.catalog.datalake.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    logger.info(f"Spark Session initialized successfully. Spark version: {spark.version}")
    
    # Log important configs
    conf = spark.sparkContext.getConf()
    s3_endpoint = conf.get("spark.hadoop.fs.s3a.endpoint", "N/A")
    catalog_type = conf.get(f"spark.sql.catalog.{TARGET_CATALOG}.type", "N/A")
    catalog_uri = conf.get(f"spark.sql.catalog.{TARGET_CATALOG}.uri", "N/A")
    warehouse = conf.get(f"spark.sql.catalog.{TARGET_CATALOG}.warehouse", "N/A")
    
    logger.info(f"Source Namespace: {SOURCE_NAMESPACE}")
    logger.info(f"Target Namespace: {TARGET_NAMESPACE}")
    logger.info(f"S3 Endpoint (Hadoop): {s3_endpoint}")
    logger.info(f"Catalog Type: {catalog_type}")
    logger.info(f"Catalog URI: {catalog_uri}")
    logger.info(f"Catalog Warehouse: {warehouse}")
    
    return spark

def create_namespace_if_not_exists(spark: SparkSession, namespace: str):
    """Create Iceberg namespace if it doesn't exist"""
    try:
        logger.info(f"Checking/Creating namespace: {namespace}")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
        logger.info(f"Namespace {namespace} ensured.")
    except Exception as e:
        logger.error(f"Failed to create namespace {namespace}: {str(e)}")

# Read silver tables

def read_table(spark: SparkSession, table_name: str) -> pd.DataFrame:
    try:
        logger.info(f"Reading table: {table_name}")
        df = spark.table(table_name)
        logger.info("Table schema:")
        df.printSchema()
        pdf = df.orderBy(DATE_COLUMN).toPandas()
        logger.info("Table successfully converted to Pandas DataFrame")
        return pdf
    except Exception as e:
        logger.error(f"Error reading table {table_name}: {e}")
        raise

def scale_data(train_df: pd.DataFrame, test_df: pd.DataFrame):
    try:
        scaled_train = train_df.copy()
        scalers = {}
        for col in train_df.columns:
            scaler = MinMaxScaler(feature_range=(-1, 1))
            scaled_train[col] = scaler.fit_transform(train_df[[col]]).flatten()
            scalers[col] = scaler
        logger.info("Training data scaled successfully")

        scaled_test = test_df.copy()
        for col in test_df.columns:
            scaler = scalers[col]
            scaled_test[col] = scaler.transform(test_df[[col]]).flatten()
        logger.info("Testing data scaled successfully")

        return scaled_train, scaled_test, scalers
    except Exception as e:
        logger.error(f"Error in scaling data: {e}")
        raise

def split_the_data(data: pd.DataFrame):
    try:
        limit = int(len(data) * 0.8)
        drop_cols = ['date', 'year', 'month', 'id']
        train_df = data.iloc[:limit].drop(columns=[c for c in drop_cols if c in data.columns])
        test_df = data.iloc[limit:].drop(columns=[c for c in drop_cols if c in data.columns])
        logger.info(f'Train shape: {train_df.shape}, Test shape: {test_df.shape}')
        scaled_train, scaled_test, scalers = scale_data(train_df, test_df)
        logger.info('Data split and scaled successfully')
        return scaled_train, scaled_test, scalers
    except Exception as e:
        logger.error(f"Error splitting data: {e}")
        raise

def ADF_test(data: pd.DataFrame) -> pd.DataFrame:
    statistic = []
    p_value = []
    for col in data.columns:
        result = adfuller(data[col])
        statistic.append(result[0])
        p_value.append(round(result[1], 5))
    return pd.DataFrame({'Statistic': statistic, 'P-value': p_value}, index=data.columns)

def checking_ADF(adf_results: pd.DataFrame) -> bool:
    return all(p <= 0.05 for p in adf_results['P-value'])

def difference(dataset, order):
    return dataset.diff(periods=order).dropna()

def optimize_VAR(endog: Union[pd.DataFrame, list]) -> pd.DataFrame:
    results = []
    for p in range(1, 30):
        try:
            model = VAR(endog)
            fitted_model = model.fit(maxlags=p, ic=None)
            results.append([p, fitted_model.aic])
        except Exception as e:
            logger.warning(f"VAR fit failed at lag {p}: {e}")
            continue
    result_df = pd.DataFrame(results, columns=['p', 'AIC']).sort_values(by='AIC').reset_index(drop=True)
    return result_df

def to_sequences_multivariate(dataset: pd.DataFrame, p: int):
    x, y = [], []
    for i in range(p, len(dataset)):
        x.append(dataset.iloc[i - p:i, :].values)
        y.append(dataset.iloc[i:i + 1, :].values)
    return np.array(x), np.array(y).reshape(len(y), dataset.shape[1])

def train_model(var_weights, var_bias, no_columns, p, trainX, trainY):
    try:
        VARNN_model = VARNN(var_weights, var_bias, no_columns * p, p, no_columns)
        VARNN_model.compile(optimizer='adam', loss='mse')

        # Define EarlyStopping callback
        early_stopping = EarlyStopping(
            monitor='val_loss',   # Monitor validation loss
            patience=5,          # Number of epochs with no improvement after which training will be stopped
            restore_best_weights=True,  # Restore model weights from the epoch with the best value of the monitored quantity
            verbose=1
        )

        start = time.time()
        history = VARNN_model.fit(
            trainX, trainY,
            epochs=100,
            batch_size=32,
            validation_split=0.2,
            verbose=0,
            callbacks=[early_stopping]  # Add the callback here
        )
        end = time.time()

        logger.info(f"Model trained in {end - start:.2f} seconds")
        return VARNN_model, history
    except Exception as e:
        logger.error(f"Training failed: {e}")
        raise

def reverse_difference(original_data, differenced_data, order=1):
    last_values = original_data.iloc[-len(differenced_data) - order:-order].values
    restored = differenced_data + last_values
    return restored

def evaluate_data(original, predicted, columns):
    for i, col in enumerate(columns):
        mse = mean_squared_error(original[:, i], predicted[:, i])
        rmse = np.sqrt(mse)
        mae = mean_absolute_error(original[:, i], predicted[:, i])
        logger.info(f"{col} -- MSE: {mse:.5f}, RMSE: {rmse:.5f}, MAE: {mae:.5f}")
    total_mse = mean_squared_error(original, predicted)
    total_rmse = np.sqrt(total_mse)
    total_mae = mean_absolute_error(original, predicted)
    logger.info(f"Total -- MSE: {total_mse:.5f}, RMSE: {total_rmse:.5f}, MAE: {total_mae:.5f}")

def unscale(data, train_df: pd.DataFrame, scalers):
    temp = data.copy()
    for index, i in enumerate(train_df.columns):
        scaler = scalers[i]
        temp[:, index] = scaler.inverse_transform(np.reshape(data[:, index], (-1, 1))).flatten()
    return temp

def predict_future(model, last_window, p, num_steps):
    predictions = []
    current_window = last_window.copy()  # shape (p, num_vars)

    for _ in range(num_steps):
        input_seq = current_window[np.newaxis, ...]  # shape (1, p, num_vars)
        pred = model.predict(input_seq, verbose=0)[0]  # shape (num_vars,)
        predictions.append(pred)
        
        # Slide the window: drop the oldest, append the new prediction
        current_window = np.vstack([current_window[1:], pred])

    return np.array(predictions)

def main(table_name: str):
    """Main function to orchestrate the ETL process"""
    spark = None
    start_time = datetime.now()
    logger.info(f"Starting Training VARNN model for {table_name}")
    
    try:
        # Initialize Spark
        spark = create_spark_session(f"Training VARNN for {table_name}")
        
        # Ensure target namespace exists
        create_namespace_if_not_exists(spark, SOURCE_NAMESPACE)
        create_namespace_if_not_exists(spark, TARGET_NAMESPACE)
        
        # List folders to process
        try:
            data = read_table(spark, f'{SOURCE_NAMESPACE}.{table_name}')
        except Exception as e:
            logger.error(f"Could not read data: {str(e)}")
            return

        scaled_train, scaled_test, scalers = split_the_data(data)
                
        i = 0
        train_diff = scaled_train
        while not checking_ADF(ADF_test(train_diff)):
            i += 1
            train_diff = difference(train_diff, order=i)

        logger.info('Difference data')
        train_diff = difference(scaled_train, order=i)
        test_diff = difference(scaled_test, order=i)

        VAR_p_df = optimize_VAR(train_diff)
        
        p = int(VAR_p_df.iloc[0, 0])
        no_columns = len(train_diff.columns)
        logger.info(f'Finding optimal p: {p}')

        strategy = tf.distribute.MirroredStrategy()
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        trainX, trainY = to_sequences_multivariate(train_diff, p)
        testX, testY = to_sequences_multivariate(test_diff, p)
        logger.info('Finish slice the data into sequences')

        var_model = VAR(train_diff)
        var_results = var_model.fit(p)
        var_coefficients = var_results.params

        var_bias = var_coefficients.loc['const'].values
        var_weights = var_coefficients.drop('const').values

        logger.info('Start training model')
        VARNN_model, history = train_model(var_weights, var_bias, no_columns, p, trainX, trainY)

        logger.info('Finished predict test data')
        pred_VARNN = VARNN_model.predict(testX)

        # Reverse the differencing
        last_test_values = scaled_test.iloc[p:]
        testY_original = reverse_difference(last_test_values, testY)
        pred_VARNN_original = reverse_difference(last_test_values, pred_VARNN)

        evaluate_data(testY_original, pred_VARNN_original, scaled_test.columns)

        unscaled_VARNN = unscale(pred_VARNN_original, scaled_train, scalers)
        originY = unscale(testY_original, scaled_train, scalers)
        evaluate_data(originY, unscaled_VARNN, scaled_test.columns)

        days_to_add = len(scaled_train)
        predict_date = start_date_dt + timedelta(days=days_to_add)
        future_date = datetime.today() + timedelta(days=1)

        # Convert both dates to dd-mm-yyyy format
        PREDICT_DATE = predict_date.strftime('%d-%m-%Y')
        FUTURE_DATE = future_date.strftime('%d-%m-%Y')

        VAR_predict = pd.DataFrame(originY, columns=scaled_test.columns)
        start_date = pd.to_datetime(PREDICT_DATE, dayfirst=True)
        VAR_predict["date"] = pd.date_range(start=start_date, periods=len(VAR_predict), freq='D')

        predict_path = os.path.join(BASE_PREDICT_PATH, 'VARNN', f'{table_name}_predict_{today}.csv')

        VAR_predict.to_csv(predict_path, index=False)

        # Get the last `p` rows from training or testing data
        last_window = test_diff.iloc[-p:].values  # shape: (p, num_vars)

        # Predict p steps into the future
        logger.info('Finished future predict')
        future_preds = predict_future(VARNN_model, last_window, p=p, num_steps=p)
        future_preds = unscale(reverse_difference(last_test_values, future_preds), scaled_train, scalers)


        future_VARNN = pd.DataFrame(future_preds, columns=scaled_test.columns)
        start_date = pd.to_datetime(FUTURE_DATE, dayfirst=True)
        future_VARNN["date"] = pd.date_range(start=start_date, periods=len(future_VARNN), freq='D')

        full_path = os.path.join(BASE_FUTURE_PATH, 'VARNN', f'{table_name}_future_{today}.csv')

        future_VARNN.to_csv(full_path, index=False)
    except Exception as e:
        logger.critical(f"Job failed with critical error: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()
            end_time = datetime.now()
            logger.info(f"Spark Session stopped. Total job duration: {end_time - start_time}")