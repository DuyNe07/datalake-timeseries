from pyspark.sql import SparkSession
from functools import reduce
import os
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import time
from statsmodels.tsa.stattools import adfuller, grangercausalitytests
from statsmodels.tsa.api import VAR
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from typing import Union
import tensorflow as tf
from keras.models import Sequential
from tensorflow.keras import layers
from keras.layers import Dense
import torch
from torch import nn, optim
import warnings
warnings.filterwarnings('ignore')
from copy import deepcopy
from sklearn.model_selection import train_test_split
import logging
from srvar_model import SrVAR

SOURCE_CATALOG = "datalake"
SOURCE_NAMESPACE = f"{SOURCE_CATALOG}.gold"
TARGET_CATALOG = "datalake"
TARGET_NAMESPACE = f"{TARGET_CATALOG}.gold"
FINANCIAL_COLUMNS = ["price"]
DATE_COLUMN = "date"
BASE_PREDICT_PATH = '/src/data/predict'
BASE_FUTURE_PATH = '/src/data/future'
BASE_MODEL_PATH = '/src/airflow/model'
BASE_EVAL_PATH = '/src/data/evaluation'
today = datetime.today().date()
today = today.strftime('%d_%m_%Y')
START_DATE = '05-01-1995'
start_date_dt = datetime.strptime(START_DATE, '%d-%m-%Y')
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("Training_SrVAR")

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

def train_srvarm(model, trainX, trainY, valX, valY, num_epochs=100, batch_size=32, lr=0.001, tau=1.0, verbose=0, patience=5):
    optimizer = optim.Adam(model.parameters(), lr=lr)
    mse_loss = nn.MSELoss()

    # Lagrangian parameters
    rho = 1.0
    lagrangian_multiplier = 0.0
    max_rho = 10.0

    train_loss_history = []
    val_loss_history = []

    best_val_loss = float('inf')
    best_model_state = None
    epochs_no_improve = 0

    for epoch in range(num_epochs):
        model.train()
        train_epoch_loss = 0

        for i in range(0, len(trainX), batch_size):
            batch_x = torch.tensor(trainX[i:i + batch_size], dtype=torch.float32)
            batch_y = torch.tensor(trainY[i:i + batch_size], dtype=torch.float32)

            x_hat, alpha, entropy_reg, acyclic_penalty = model(batch_x)

            loss = (
                mse_loss(x_hat, batch_y)
                + 0.01 * entropy_reg
                + rho / 2 * acyclic_penalty.pow(2).mean()
                + lagrangian_multiplier * acyclic_penalty.mean()
            )

            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

            train_epoch_loss += loss.item()

        train_avg_loss = train_epoch_loss / (len(trainX) / batch_size)
        train_loss_history.append(train_avg_loss)

        model.eval()
        val_epoch_loss = 0
        with torch.no_grad():
            for i in range(0, len(valX), batch_size):
                batch_val_x = torch.tensor(valX[i:i + batch_size], dtype=torch.float32)
                batch_val_y = torch.tensor(valY[i:i + batch_size], dtype=torch.float32)

                x_hat, alpha, entropy_reg, acyclic_penalty = model(batch_val_x)

                val_loss = mse_loss(x_hat, batch_val_y) + 0.01 * entropy_reg
                val_epoch_loss += val_loss.item()

        val_avg_loss = val_epoch_loss / (len(valX) / batch_size)
        val_loss_history.append(val_avg_loss)

        # Verbose output
        if verbose == 1:
            if (epoch + 1) % 10 == 0:
                logger.info(f"Epoch {epoch + 1}/{num_epochs}, Train Loss: {train_avg_loss:.4f}, Val Loss: {val_avg_loss:.4f}")

        # Early stopping
        if val_avg_loss < best_val_loss:
            best_val_loss = val_avg_loss
            best_model_state = model.state_dict()
            epochs_no_improve = 0
        else:
            epochs_no_improve += 1
            if epochs_no_improve >= patience:
                if verbose:
                    logger.info(f"Early stopping triggered at epoch {epoch + 1}")
                break

    # Restore best model
    if best_model_state is not None:
        model.load_state_dict(best_model_state)

    return model, train_loss_history, val_loss_history

def reverse_difference(original_data, differenced_data, order=1):
    last_values = original_data.iloc[-len(differenced_data) - order:-order].values
    restored = differenced_data + last_values
    return restored

def evaluate_data(original, predicted, columns, table_name: str):
    metrics = []

    for i, col in enumerate(columns):
        mse = mean_squared_error(original[:, i], predicted[:, i])
        rmse = np.sqrt(mse)
        mae = mean_absolute_error(original[:, i], predicted[:, i])
        logger.info(f"{col} -- MSE: {mse:.5f}, RMSE: {rmse:.5f}, MAE: {mae:.5f}")
        metrics.append({
            "variable": col,
            "MSE": mse,
            "RMSE": rmse,
            "MAE": mae
        })

    # Aggregate (total) metrics
    total_mse = mean_squared_error(original, predicted)
    total_rmse = np.sqrt(total_mse)
    total_mae = mean_absolute_error(original, predicted)
    logger.info(f"Total -- MSE: {total_mse:.5f}, RMSE: {total_rmse:.5f}, MAE: {total_mae:.5f}")
    
    metrics.append({
        "variable": "Total",
        "MSE": total_mse,
        "RMSE": total_rmse,
        "MAE": total_mae
    })

    # Save to CSV
    os.makedirs(BASE_EVAL_PATH, exist_ok=True)
    output_path = os.path.join(BASE_EVAL_PATH, f"SrVAR_{table_name}_metrics.csv")
    pd.DataFrame(metrics).to_csv(output_path, index=True)
    logger.info(f"Evaluation metrics saved to {output_path}")

def unscale(data, train_df: pd.DataFrame, scalers):
    temp = data.copy()
    for index, i in enumerate(train_df.columns):
        scaler = scalers[i]
        temp[:, index] = scaler.inverse_transform(np.reshape(data[:, index], (-1, 1))).flatten()
    return temp

def predict_future(model, last_window, num_steps):
    model.eval()
    predictions = []

    # Ensure starting window shape is (1, p, num_vars)
    current_window = torch.tensor(last_window[np.newaxis, ...], dtype=torch.float32)

    with torch.no_grad():
        for _ in range(num_steps):
            pred, _, _, _ = model(current_window)     # shape: (1, num_vars)
            pred_np = pred.cpu().numpy()[0]           # shape: (num_vars,)
            predictions.append(pred_np)

            # Update window: remove first row, append prediction
            pred_tensor = pred.unsqueeze(1)           # shape: (1, 1, num_vars)
            current_window = torch.cat([current_window[:, 1:], pred_tensor], dim=1)  # Keep shape (1, p, num_vars)

    return np.array(predictions)

def main(table_name: str):
    """Main function to orchestrate the ETL process"""
    spark = None
    start_time = datetime.now()
    logger.info(f"Starting Training SrVAR model for {table_name}")
    
    try:
        # Initialize Spark
        spark = create_spark_session(f"Training SrVAR for {table_name}")
        
        # Ensure target namespace exists
        create_namespace_if_not_exists(spark, SOURCE_NAMESPACE)
        create_namespace_if_not_exists(spark, TARGET_NAMESPACE)
        
        # Read the data
        try:
            data = read_table(spark, f'{SOURCE_NAMESPACE}.{table_name}')
        except Exception as e:
            logger.error(f"Could not read data: {str(e)}")
            return

        # Split and scale the data
        scaled_train, scaled_test, scalers = split_the_data(data)

        # Check if the data needs to be differenced     
        i = 0
        train_diff = scaled_train
        while not checking_ADF(ADF_test(train_diff)):
            i += 1
            train_diff = difference(train_diff, order=i)

        logger.info('Difference data')
        train_diff = difference(scaled_train, order=i)
        test_diff = difference(scaled_test, order=i)

        # Finding optimize p value
        VAR_p_df = optimize_VAR(train_diff)
        
        p = int(VAR_p_df.iloc[0, 0])
        no_columns = len(train_diff.columns)
        logger.info(f'Finding optimal p: {p}')

        strategy = tf.distribute.MirroredStrategy()
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        # Transform data into sequences
        trainX, trainY = to_sequences_multivariate(train_diff, p)
        testX, testY = to_sequences_multivariate(test_diff, p)
        logger.info('Finish slice the data into sequences')

        # Training SrVAR model
        Xtrain, valX, Ytrain, valY = train_test_split(trainX, trainY, test_size=0.2, random_state=42)

        input_dim = trainX.shape[2]
        hidden_dim = 64
        num_vars = trainX.shape[2]

        best_val_loss = float('inf')
        best_num_states = None
        best_model = None
        best_train_loss = None
        logger.info('Start training model')
        # Try models with different number of hidden states
        for num_states in range(1, 6):
            model = SrVAR(input_dim=input_dim, hidden_dim=hidden_dim, num_states=num_states, num_vars=num_vars)

            trained_model_SrVAR, train_loss_history_SrVAR, val_loss_history_SrVAR = train_srvarm(
                model,
                Xtrain,
                Ytrain,
                valX,
                valY,
                num_epochs=100,
                batch_size=32,
                lr=0.0001,
                tau=1.0,
                verbose=0,
                patience=5
            )

            final_val_loss = val_loss_history_SrVAR[-1]
            final_train_loss = train_loss_history_SrVAR[-1]

            logger.info(f"num_states: {num_states}, train_loss: {final_train_loss:.4f}, val_loss: {final_val_loss:.4f}")

            if final_val_loss < best_val_loss:
                best_val_loss = final_val_loss
                best_num_states = num_states
                best_model = deepcopy(trained_model_SrVAR)
                best_train_loss = final_train_loss

        logger.info(f"\nBest model: num_states={best_num_states}, val_loss={best_val_loss:.4f}, train_loss={best_train_loss:.4f}")

        model_SrVAR = best_model

        start = time.time()

        trained_model_SrVAR, train_loss_history_SrVAR, val_loss_history_SrVAR = train_srvarm(
            model_SrVAR,
            trainX,
            trainY,
            valX,
            valY,
            num_epochs=100,
            batch_size=32,
            lr=0.0001,
            tau=1.0,
            verbose=1,
            patience=5  # Set patience for early stopping
        )

        end = time.time()

        logger.info(f"Training completed in {end - start:.2f} seconds")

        # Predict model on test data
        model_SrVAR.eval() 
        predictions_SrVAR = []
        batch_size = 32

        with torch.no_grad():
            for i in range(0, len(testX), batch_size):
                batch_x = torch.tensor(testX[i:i+batch_size], dtype=torch.float32)

                batch_pred, _, _, _ = model_SrVAR(batch_x)

                predictions_SrVAR.append(batch_pred.cpu().numpy())

        predictions_SrVAR = np.concatenate(predictions_SrVAR, axis=0)
        logger.info('Finished predict test data')

        # Reverse the differencing
        last_test_values = scaled_test.iloc[p:]
        testY_original = reverse_difference(last_test_values, testY)
        pred_SrVAR_original = reverse_difference(last_test_values, predictions_SrVAR)

        #Save model
        model_path = os.path.join(BASE_MODEL_PATH, f"srvar_model_{table_name}.pth")
        torch.save(trained_model_SrVAR.state_dict(), model_path)

        evaluate_data(testY_original, pred_SrVAR_original, scaled_test.columns, table_name)

        unscaled_SrVAR = unscale(pred_SrVAR_original, scaled_train, scalers)
        originY = unscale(testY_original, scaled_train, scalers)
        evaluate_data(originY, unscaled_SrVAR, scaled_test.columns, table_name)

        days_to_add = len(scaled_train)
        predict_date = start_date_dt + timedelta(days=days_to_add)
        future_date = datetime.today() + timedelta(days=1)

        # Convert both dates to dd-mm-yyyy format
        PREDICT_DATE = predict_date.strftime('%d-%m-%Y')
        FUTURE_DATE = future_date.strftime('%d-%m-%Y')

        SrVAR_predict = pd.DataFrame(originY, columns=scaled_test.columns)
        start_date = pd.to_datetime(PREDICT_DATE, dayfirst=True)
        SrVAR_predict["date"] = pd.date_range(start=start_date, periods=len(SrVAR_predict), freq='D')

        predict_path = os.path.join(BASE_PREDICT_PATH, 'SrVAR', f'{table_name}_predict_{today}.csv')

        SrVAR_predict.to_csv(predict_path, index=False)

        # Get the last `p` rows from training or testing data
        last_window = test_diff.iloc[-p:].values  # shape: (p, num_vars)

        # Predict future data
        # Predict p steps into the future
        logger.info('Finished future predict')
        future_preds_srvar = predict_future(model_SrVAR, last_window, num_steps=p)
        future_preds_srvar = unscale(reverse_difference(last_test_values, future_preds_srvar), scaled_train, scalers)

        future_SrVAR = pd.DataFrame(future_preds_srvar, columns=scaled_test.columns)
        start_date = pd.to_datetime(FUTURE_DATE, dayfirst=True)
        future_SrVAR["date"] = pd.date_range(start=start_date, periods=len(future_SrVAR), freq='D')

        full_path = os.path.join(BASE_FUTURE_PATH, 'SrVAR', f'{table_name}_future_{today}.csv')

        future_SrVAR.to_csv(full_path, index=False)
    except Exception as e:
        logger.critical(f"Job failed with critical error: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()
            end_time = datetime.now()
            logger.info(f"Spark Session stopped. Total job duration: {end_time - start_time}")