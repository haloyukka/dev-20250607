import os
import logging
import csv
import io
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Union
import pytz
import pandas as pd
import sqlalchemy
from sqlalchemy import text, inspect
from google.cloud import bigquery
from google.cloud import storage
import json
import random

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# JST タイムゾーン
JST = pytz.timezone('Asia/Tokyo')

# ハードコーディング設定値
HARDCODED_CONFIG = {
    # システム設定
    "USE_MOCK": True,  # Mockモードの有効/無効
    
    # SQL Server設定（実際の接続時に使用）
    "SQL_SERVER_HOST": "10.0.0.100",
    "SQL_SERVER_PORT": "1433",
    "SQL_SERVER_USER": "sync_user",
    "SQL_SERVER_PASSWORD": "your_secure_password",
    "SQL_SERVER_DATABASE": "production_db",
    
    # BigQuery設定
    "BIGQUERY_PROJECT": "test-project-12345",
    "BIGQUERY_DATASET": "data_sync",
    "BIGQUERY_LOCATION": "asia-northeast1",
    
    # Cloud Storage設定
    "GCS_BUCKET": "data-sync-bucket-test",
    
    # 同期テーブル設定
    "SYNC_TABLES_CONFIG": {
        "orders": {
            "timestamp_column": "updated_at"
        },
        "products": {
            "timestamp_column": "modified_date"
        },
        "customers": {
            "timestamp_column": None
        },
        "transactions": {
            "timestamp_column": "created_at"
        },
        "user_activities": {
            "timestamp_column": "activity_timestamp"
        }
    }
}

class MockSQLServerEngine:
    """SQL Server接続のMockクラス"""
    
    def __init__(self):
        self.is_connected = True
        self.mock_data = self._generate_mock_data()
        logger.info("Mock SQL Server Engine initialized")
    
    def _generate_mock_data(self) -> Dict[str, pd.DataFrame]:
        """モックデータを生成"""
        base_time = datetime.now(timezone.utc) - timedelta(days=30)
        
        # ordersテーブル（timestamp列あり）
        orders_data = []
        for i in range(150):
            orders_data.append({
                'order_id': f'ORD{i+1:06d}',
                'customer_id': f'CUST{random.randint(1, 50):03d}',
                'product_id': f'PROD{random.randint(1, 20):03d}',
                'quantity': random.randint(1, 10),
                'price': round(random.uniform(100, 10000), 2),
                'status': random.choice(['pending', 'processing', 'completed', 'cancelled']),
                'order_date': (base_time + timedelta(days=random.randint(0, 30))).date(),
                'updated_at': base_time + timedelta(
                    minutes=random.randint(0, 43200)  # 30日間のランダム時刻
                )
            })
        
        # productsテーブル（timestamp列あり）
        products_data = []
        categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports', 'Automotive']
        for i in range(25):
            products_data.append({
                'product_id': f'PROD{i+1:03d}',
                'product_name': f'Product {i+1}',
                'category': random.choice(categories),
                'description': f'High quality product {i+1} with great features',
                'price': round(random.uniform(100, 5000), 2),
                'stock_quantity': random.randint(0, 100),
                'is_active': random.choice([True, False]),
                'modified_date': base_time + timedelta(
                    hours=random.randint(0, 720)  # 30日間のランダム時刻
                )
            })
        
        # customersテーブル（timestamp列なし）
        customers_data = []
        prefectures = ['東京都', '大阪府', '愛知県', '神奈川県', '北海道', '福岡県']
        for i in range(50):
            customers_data.append({
                'customer_id': f'CUST{i+1:03d}',
                'customer_name': f'田中{i+1}',
                'email': f'customer{i+1}@example.com',
                'phone': f'090-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}',
                'prefecture': random.choice(prefectures),
                'address': f'{random.randint(1, 999)}番地',
                'birth_date': (base_time - timedelta(days=random.randint(7300, 21900))).date(),  # 20-60歳
                'registration_date': (base_time - timedelta(days=random.randint(0, 365))).date(),
                'is_premium': random.choice([True, False])
            })
        
        # transactionsテーブル（timestamp列あり）
        transactions_data = []
        payment_methods = ['credit_card', 'bank_transfer', 'cash', 'electronic_money', 'points']
        for i in range(300):
            transactions_data.append({
                'transaction_id': f'TXN{i+1:08d}',
                'order_id': f'ORD{random.randint(1, 150):06d}',
                'payment_method': random.choice(payment_methods),
                'amount': round(random.uniform(100, 15000), 2),
                'tax_amount': round(random.uniform(10, 1500), 2),
                'transaction_status': random.choice(['pending', 'completed', 'failed', 'refunded']),
                'created_at': base_time + timedelta(
                    minutes=random.randint(0, 43200)
                )
            })
        
        # user_activitiesテーブル（timestamp列あり）
        user_activities_data = []
        activities = ['login', 'logout', 'view_product', 'add_to_cart', 'purchase', 'review']
        for i in range(500):
            user_activities_data.append({
                'activity_id': f'ACT{i+1:08d}',
                'customer_id': f'CUST{random.randint(1, 50):03d}',
                'activity_type': random.choice(activities),
                'product_id': f'PROD{random.randint(1, 25):03d}' if random.random() > 0.3 else None,
                'session_id': f'SES{random.randint(1, 1000):06d}',
                'ip_address': f'192.168.{random.randint(1, 255)}.{random.randint(1, 255)}',
                'user_agent': 'Mozilla/5.0 (compatible; test browser)',
                'activity_timestamp': base_time + timedelta(
                    minutes=random.randint(0, 43200)
                )
            })
        
        return {
            'orders': pd.DataFrame(orders_data),
            'products': pd.DataFrame(products_data),
            'customers': pd.DataFrame(customers_data),
            'transactions': pd.DataFrame(transactions_data),
            'user_activities': pd.DataFrame(user_activities_data)
        }
    
    def execute(self, query: str, params: Optional[List] = None) -> pd.DataFrame:
        """SQLクエリのMock実行"""
        logger.info(f"Mock SQL execution: {query[:100]}...")
        
        # テーブル名を抽出
        query_lower = query.lower()
        table_name = None
        for name in self.mock_data.keys():
            if f"from {name}" in query_lower or f"from [{name}]" in query_lower:
                table_name = name
                break
        
        if not table_name:
            raise ValueError(f"Unknown table in query: {query}")
        
        df = self.mock_data[table_name].copy()
        
        # WHERE句の簡易処理（timestampフィルタ）
        if params and len(params) > 0 and "where" in query_lower:
            timestamp_param = params[0]
            if isinstance(timestamp_param, datetime):
                # timestamp列を特定
                timestamp_columns = ['updated_at', 'modified_date', 'created_at', 'activity_timestamp']
                for col in timestamp_columns:
                    if col in df.columns:
                        # タイムゾーンを考慮した比較
                        if df[col].dtype == 'datetime64[ns]':
                            df[col] = pd.to_datetime(df[col])
                        df = df[df[col] > timestamp_param]
                        logger.info(f"Filtered by {col} > {timestamp_param}, remaining rows: {len(df)}")
                        break
        
        logger.info(f"Mock query returned {len(df)} rows from {table_name}")
        return df
    
    def dispose(self):
        """接続クローズのMock"""
        logger.info("Mock SQL Server connection disposed")

class MockBigQueryClient:
    """BigQuery クライアントのMockクラス"""
    
    def __init__(self, project: str):
        self.project = project
        self.datasets: Dict[str, Any] = {}
        self.tables: Dict[str, Any] = {}
        self.sync_metadata: List[Dict[str, Any]] = []
        logger.info(f"Mock BigQuery Client initialized for project: {project}")
    
    def create_dataset(self, dataset_id: str):
        """データセット作成のMock"""
        self.datasets[dataset_id] = {}
        logger.info(f"Mock dataset created: {dataset_id}")
    
    def create_table(self, table, exists_ok=True):
        """テーブル作成のMock"""
        if hasattr(table, 'project'):
            table_id = f"{table.project}.{table.dataset_id}.{table.table_id}"
        else:
            table_id = f"{self.project}.{HARDCODED_CONFIG['BIGQUERY_DATASET']}.sync_metadata"
        
        self.tables[table_id] = {
            'schema': getattr(table, 'schema', []),
            'data': []
        }
        logger.info(f"Mock table created: {table_id}")
    
    def query(self, query: str, job_config=None):
        """クエリ実行のMock"""
        logger.info(f"Mock BigQuery query: {query[:100]}...")
        
        # sync_metadataからの取得をシミュレート
        if "sync_metadata" in query and "SELECT" in query.upper():
            # パラメータからテーブル名を取得
            table_name = "unknown"
            if job_config and hasattr(job_config, 'query_parameters') and job_config.query_parameters:
                for param in job_config.query_parameters:
                    if param.name == "table_name":
                        table_name = param.value
                        break
            
            # 既存のメタデータから検索
            matching_metadata = [m for m in self.sync_metadata if m.get('table_name') == table_name]
            
            if matching_metadata:
                last_sync = matching_metadata[-1]['last_sync_time']
            else:
                # 初回実行時は2時間前に設定（差分データを取得できるように）
                last_sync = datetime.now(timezone.utc) - timedelta(hours=2)
            
            logger.info(f"Mock returned last sync time for {table_name}: {last_sync}")
            return MockQueryResult([{'last_sync': last_sync}])
        
        # MERGE文の処理をシミュレート
        elif "MERGE" in query.upper() and "sync_metadata" in query:
            if job_config and hasattr(job_config, 'query_parameters') and job_config.query_parameters:
                metadata = {}
                for param in job_config.query_parameters:
                    metadata[param.name] = param.value
                
                self.sync_metadata.append(metadata)
                logger.info(f"Mock sync metadata updated: {metadata.get('table_name', 'unknown')} -> {metadata.get('last_sync_time', 'N/A')}")
            
            return MockQueryResult([])
        
        return MockQueryResult([])

class MockQueryResult:
    """BigQueryクエリ結果のMockクラス"""
    
    def __init__(self, data: List[Dict]):
        self.data = data
    
    def result(self):
        """結果取得のMock"""
        return self
    
    def __iter__(self):
        return iter(self.data)

class MockStorageClient:
    """Cloud Storage クライアントのMockクラス"""
    
    def __init__(self):
        self.buckets = {}
        logger.info("Mock Storage Client initialized")
    
    def bucket(self, bucket_name: str):
        """バケット取得のMock"""
        if bucket_name not in self.buckets:
            self.buckets[bucket_name] = MockBucket(bucket_name)
        return self.buckets[bucket_name]

class MockBucket:
    """Cloud Storage バケットのMockクラス"""
    
    def __init__(self, name: str):
        self.name = name
        self.blobs: Dict[str, 'MockBlob'] = {}
    
    def blob(self, blob_name: str):
        """Blob取得のMock"""
        if blob_name not in self.blobs:
            self.blobs[blob_name] = MockBlob(blob_name, self.name)
        return self.blobs[blob_name]

class MockBlob:
    """Cloud Storage BlobのMockクラス"""
    
    def __init__(self, name: str, bucket_name: str):
        self.name = name
        self.bucket_name = bucket_name
        self.content: Optional[str] = None
        self.content_type: Optional[str] = None
    
    def upload_from_string(self, data: str, content_type: Optional[str] = None):
        """文字列アップロードのMock"""
        self.content = data
        self.content_type = content_type
        
        # CSVの行数をカウント
        line_count = data.count('\n')
        data_size_kb = len(data) / 1024
        
        logger.info(f"Mock file uploaded: gs://{self.bucket_name}/{self.name}")
        logger.info(f"  - Size: {data_size_kb:.2f} KB")
        logger.info(f"  - Lines: {line_count}")
        logger.info(f"  - Content-Type: {content_type}")
        
        # サンプルデータの最初の数行を表示
        lines = data.split('\n')
        if len(lines) > 1:
            logger.info(f"  - Header: {lines[0]}")
            if len(lines) > 2:
                logger.info(f"  - Sample: {lines[1]}")

class DatabaseConfig:
    """データベース設定クラス（ハードコーディング対応）"""
    def __init__(self):
        # ハードコーディングされた設定値を使用
        self.use_mock = HARDCODED_CONFIG["USE_MOCK"]
        
        # SQL Server設定
        self.sql_server_host = HARDCODED_CONFIG["SQL_SERVER_HOST"]
        self.sql_server_port = HARDCODED_CONFIG["SQL_SERVER_PORT"]
        self.sql_server_user = HARDCODED_CONFIG["SQL_SERVER_USER"]
        self.sql_server_password = HARDCODED_CONFIG["SQL_SERVER_PASSWORD"]
        self.sql_server_database = HARDCODED_CONFIG["SQL_SERVER_DATABASE"]
        
        # BigQuery設定
        self.bigquery_project = HARDCODED_CONFIG["BIGQUERY_PROJECT"]
        self.bigquery_dataset = HARDCODED_CONFIG["BIGQUERY_DATASET"]
        self.bigquery_location = HARDCODED_CONFIG["BIGQUERY_LOCATION"]
        
        # Cloud Storage設定
        self.gcs_bucket = HARDCODED_CONFIG["GCS_BUCKET"]
        
        # 同期対象テーブル設定
        self.sync_tables = HARDCODED_CONFIG["SYNC_TABLES_CONFIG"]
        
        logger.info(f"Configuration initialized (Mock mode: {self.use_mock})")
        logger.info(f"Target tables: {list(self.sync_tables.keys())}")

class DataSyncManager:
    """データ同期管理クラス（Mock対応）"""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.bigquery_client: Any
        self.storage_client: Any
        self.sql_engine: Optional[MockSQLServerEngine] = None
        
        # MockまたはReal clientsの初期化
        if config.use_mock:
            self.bigquery_client = MockBigQueryClient(config.bigquery_project)
            self.storage_client = MockStorageClient()
            self.sql_engine = MockSQLServerEngine()
        else:
            self.bigquery_client = bigquery.Client(project=config.bigquery_project)
            self.storage_client = storage.Client()
            self.sql_engine = None
        
        logger.info(f"DataSyncManager initialized (Mock: {config.use_mock})")
        
    def create_sql_engine(self) -> Optional[Union[sqlalchemy.engine.Engine, MockSQLServerEngine]]:
        """SQL Server接続エンジンを作成（Mock対応）"""
        if self.config.use_mock:
            return self.sql_engine
        
        try:
            # 実際のSQL Server接続
            connection_string = (
                f"mssql+pyodbc://{self.config.sql_server_user}:"
                f"{self.config.sql_server_password}@"
                f"{self.config.sql_server_host}:{self.config.sql_server_port}/"
                f"{self.config.sql_server_database}?"
                f"driver=ODBC+Driver+17+for+SQL+Server&"
                f"TrustServerCertificate=yes"
            )
            
            engine = sqlalchemy.create_engine(
                connection_string,
                pool_pre_ping=True,
                pool_recycle=300,
                echo=False
            )
            
            logger.info("Real SQL Server connection engine created")
            return engine
            
        except Exception as e:
            logger.error(f"SQL Server connection engine creation error: {e}")
            raise

    def get_max_timestamp(self, df: pd.DataFrame, timestamp_column: str) -> Optional[datetime]:
        """データフレームから最大タイムスタンプを取得"""
        try:
            if df.empty or timestamp_column not in df.columns:
                return None
                
            max_ts = df[timestamp_column].max()
            
            if pd.isna(max_ts):
                return None
                
            if isinstance(max_ts, pd.Timestamp):
                return max_ts.to_pydatetime()
            elif isinstance(max_ts, datetime):
                return max_ts
            else:
                return pd.to_datetime(max_ts).to_pydatetime()
                
        except Exception as e:
            logger.error(f"Max timestamp retrieval error: {e}")
            return None

    def sync_table(self, table_name: str, table_config: Dict[str, Any]):
        """単一テーブルの同期を実行（Mock対応）"""
        try:
            logger.info(f"=== Table sync started: {table_name} ===")
            
            timestamp_column = table_config.get('timestamp_column')
            logger.info(f"Timestamp column: {timestamp_column}")
            
            # テーブルカラム情報を取得（Mock対応）
            columns = self.get_table_columns(table_name)
            if columns:
                logger.info(f"Table columns: {columns}")
            
            # データ抽出
            df = self.extract_data(table_name, timestamp_column)
            
            if df.empty:
                logger.info(f"No sync target data: {table_name}")
                return
            
            # データ概要をログ出力
            logger.info(f"Data summary for {table_name}:")
            logger.info(f"  - Rows: {len(df)}")
            logger.info(f"  - Columns: {list(df.columns)}")
            if timestamp_column and timestamp_column in df.columns:
                min_ts = df[timestamp_column].min()
                max_ts = df[timestamp_column].max()
                logger.info(f"  - Timestamp range: {min_ts} to {max_ts}")
            
            # GCSに保存
            gcs_filename = self.save_to_gcs(df, table_name)
            
            # 最大タイムスタンプを取得
            max_timestamp = None
            if timestamp_column:
                max_timestamp = self.get_max_timestamp(df, timestamp_column)
                logger.info(f"Max timestamp for metadata: {max_timestamp}")
            
            # 同期メタデータを更新
            self.update_sync_metadata(table_name, max_timestamp)
            
            logger.info(f"=== Table sync completed: {table_name} (File: {gcs_filename}) ===")
            
        except Exception as e:
            logger.error(f"Table sync error: {table_name} - {e}")
            raise

    def run_sync(self):
        """全体の同期プロセスを実行（Mock対応）"""
        try:
            logger.info("==========================================")
            logger.info("Data sync process started")
            logger.info(f"Mode: {'Mock' if self.config.use_mock else 'Real'}")
            logger.info(f"Target tables: {list(self.config.sync_tables.keys())}")
            logger.info("==========================================")
            
            # SQL Server接続（Mock対応）
            if not self.config.use_mock:
                self.sql_engine = self.create_sql_engine()
            
            sync_results = []
            
            # 各テーブルを同期
            for table_name, table_config in self.config.sync_tables.items():
                try:
                    self.sync_table(table_name, table_config)
                    sync_results.append({"table": table_name, "status": "success"})
                except Exception as e:
                    logger.error(f"Error occurred in table {table_name} sync: {e}")
                    sync_results.append({"table": table_name, "status": "error", "error": str(e)})
                    continue
            
            # 結果サマリー
            success_count = len([r for r in sync_results if r["status"] == "success"])
            error_count = len([r for r in sync_results if r["status"] == "error"])
            
            logger.info("==========================================")
            logger.info("Data sync process completed")
            logger.info(f"Summary: {success_count} success, {error_count} errors")
            
            for result in sync_results:
                if result["status"] == "success":
                    logger.info(f"  ✓ {result['table']}: Success")
                else:
                    logger.error(f"  ✗ {result['table']}: {result.get('error', 'Unknown error')}")
            
            logger.info("==========================================")
            
            return sync_results
            
        except Exception as e:
            logger.error(f"Error occurred in data sync process: {e}")
            raise
        finally:
            # リソースクリーンアップ
            if hasattr(self.sql_engine, 'dispose'):
                self.sql_engine.dispose()

    def get_table_columns(self, table_name: str) -> List[str]:
        """テーブルのカラム一覧を取得（Mock対応）"""
        if self.config.use_mock:
            if self.sql_engine is None:
                raise ValueError("SQL engine is not initialized")
            if hasattr(self.sql_engine, 'mock_data') and table_name in self.sql_engine.mock_data:
                columns = list(self.sql_engine.mock_data[table_name].columns)
                logger.info(f"Mock table columns for {table_name}: {columns}")
                return columns
            return []
        
        try:
            if self.sql_engine is None:
                raise ValueError("SQL engine is not initialized")
            inspector = inspect(self.sql_engine)
            if inspector is None:
                raise ValueError("Failed to create inspector")
            columns = inspector.get_columns(table_name)
            return [col['name'] for col in columns]
        except Exception as e:
            logger.error(f"Table {table_name} column retrieval error: {e}")
            raise

    def extract_data(self, table_name: str, timestamp_column: Optional[str]) -> pd.DataFrame:
        """SQL Serverからデータを抽出（Mock対応）"""
        try:
            if self.config.use_mock:
                if self.sql_engine is None:
                    raise ValueError("SQL engine is not initialized")
                # Mock用のデータ抽出
                if timestamp_column:
                    last_sync = self.get_last_sync_time(table_name)
                    if last_sync:
                        query = f"SELECT * FROM {table_name} WHERE {timestamp_column} > ? ORDER BY {timestamp_column}"
                        df = self.sql_engine.execute(query, [last_sync])
                        logger.info(f"Mock differential data extracted: {table_name} ({len(df)} records) since {last_sync}")
                    else:
                        query = f"SELECT * FROM {table_name} ORDER BY {timestamp_column}"
                        df = self.sql_engine.execute(query)
                        logger.info(f"Mock initial full data extracted: {table_name} ({len(df)} records)")
                else:
                    query = f"SELECT * FROM {table_name}"
                    df = self.sql_engine.execute(query)
                    logger.info(f"Mock full data extracted: {table_name} ({len(df)} records)")
                
                return df
            else:
                # 実際のSQL Server処理
                if timestamp_column:
                    last_sync = self.get_last_sync_time(table_name)
                    
                    if last_sync:
                        query = f"""
                        SELECT * FROM {table_name}
                        WHERE {timestamp_column} > ?
                        ORDER BY {timestamp_column}
                        """
                        df = pd.read_sql(query, self.sql_engine, params=[last_sync])
                        logger.info(f"Differential data extracted: {table_name} ({len(df)} records)")
                    else:
                        query = f"SELECT * FROM {table_name} ORDER BY {timestamp_column}"
                        df = pd.read_sql(query, self.sql_engine)
                        logger.info(f"Initial full data extracted: {table_name} ({len(df)} records)")
                else:
                    query = f"SELECT * FROM {table_name}"
                    df = pd.read_sql(query, self.sql_engine)
                    logger.info(f"Full data extracted: {table_name} ({len(df)} records)")
                    
                return df
                
        except Exception as e:
            logger.error(f"Data extraction error (Table: {table_name}): {e}")
            raise

    def save_to_gcs(self, df: pd.DataFrame, table_name: str) -> Optional[str]:
        """データをCSVとしてGCSに保存（Mock対応）"""
        try:
            if df.empty:
                logger.info(f"Data is empty, skipping GCS save: {table_name}")
                return None
                
            # JST タイムスタンプ付きファイル名
            now_jst = datetime.now(JST)
            filename = f"{table_name}_{now_jst.strftime('%Y%m%d_%H%M%S')}.csv"
            
            # CSVデータをメモリ上で作成
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False, encoding='utf-8')
            csv_content = csv_buffer.getvalue()
            
            # GCSにアップロード（Mock対応）
            bucket = self.storage_client.bucket(self.config.gcs_bucket)
            blob = bucket.blob(filename)
            blob.upload_from_string(csv_content, content_type='text/csv')
            
            return filename
            
        except Exception as e:
            logger.error(f"GCS save error: {e}")
            raise

    def update_sync_metadata(self, table_name: str, max_timestamp: Optional[datetime]):
        """同期メタデータを更新（Mock対応）"""
        try:
            current_time = datetime.now(timezone.utc)
            
            # sync_metadataテーブルが存在しない場合は作成
            self.ensure_sync_metadata_table()
            
            if self.config.use_mock:
                # Mock用の処理
                class MockQueryParameter:
                    def __init__(self, name, param_type, value):
                        self.name = name
                        self.type = param_type
                        self.value = value
                
                class MockJobConfig:
                    def __init__(self, query_parameters):
                        self.query_parameters = query_parameters
                
                query = f"""
                MERGE `{self.config.bigquery_project}.{self.config.bigquery_dataset}.sync_metadata` AS target
                USING (SELECT @table_name as table_name, @last_sync_time as last_sync_time, @updated_at as updated_at) AS source
                ON target.table_name = source.table_name
                """
                
                job_config = MockJobConfig([
                    MockQueryParameter("table_name", "STRING", table_name),
                    MockQueryParameter("last_sync_time", "TIMESTAMP", max_timestamp),
                    MockQueryParameter("updated_at", "TIMESTAMP", current_time)
                ])
                
                self.bigquery_client.query(query, job_config=job_config).result()
            else:
                # 実際のBigQuery処理
                query = f"""
                MERGE `{self.config.bigquery_project}.{self.config.bigquery_dataset}.sync_metadata` AS target
                USING (
                    SELECT 
                        @table_name as table_name,
                        @last_sync_time as last_sync_time,
                        @updated_at as updated_at
                ) AS source
                ON target.table_name = source.table_name
                WHEN MATCHED THEN
                    UPDATE SET 
                        last_sync_time = source.last_sync_time,
                        updated_at = source.updated_at
                WHEN NOT MATCHED THEN
                    INSERT (table_name, last_sync_time, updated_at)
                    VALUES (source.table_name, source.last_sync_time, source.updated_at)
                """
                
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("table_name", "STRING", table_name),
                        bigquery.ScalarQueryParameter("last_sync_time", "TIMESTAMP", max_timestamp),
                        bigquery.ScalarQueryParameter("updated_at", "TIMESTAMP", current_time)
                    ]
                )
                
                self.bigquery_client.query(query, job_config=job_config).result()
            
            logger.info(f"Sync metadata updated: {table_name} -> {max_timestamp}")
            
        except Exception as e:
            logger.error(f"Sync metadata update error: {e}")
            raise

    def ensure_sync_metadata_table(self):
        """sync_metadataテーブルが存在しない場合は作成（Mock対応）"""
        try:
            table_id = f"{self.config.bigquery_project}.{self.config.bigquery_dataset}.sync_metadata"
            
            if self.config.use_mock:
                # Mock用のテーブル作成
                class MockTable:
                    def __init__(self, table_id, schema):
                        self.config = HARDCODED_CONFIG
                        self.project = self.config["BIGQUERY_PROJECT"]
                        self.dataset_id = self.config["BIGQUERY_DATASET"]
                        self.table_id = "sync_metadata"
                        self.schema = schema
                        self.clustering_fields = None
                
                class MockSchemaField:
                    def __init__(self, name, field_type, mode):
                        self.name = name
                        self.field_type = field_type
                        self.mode = mode
                
                schema = [
                    MockSchemaField("table_name", "STRING", "REQUIRED"),
                    MockSchemaField("last_sync_time", "TIMESTAMP", "NULLABLE"),
                    MockSchemaField("updated_at", "TIMESTAMP", "REQUIRED"),
                ]
                
                table = MockTable(table_id, schema)
                self.bigquery_client.create_table(table, exists_ok=True)
            else:
                # 実際のBigQuery処理
                schema = [
                    bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("last_sync_time", "TIMESTAMP", mode="NULLABLE"),
                    bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
                ]
                
                table = bigquery.Table(table_id, schema=schema)
                table.clustering_fields = ["table_name"]
                
                self.bigquery_client.create_table(table, exists_ok=True)
            
            logger.info("sync_metadata table confirmed/created")
            
        except Exception as e:
            logger.error(f"sync_metadata table creation error: {e}")
            raise

    def get_last_sync_time(self, table_name: str) -> Optional[datetime]:
        """BigQueryから前回同期時刻を取得（Mock対応）"""
        try:
            if self.config.use_mock:
                # MockBigQueryClientのquery方法を使用
                query = f"""
                SELECT MAX(last_sync_time) as last_sync
                FROM `{self.config.bigquery_project}.{self.config.bigquery_dataset}.sync_metadata`
                WHERE table_name = @table_name
                """
                
                # Mock用のjob_config
                class MockQueryParameter:
                    def __init__(self, name, param_type, value):
                        self.name = name
                        self.type = param_type
                        self.value = value
                
                class MockJobConfig:
                    def __init__(self, query_parameters):
                        self.query_parameters = query_parameters
                
                job_config = MockJobConfig([
                    MockQueryParameter("table_name", "STRING", table_name)
                ])
                
                query_job = self.bigquery_client.query(query, job_config=job_config)
                results = query_job.result()
                
                for row in results:
                    if row.get('last_sync'):
                        return row['last_sync']
                return None
            else:
                # 実際のBigQuery処理
                query = f"""
                SELECT MAX(last_sync_time) as last_sync
                FROM `{self.config.bigquery_project}.{self.config.bigquery_dataset}.sync_metadata`
                WHERE table_name = @table_name
                """
                
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("table_name", "STRING", table_name)
                    ]
                )
                
                query_job = self.bigquery_client.query(query, job_config=job_config)
                results = query_job.result()
                
                for row in results:
                    if row.last_sync:
                        return row.last_sync
                return None
                
        except Exception as e:
            logger.warning(f"Last sync time retrieval error (Table: {table_name}): {e}")
            return None

def main(request=None):
    """Cloud Function エントリーポイント（Mock対応）"""
    try:
        logger.info("Cloud Function execution started")
        
        # 設定を読み込み
        config = DatabaseConfig()
        
        # 設定情報をログ出力
        logger.info("Configuration loaded:")
        logger.info(f"  - Mock mode: {config.use_mock}")
        logger.info(f"  - BigQuery project: {config.bigquery_project}")
        logger.info(f"  - BigQuery dataset: {config.bigquery_dataset}")
        logger.info(f"  - GCS bucket: {config.gcs_bucket}")
        logger.info(f"  - SQL Server host: {config.sql_server_host}")
        
        # 同期マネージャーを初期化して実行
        sync_manager = DataSyncManager(config)
        sync_results = sync_manager.run_sync()
        
        # 結果レスポンス作成
        mode = "Mock mode" if config.use_mock else "Real mode"
        success_count = len([r for r in sync_results if r["status"] == "success"])
        error_count = len([r for r in sync_results if r["status"] == "error"])
        
        response = {
            "status": "success" if error_count == 0 else "partial_success",
            "message": f"Data sync completed ({mode})",
            "summary": {
                "total_tables": len(sync_results),
                "success_count": success_count,
                "error_count": error_count
            },
            "details": sync_results
        }
        
        logger.info("Cloud Function execution completed successfully")
        return response
        
    except Exception as e:
        logger.error(f"Cloud Function execution error: {e}")
        return {
            "status": "error", 
            "message": str(e),
            "summary": {
                "total_tables": 0,
                "success_count": 0,
                "error_count": 1
            }
        }, 500

def test_sync_locally():
    """ローカルテスト実行用関数"""
    print("=" * 60)
    print("LOCAL TEST EXECUTION")
    print("=" * 60)
    
    try:
        result = main()
        print("\n" + "=" * 60)
        print("TEST RESULT:")
        print("=" * 60)
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
        print("=" * 60)
        
        return result
        
    except Exception as e:
        print(f"Test execution failed: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    # ローカルテスト実行
    test_sync_locally()