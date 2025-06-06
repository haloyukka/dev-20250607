import os
import logging
import csv
import io
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import pytz
import pandas as pd
import sqlalchemy
from sqlalchemy import text, inspect
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.sql.connector import Connector

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# JST タイムゾーン
JST = pytz.timezone('Asia/Tokyo')

class DatabaseConfig:
    """データベース設定クラス"""
    def __init__(self):
        self.sql_server_host = os.environ.get('SQL_SERVER_HOST')
        self.sql_server_port = os.environ.get('SQL_SERVER_PORT', '1433')
        self.sql_server_user = os.environ.get('SQL_SERVER_USER')
        self.sql_server_password = os.environ.get('SQL_SERVER_PASSWORD')
        self.sql_server_database = os.environ.get('SQL_SERVER_DATABASE')
        
        self.bigquery_project = os.environ.get('BIGQUERY_PROJECT')
        self.bigquery_dataset = os.environ.get('BIGQUERY_DATASET', 'data_sync')
        self.bigquery_location = os.environ.get('BIGQUERY_LOCATION', 'asia-northeast1')
        
        self.gcs_bucket = os.environ.get('GCS_BUCKET')
        
        # 同期対象テーブル設定（環境変数から取得、JSON形式）
        import json
        tables_config = os.environ.get('SYNC_TABLES_CONFIG', '{}')
        self.sync_tables = json.loads(tables_config)

class DataSyncManager:
    """データ同期管理クラス"""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.bigquery_client = bigquery.Client(project=config.bigquery_project)
        self.storage_client = storage.Client()
        self.sql_engine = None
        
    def create_sql_engine(self) -> sqlalchemy.engine.Engine:
        """SQL Server接続エンジンを作成"""
        try:
            # SQL Server接続文字列を構築
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
            
            logger.info("SQL Server接続エンジンを作成しました")
            return engine
            
        except Exception as e:
            logger.error(f"SQL Server接続エンジン作成エラー: {e}")
            raise

    def get_table_columns(self, table_name: str) -> List[str]:
        """テーブルのカラム一覧を取得"""
        try:
            inspector = inspect(self.sql_engine)
            columns = inspector.get_columns(table_name)
            return [col['name'] for col in columns]
        except Exception as e:
            logger.error(f"テーブル {table_name} のカラム取得エラー: {e}")
            raise

    def get_last_sync_time(self, table_name: str) -> Optional[datetime]:
        """BigQueryから前回同期時刻を取得"""
        try:
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
            logger.warning(f"前回同期時刻取得エラー (テーブル: {table_name}): {e}")
            return None

    def update_sync_metadata(self, table_name: str, max_timestamp: Optional[datetime]):
        """同期メタデータを更新"""
        try:
            current_time = datetime.now(timezone.utc)
            
            # sync_metadataテーブルが存在しない場合は作成
            self.ensure_sync_metadata_table()
            
            # メタデータを挿入/更新
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
            logger.info(f"同期メタデータを更新しました: {table_name}")
            
        except Exception as e:
            logger.error(f"同期メタデータ更新エラー: {e}")
            raise

    def ensure_sync_metadata_table(self):
        """sync_metadataテーブルが存在しない場合は作成"""
        try:
            table_id = f"{self.config.bigquery_project}.{self.config.bigquery_dataset}.sync_metadata"
            
            schema = [
                bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("last_sync_time", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
            ]
            
            table = bigquery.Table(table_id, schema=schema)
            table.clustering_fields = ["table_name"]
            
            self.bigquery_client.create_table(table, exists_ok=True)
            logger.info("sync_metadataテーブルを確認/作成しました")
            
        except Exception as e:
            logger.error(f"sync_metadataテーブル作成エラー: {e}")
            raise

    def extract_data(self, table_name: str, timestamp_column: Optional[str]) -> pd.DataFrame:
        """SQL Serverからデータを抽出"""
        try:
            if timestamp_column:
                # タイムスタンプカラムがある場合は差分抽出
                last_sync = self.get_last_sync_time(table_name)
                
                if last_sync:
                    query = f"""
                    SELECT * FROM {table_name}
                    WHERE {timestamp_column} > ?
                    ORDER BY {timestamp_column}
                    """
                    df = pd.read_sql(query, self.sql_engine, params=[last_sync])
                    logger.info(f"差分データを抽出しました: {table_name} ({len(df)}件)")
                else:
                    query = f"SELECT * FROM {table_name} ORDER BY {timestamp_column}"
                    df = pd.read_sql(query, self.sql_engine)
                    logger.info(f"初回全件データを抽出しました: {table_name} ({len(df)}件)")
            else:
                # タイムスタンプカラムがない場合は全件抽出
                query = f"SELECT * FROM {table_name}"
                df = pd.read_sql(query, self.sql_engine)
                logger.info(f"全件データを抽出しました: {table_name} ({len(df)}件)")
                
            return df
            
        except Exception as e:
            logger.error(f"データ抽出エラー (テーブル: {table_name}): {e}")
            raise

    def save_to_gcs(self, df: pd.DataFrame, table_name: str) -> str:
        """データをCSVとしてGCSに保存"""
        try:
            if df.empty:
                logger.info(f"データが空のため、GCSへの保存をスキップします: {table_name}")
                return None
                
            # JST タイムスタンプ付きファイル名
            now_jst = datetime.now(JST)
            filename = f"{table_name}_{now_jst.strftime('%Y%m%d_%H%M%S')}.csv"
            
            # CSVデータをメモリ上で作成
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False, encoding='utf-8')
            csv_content = csv_buffer.getvalue()
            
            # GCSにアップロード
            bucket = self.storage_client.bucket(self.config.gcs_bucket)
            blob = bucket.blob(filename)
            blob.upload_from_string(csv_content, content_type='text/csv')
            
            logger.info(f"CSVファイルをGCSに保存しました: gs://{self.config.gcs_bucket}/{filename}")
            return filename
            
        except Exception as e:
            logger.error(f"GCS保存エラー: {e}")
            raise

    def get_max_timestamp(self, df: pd.DataFrame, timestamp_column: str) -> Optional[datetime]:
        """データフレームから最大タイムスタンプを取得"""
        try:
            if df.empty or timestamp_column not in df.columns:
                return None
                
            max_ts = df[timestamp_column].max()
            
            # pandas Timestampをdatetimeに変換
            if pd.isna(max_ts):
                return None
                
            if isinstance(max_ts, pd.Timestamp):
                return max_ts.to_pydatetime()
            elif isinstance(max_ts, datetime):
                return max_ts
            else:
                return pd.to_datetime(max_ts).to_pydatetime()
                
        except Exception as e:
            logger.error(f"最大タイムスタンプ取得エラー: {e}")
            return None

    def sync_table(self, table_name: str, table_config: Dict[str, Any]):
        """単一テーブルの同期を実行"""
        try:
            logger.info(f"テーブル同期開始: {table_name}")
            
            timestamp_column = table_config.get('timestamp_column')
            
            # データ抽出
            df = self.extract_data(table_name, timestamp_column)
            
            if df.empty:
                logger.info(f"同期対象データなし: {table_name}")
                return
            
            # GCSに保存
            gcs_filename = self.save_to_gcs(df, table_name)
            
            # 最大タイムスタンプを取得
            max_timestamp = None
            if timestamp_column:
                max_timestamp = self.get_max_timestamp(df, timestamp_column)
            
            # 同期メタデータを更新
            self.update_sync_metadata(table_name, max_timestamp)
            
            logger.info(f"テーブル同期完了: {table_name} (ファイル: {gcs_filename})")
            
        except Exception as e:
            logger.error(f"テーブル同期エラー: {table_name} - {e}")
            raise

    def run_sync(self):
        """全体の同期プロセスを実行"""
        try:
            logger.info("データ同期処理を開始します")
            
            # SQL Server接続
            self.sql_engine = self.create_sql_engine()
            
            # 各テーブルを同期
            for table_name, table_config in self.config.sync_tables.items():
                try:
                    self.sync_table(table_name, table_config)
                except Exception as e:
                    logger.error(f"テーブル {table_name} の同期でエラーが発生しました: {e}")
                    # 他のテーブルの同期は続行
                    continue
            
            logger.info("データ同期処理が完了しました")
            
        except Exception as e:
            logger.error(f"データ同期処理でエラーが発生しました: {e}")
            raise
        finally:
            # リソースクリーンアップ
            if self.sql_engine:
                self.sql_engine.dispose()

def main(request):
    """Cloud Function エントリーポイント"""
    try:
        # 設定を読み込み
        config = DatabaseConfig()
        
        # 同期マネージャーを初期化して実行
        sync_manager = DataSyncManager(config)
        sync_manager.run_sync()
        
        return {"status": "success", "message": "データ同期が正常に完了しました"}
        
    except Exception as e:
        logger.error(f"Cloud Function実行エラー: {e}")
        return {"status": "error", "message": str(e)}, 500

if __name__ == "__main__":
    # ローカルテスト用
    import json
    
    class MockRequest:
        def get_json(self):
            return {}
    
    result = main(MockRequest())
    print(json.dumps(result, indent=2, ensure_ascii=False))