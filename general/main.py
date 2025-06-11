import os
import csv
import io
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import pytz
import pymssql
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.sql.connector import Connector
from google.cloud import logging

# Cloud Logging設定
logging_client = logging.Client()
logging_client.setup_logging()

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
        self.db_conn = None
        self.logger = logging_client.logger('data_sync')
        
    def create_db_connection(self) -> pymssql.Connection:
        """SQL Server接続を作成"""
        try:
            conn = pymssql.connect(
                server=self.config.sql_server_host,
                port=int(self.config.sql_server_port),
                user=self.config.sql_server_user,
                password=self.config.sql_server_password,
                database=self.config.sql_server_database
            )
            
            self.logger.log_text("SQL Server接続を作成しました", severity="INFO")
            return conn
            
        except Exception as e:
            self.logger.log_text(f"SQL Server接続作成エラー: {e}", severity="ERROR")
            raise

    def get_table_columns(self, table_name: str) -> List[str]:
        """テーブルのカラム一覧を取得"""
        try:
            if not self.db_conn:
                raise ValueError("データベース接続が初期化されていません")

            cursor = self.db_conn.cursor()
            cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
            columns = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return columns
            
        except Exception as e:
            self.logger.log_text(f"テーブル {table_name} のカラム取得エラー: {e}", severity="ERROR")
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
            self.logger.log_text(f"前回同期時刻取得エラー (テーブル: {table_name}): {e}", severity="WARNING")
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
            self.logger.log_text(f"同期メタデータを更新しました: {table_name}", severity="INFO")
            
        except Exception as e:
            self.logger.log_text(f"同期メタデータ更新エラー: {e}", severity="ERROR")
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
            self.logger.log_text("sync_metadataテーブルを確認/作成しました", severity="INFO")
            
        except Exception as e:
            self.logger.log_text(f"sync_metadataテーブル作成エラー: {e}", severity="ERROR")
            raise

    def extract_data(self, table_name: str, timestamp_column: Optional[str]) -> List[Dict[str, Any]]:
        """SQL Serverからデータを抽出"""
        try:
            if not self.db_conn:
                raise ValueError("データベース接続が初期化されていません")

            cursor = self.db_conn.cursor(as_dict=True)
            
            if timestamp_column:
                # タイムスタンプカラムがある場合は差分抽出
                last_sync = self.get_last_sync_time(table_name)
                
                if last_sync:
                    query = f"""
                    SELECT * FROM {table_name}
                    WHERE {timestamp_column} > %s
                    ORDER BY {timestamp_column}
                    """
                    cursor.execute(query, (last_sync,))
                    data = cursor.fetchall()
                    self.logger.log_text(f"差分データを抽出しました: {table_name} ({len(data)}件)", severity="INFO")
                else:
                    query = f"SELECT * FROM {table_name} ORDER BY {timestamp_column}"
                    cursor.execute(query)
                    data = cursor.fetchall()
                    self.logger.log_text(f"初回全件データを抽出しました: {table_name} ({len(data)}件)", severity="INFO")
            else:
                # タイムスタンプカラムがない場合は全件抽出
                query = f"SELECT * FROM {table_name}"
                cursor.execute(query)
                data = cursor.fetchall()
                self.logger.log_text(f"全件データを抽出しました: {table_name} ({len(data)}件)", severity="INFO")
            
            cursor.close()
            return data
            
        except Exception as e:
            self.logger.log_text(f"データ抽出エラー (テーブル: {table_name}): {e}", severity="ERROR")
            raise

    def save_to_gcs(self, data: List[Dict[str, Any]], table_name: str) -> str:
        """データをCSVとしてGCSに保存"""
        try:
            if not data:
                self.logger.log_text(f"データが空のため、GCSへの保存をスキップします: {table_name}", severity="INFO")
                return ""
                
            # JST タイムスタンプ付きファイル名
            now_jst = datetime.now(JST)
            filename = f"{table_name}_{now_jst.strftime('%Y%m%d_%H%M%S')}.csv"
            
            # CSVデータをメモリ上で作成
            csv_buffer = io.StringIO()
            if data:
                writer = csv.DictWriter(csv_buffer, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
            
            # GCSにアップロード
            bucket = self.storage_client.bucket(self.config.gcs_bucket)
            blob = bucket.blob(filename)
            blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
            
            self.logger.log_text(f"CSVファイルをGCSに保存しました: gs://{self.config.gcs_bucket}/{filename}", severity="INFO")
            return filename
            
        except Exception as e:
            self.logger.log_text(f"GCS保存エラー: {e}", severity="ERROR")
            raise

    def get_max_timestamp(self, data: List[Dict[str, Any]], timestamp_column: str) -> Optional[datetime]:
        """データから最大タイムスタンプを取得"""
        try:
            if not data or timestamp_column not in data[0]:
                return None
                
            max_ts = max(row[timestamp_column] for row in data if row[timestamp_column] is not None)
            
            if max_ts is None:
                return None
                
            if isinstance(max_ts, datetime):
                return max_ts
            else:
                return datetime.fromisoformat(str(max_ts))
                
        except Exception as e:
            self.logger.log_text(f"最大タイムスタンプ取得エラー: {e}", severity="ERROR")
            return None

    def sync_table(self, table_name: str, table_config: Dict[str, Any]):
        """単一テーブルの同期を実行"""
        try:
            self.logger.log_text(f"テーブル同期開始: {table_name}", severity="INFO")
            
            timestamp_column = table_config.get('timestamp_column')
            
            # データ抽出
            data = self.extract_data(table_name, timestamp_column)
            
            if not data:
                self.logger.log_text(f"同期対象データなし: {table_name}", severity="INFO")
                return
            
            # GCSに保存
            gcs_filename = self.save_to_gcs(data, table_name)
            
            # 最大タイムスタンプを取得
            max_timestamp = None
            if timestamp_column:
                max_timestamp = self.get_max_timestamp(data, timestamp_column)
            
            # 同期メタデータを更新
            self.update_sync_metadata(table_name, max_timestamp)
            
            self.logger.log_text(f"テーブル同期完了: {table_name} (ファイル: {gcs_filename})", severity="INFO")
            
        except Exception as e:
            self.logger.log_text(f"テーブル同期エラー: {table_name} - {e}", severity="ERROR")
            raise

    def run_sync(self):
        """全体の同期プロセスを実行"""
        try:
            self.logger.log_text("データ同期処理を開始します", severity="INFO")
            
            # SQL Server接続
            self.db_conn = self.create_db_connection()
            
            # 各テーブルを同期
            for table_name, table_config in self.config.sync_tables.items():
                try:
                    self.sync_table(table_name, table_config)
                except Exception as e:
                    self.logger.log_text(f"テーブル {table_name} の同期でエラーが発生しました: {e}", severity="ERROR")
                    # 他のテーブルの同期は続行
                    continue
            
            self.logger.log_text("データ同期処理が完了しました", severity="INFO")
            
        except Exception as e:
            self.logger.log_text(f"データ同期処理でエラーが発生しました: {e}", severity="ERROR")
            raise
        finally:
            # リソースクリーンアップ
            if self.db_conn:
                self.db_conn.close()

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
        logger = logging_client.logger('data_sync')
        logger.log_text(f"Cloud Function実行エラー: {e}", severity="ERROR")
        return {"status": "error", "message": str(e)}, 500

if __name__ == "__main__":
    # ローカルテスト用
    import json
    
    class MockRequest:
        def get_json(self):
            return {}
    
    result = main(MockRequest())
    print(json.dumps(result, indent=2, ensure_ascii=False))