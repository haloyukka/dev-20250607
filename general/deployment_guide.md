# Google Cloud Function SQL Serverデータ同期システム デプロイガイド

## 事前準備

### 1. GCPプロジェクトの設定

```bash
# プロジェクトIDを設定
export PROJECT_ID="your-project-id"

# プロジェクトを設定
gcloud config set project $PROJECT_ID

# 必要なAPIを有効化
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable sql-admin.googleapis.com
gcloud services enable logging.googleapis.com
gcloud services enable monitoring.googleapis.com
```

### 2. GCSバケットの作成

```bash
# バケット名を設定
export BUCKET_NAME="data-sync-bucket-$PROJECT_ID"

# バケットを作成
gsutil mb -l asia-northeast1 gs://$BUCKET_NAME

# バケットの詳細確認
gsutil ls -L gs://$BUCKET_NAME
```

### 3. BigQueryデータセットの作成

```bash
# データセットを作成
bq mk --location=asia-northeast1 --dataset $PROJECT_ID:data_sync

# データセット確認
bq ls
```

### 4. サービスアカウントの作成と権限設定

```bash
# サービスアカウント作成
gcloud iam service-accounts create data-sync-function \
    --display-name="Data Sync Function Service Account"

# BigQuery権限付与
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:data-sync-function@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataEditor"

# Cloud Storage権限付与
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:data-sync-function@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/storage.objectAdmin"

# Cloud Logging権限付与
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:data-sync-function@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/logging.logWriter"
```

## 環境変数設定ファイル

### env.yaml作成

```yaml
SQL_SERVER_HOST: "10.0.0.100"
SQL_SERVER_PORT: "1433"
SQL_SERVER_USER: "sync_user"
SQL_SERVER_PASSWORD: "your_password_here"
SQL_SERVER_DATABASE: "production_db"
BIGQUERY_PROJECT: "your-project-id"
BIGQUERY_DATASET: "data_sync"
BIGQUERY_LOCATION: "asia-northeast1"
GCS_BUCKET: "data-sync-bucket-your-project-id"
SYNC_TABLES_CONFIG: >
  {
    "orders": {
      "timestamp_column": "updated_at"
    },
    "products": {
      "timestamp_column": "modified_date"
    },
    "customers": {
      "timestamp_column": null
    }
  }
```

## デプロイコマンド

### 基本デプロイ

```bash
gcloud functions deploy data-sync-function \
    --runtime python311 \
    --trigger-http \
    --entry-point main \
    --memory 2GB \
    --timeout 540s \
    --env-vars-file env.yaml \
    --service-account data-sync-function@$PROJECT_ID.iam.gserviceaccount.com \
    --region asia-northeast1 \
    --max-instances 10 \
    --allow-unauthenticated
```

### セキュアなデプロイ（認証あり）

```bash
gcloud functions deploy data-sync-function \
    --runtime python311 \
    --trigger-http \
    --entry-point main \
    --memory 2GB \
    --timeout 540s \
    --env-vars-file env.yaml \
    --service-account data-sync-function@$PROJECT_ID.iam.gserviceaccount.com \
    --region asia-northeast1 \
    --max-instances 10 \
    --no-allow-unauthenticated
```

## Cloud Schedulerでの定期実行設定

### 6時間ごとの実行

```bash
# 認証なしの場合
gcloud scheduler jobs create http data-sync-job \
    --schedule="0 */6 * * *" \
    --uri="https://asia-northeast1-$PROJECT_ID.cloudfunctions.net/data-sync-function" \
    --http-method=POST \
    --time-zone="Asia/Tokyo"

# 認証ありの場合
gcloud scheduler jobs create http data-sync-job \
    --schedule="0 */6 * * *" \
    --uri="https://asia-northeast1-$PROJECT_ID.cloudfunctions.net/data-sync-function" \
    --http-method=POST \
    --time-zone="Asia/Tokyo" \
    --oidc-service-account-email="data-sync-function@$PROJECT_ID.iam.gserviceaccount.com"
```

## 動作確認とテスト

### 手動実行テスト

```bash
# HTTPトリガーで実行
curl -X POST "https://asia-northeast1-$PROJECT_ID.cloudfunctions.net/data-sync-function" \
     -H "Content-Type: application/json" \
     -d '{}'
```

### ログ確認

```bash
# Cloud Functionのログを確認
gcloud logging read "resource.type=cloud_function AND resource.labels.function_name=data-sync-function" \
    --limit=50 \
    --format="table(timestamp,severity,textPayload)"

# エラーログのみ確認
gcloud logging read "resource.type=cloud_function AND resource.labels.function_name=data-sync-function AND severity>=ERROR" \
    --limit=20
```

### BigQueryでの確認

```sql
-- 同期メタデータ確認
SELECT 
    table_name,
    last_sync_time,
    updated_at
FROM `your-project-id.data_sync.sync_metadata`
ORDER BY updated_at DESC;
```

### GCSファイル確認

```bash
# アップロードされたCSVファイル確認
gsutil ls gs://$BUCKET_NAME/

# 最新ファイルの内容確認
gsutil cat gs://$BUCKET_NAME/orders_$(date +%Y%m%d)*.csv | head -10
```

## トラブルシューティング

### よくあるエラーと対処法

#### 1. SQL Server接続エラー
```
Error: Unable to connect to SQL Server
```

**対処法:**
- ネットワーク接続確認
- SQL Server設定（TCP/IP有効化、ポート1433開放）
- 認証情報確認

```bash
# SQL Server接続テスト（Cloud Shellから）
telnet $SQL_SERVER_HOST 1433
```

#### 2. BigQuery権限エラー
```
Error: Access Denied: BigQuery
```

**対処法:**
```bash
# 権限再設定
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:data-sync-function@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataEditor"
```

#### 3. GCS権限エラー
```
Error: Access Denied: Cloud Storage
```

**対処法:**
```bash
# 権限再設定
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:data-sync-function@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/storage.objectAdmin"
```

#### 4. タイムアウトエラー
```
Error: Function timeout
```

**対処法:**
```bash
# タイムアウト時間を延長
gcloud functions deploy data-sync-function \
    --timeout 900s \
    --memory 4GB
```

### パフォーマンス最適化

#### メモリとタイムアウトの調整

```bash
# 大容量データ処理用設定
gcloud functions deploy data-sync-function \
    --memory 8GB \
    --timeout 540s \
    --max-instances 5
```

#### 並列処理の制限

```bash
# 同時実行数制限
gcloud functions deploy data-sync-function \
    --max-instances 3 \
    --min-instances 0
```

## 監視とアラート設定

### Cloud Monitoringアラートポリシー

```bash
# エラー率アラート作成
gcloud alpha monitoring policies create \
    --policy-from-file=monitoring/error-rate-policy.yaml
```

### monitoring/error-rate-policy.yaml

```yaml
displayName: "Data Sync Function Error Rate"
conditions:
  - displayName: "Error rate too high"
    conditionThreshold:
      filter: 'resource.type="cloud_function" AND resource.label.function_name="data-sync-function"'
      comparison: COMPARISON_GREATER_THAN
      thresholdValue: 0.1
      duration: 300s
notificationChannels:
  - "projects/your-project-id/notificationChannels/YOUR_CHANNEL_ID"
```

## セキュリティ強化

### Secret Managerの使用

```bash
# パスワードをSecret Managerに保存
echo -n "your_sql_password" | gcloud secrets create sql-server-password --data-file=-

# 権限付与
gcloud secrets add-iam-policy-binding sql-server-password \
    --member="serviceAccount:data-sync-function@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/secretmanager.secretAccessor"
```

### main.pyでの使用例

```python
from google.cloud import secretmanager

def get_secret(secret_id):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

# 使用例
sql_password = get_secret("sql-server-password")
```

## バックアップとリストア

### 設定のバックアップ

```bash
# Cloud Function設定をエクスポート
gcloud functions describe data-sync-function \
    --region=asia-northeast1 \
    --format="export" > function-config.yaml
```

### 災害復旧手順

1. **設定復元**
```bash
gcloud functions deploy data-sync-function \
    --source=. \
    --runtime=python311 \
    --env-vars-file=env.yaml
```

2. **スケジューラー復元**
```bash
gcloud scheduler jobs create http data-sync-job \
    --schedule="0 */6 * * *" \
    --uri="https://asia-northeast1-$PROJECT_ID.cloudfunctions.net/data-sync-function"
```

## 本番運用チェックリスト

- [ ] すべてのAPIが有効化されている
- [ ] サービスアカウントに必要な権限が付与されている
- [ ] 環境変数が正しく設定されている
- [ ] SQL Server接続が確立できる
- [ ] BigQueryデータセットが作成されている
- [ ] GCSバケットが作成されている
- [ ] Cloud Schedulerが設定されている
- [ ] ログ監視が設定されている
- [ ] アラート通知が設定されている
- [ ] セキュリティポリシーが適用されている