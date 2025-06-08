# Google Cloud Function SQL Server to BigQuery データ同期システム 仕様書

## 1. システム概要

### 1.1 目的
SQL ServerからBigQueryへの効率的なデータ同期を実現するGoogle Cloud Functionベースのシステム

### 1.2 主要機能
- SQL Serverから複数テーブルのデータ抽出
- タイムスタンプベースの差分同期
- GCSへのCSVファイル出力（JST形式タイムスタンプ付き）
- BigQueryでの同期メタデータ管理
- 包括的なエラーハンドリング

## 2. アーキテクチャ

### 2.1 システム構成
```
[SQL Server] → [Cloud Function] → [Cloud Storage] → [BigQuery]
                      ↓
                [Sync Metadata Table]
```

### 2.2 使用技術
- **実行環境**: Google Cloud Functions (Python 3.11)
- **データベース接続**: SQLAlchemy + pyodbc
- **データ処理**: pandas
- **ストレージ**: Google Cloud Storage
- **メタデータ管理**: BigQuery

## 3. 環境変数設定

### 3.1 必須環境変数
| 変数名 | 説明 | 例 |
|--------|------|-----|
| `SQL_SERVER_HOST` | SQL Serverホスト名 | `10.0.0.100` |
| `SQL_SERVER_PORT` | SQL Serverポート | `1433` |
| `SQL_SERVER_USER` | SQL Serverユーザー名 | `sync_user` |
| `SQL_SERVER_PASSWORD` | SQL Serverパスワード | `password123` |
| `SQL_SERVER_DATABASE` | SQL Serverデータベース名 | `production_db` |
| `BIGQUERY_PROJECT` | BigQueryプロジェクトID | `my-project-123` |
| `BIGQUERY_DATASET` | BigQueryデータセット名 | `data_sync` |
| `BIGQUERY_LOCATION` | BigQueryロケーション | `asia-northeast1` |
| `GCS_BUCKET` | GCSバケット名 | `data-sync-bucket` |
| `SYNC_TABLES_CONFIG` | 同期テーブル設定（JSON） | 下記参照 |

### 3.2 同期テーブル設定例
```json
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

## 4. データ同期ロジック

### 4.1 同期フロー
1. **テーブル設定読み込み**: 環境変数から同期対象テーブルを取得
2. **前回同期時刻確認**: BigQueryのメタデータテーブルから確認
3. **データ抽出**: 
   - タイムスタンプカラムあり → 差分データのみ
   - タイムスタンプカラムなし → 全件データ
4. **CSV生成**: pandasでCSV形式に変換
5. **GCS保存**: JST形式タイムスタンプ付きファイル名で保存
6. **メタデータ更新**: 最新同期時刻をBigQueryに記録

### 4.2 ファイル命名規則
```
{テーブル名}_{YYYYMMDD}_{HHMMSS}.csv
例: orders_20250607_143025.csv
```

### 4.3 差分同期条件
```sql
SELECT * FROM {table_name}
WHERE {timestamp_column} > {last_sync_time}
ORDER BY {timestamp_column}
```

## 5. データベーススキーマ

### 5.1 同期メタデータテーブル
**テーブル名**: `{project}.{dataset}.sync_metadata`

| カラム名 | データ型 | 説明 |
|----------|----------|------|
| `table_name` | STRING | テーブル名（主キー） |
| `last_sync_time` | TIMESTAMP | 前回同期時刻 |
| `updated_at` | TIMESTAMP | メタデータ更新時刻 |

### 5.2 クラスタリング
- **クラスタリングフィールド**: `table_name`

## 6. エラーハンドリング

### 6.1 エラー種別と対応
| エラー種別 | 対応方法 |
|------------|----------|
| SQL Server接続エラー | 接続設定確認、ネットワーク確認 |
| データ抽出エラー | SQLクエリ確認、権限確認 |
| GCS保存エラー | バケット権限確認、容量確認 |
| BigQuery操作エラー | データセット権限確認 |

### 6.2 ログレベル
- **INFO**: 正常処理の進行状況
- **WARNING**: 非致命的な問題
- **ERROR**: 処理停止を伴うエラー

## 7. パフォーマンス考慮事項

### 7.1 最適化ポイント
- **接続プール**: SQLAlchemyの接続プール活用
- **メモリ効率**: pandas chunksizeオプション（大容量データ対応）
- **並列処理**: 複数テーブルの独立同期

### 7.2 制限事項
- **Cloud Function実行時間**: 最大9分（HTTPトリガー時）
- **メモリ制限**: 最大8GB
- **CSV最大サイズ**: 推奨500MB以下

## 8. セキュリティ

### 8.1 認証・認可
- **IAM**: Cloud Functionサービスアカウント
- **権限**: 
  - BigQuery: データ編集者
  - Cloud Storage: オブジェクト管理者
  - Cloud SQL: クライアント

### 8.2 接続セキュリティ
- **TLS**: SQL Server接続にTLS使用
- **認証情報**: Secret Managerで管理推奨

## 9. 監視・運用

### 9.1 監視項目
- **実行成功率**: Cloud Functions指標
- **実行時間**: 処理時間監視
- **エラー率**: ログベースアラート

### 9.2 ログ確認
```bash
# Cloud Loggingでのログ確認
gcloud logging read "resource.type=cloud_function" --limit=50
```

## 10. デプロイ手順

### 10.1 事前準備
1. Google Cloud Project作成
2. 必要APIの有効化
   - Cloud Functions API
   - BigQuery API
   - Cloud Storage API
3. サービスアカウント作成と権限設定

### 10.2 デプロイコマンド
```bash
gcloud functions deploy data_sync_function \
  --runtime python311 \
  --trigger-http \
  --entry-point main \
  --memory 2GB \
  --timeout 540s \
  --set-env-vars SQL_SERVER_HOST=10.0.0.100,... \
  --region asia-northeast1
```

## 11. 運用ガイドライン

### 11.1 定期実行設定
Cloud Schedulerを使用した定期実行例：
```bash
gcloud scheduler jobs create http data-sync-job \
  --schedule="0 */6 * * *" \
  --uri=https://asia-northeast1-{project}.cloudfunctions.net/data_sync_function \
  --http-method=POST
```

### 11.2 障害時対応
1. **ログ確認**: Cloud Loggingで詳細確認
2. **リトライ**: 一時的障害の場合は手動再実行
3. **データ整合性確認**: 同期前後のレコード数比較

## 12. 今後の拡張予定

### 12.1 予定機能
- **スキーマ変更検知**: テーブル構造変更の自動検知
- **データ品質チェック**: 同期データの妥当性検証
- **増分バックアップ**: GCS上のデータ世代管理
- **並列処理**: 大量テーブル対応の並列同期

### 12.2 パフォーマンス改善
- **ストリーミング処理**: 大容量データ対応
- **圧縮**: CSV圧縮オプション
- **パーティション**: BigQueryテーブルパーティション対応