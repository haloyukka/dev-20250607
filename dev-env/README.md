# Google Cloud Function データ同期システム - Mockテスト環境

## 概要

このテスト環境は、実際のMS SQL Server、BigQuery、Cloud Storageがない環境でも、データ同期システムの動作確認を行うことができます。

## 特徴

### ✅ **完全なMock環境**
- SQL Server接続をシミュレート
- BigQueryクエリ実行をシミュレート  
- Cloud Storage操作をシミュレート
- リアルなテストデータを自動生成

### 📊 **テストデータ**
| テーブル名 | 件数 | タイムスタンプ列 | 説明 |
|------------|------|------------------|------|
| orders | 150件 | updated_at | 注文データ（差分同期対象） |
| products | 25件 | modified_date | 商品データ（差分同期対象） |
| customers | 50件 | なし | 顧客データ（全件同期対象） |
| transactions | 300件 | created_at | 取引データ（差分同期対象） |
| user_activities | 500件 | activity_timestamp | ユーザー活動データ（差分同期対象） |

### 🔧 **ハードコーディング設定**
環境変数ではなく、コード内にテスト用設定を直接記述：

```python
HARDCODED_CONFIG = {
    "USE_MOCK": True,
    "BIGQUERY_PROJECT": "test-project-12345",
    "BIGQUERY_DATASET": "data_sync",
    "GCS_BUCKET": "data-sync-bucket-test",
    # その他の設定...
}
```

## 前提条件

### 必要なPythonパッケージ
```bash
pip install pandas pytz
```

### 推奨環境
- Python 3.8以上
- メモリ: 512MB以上（テストデータ生成のため）

## ファイル構成

```
mock-test-environment/
├── main_hardcoded.py          # メインシステム（Mock対応）
├── test_execution.py          # テスト実行スクリプト
├── requirements.txt      # テスト用依存関係
└── README.md            # このファイル
```

## 実行方法

### 1. 基本実行
```bash
python test_execution.py
```

### 2. メインモジュール直接実行
```bash
python main_hardcoded.py
```

### 3. 対話的実行
```python
# Python インタープリターで
from main_hardcoded import test_sync_locally
result = test_sync_locally()
```

## 実行結果の見方

### 正常実行時の出力例
```
==========================================
Data sync process started
Mode: Mock
Target tables: ['orders', 'products', 'customers', 'transactions', 'user_activities']
==========================================

=== Table sync started: orders ===
Mock returned last sync time for orders: 2025-06-07 04:30:25.123456+00:00
Mock differential data extracted: orders (45 records) since 2025-06-07 04:30:25.123456+00:00
Mock file uploaded: gs://data-sync-bucket-test/orders_20250607_143025.csv
  - Size: 12.34 KB
  - Lines: 46
Mock sync metadata updated: orders -> 2025-06-07 06:15:42.789012+00:00
=== Table sync completed: orders (File: orders_20250607_143025.csv) ===

...（他のテーブルも同様）

==========================================
Data sync process completed
Summary: 5 success, 0 errors
  ✓ orders: Success
  ✓ products: Success
  ✓ customers: Success
  ✓ transactions: Success
  ✓ user_activities: Success
==========================================
```

### 実行結果の詳細

#### ✅ 成功した場合
- `status: "success"` または `status: "partial_success"`
- 各テーブルの処理件数とファイル名が表示
- CSVファイルのサイズと行数が表示
- 同期メタデータの更新状況が表示

#### ❌ エラーが発生した場合
- `status: "error"`
- エラーメッセージとスタックトレースが表示
- 失敗したテーブル名と原因が特定される

## テストデータの詳細

### 1. ordersテーブル
```csv
order_id,customer_id,product_id,quantity,price,status,order_date,updated_at
ORD000001,CUST015,PROD008,3,2456.78,completed,2025-05-15,2025-06-07 05:30:15
...
```

### 2. productsテーブル
```csv
product_id,product_name,category,description,price,stock_quantity,is_active,modified_date
PROD001,Product 1,Electronics,High quality product 1,1299.99,85,True,2025-06-05 12:15:30
...
```

### 3. customersテーブル（timestamp列なし）
```csv
customer_id,customer_name,email,phone,prefecture,address,birth_date,registration_date,is_premium
CUST001,田中1,customer1@example.com,090-1234-5678,東京都,123番地,1985-03-15,2024-08-20,True
...
```

## Mock機能の詳細

### SQL Server Mock (`MockSQLServerEngine`)
- **接続シミュレート**: 実際のTCP接続なしで動作
- **クエリ実行**: WHERE句のタイムスタンプフィルタリングをサポート
- **データ生成**: リアルな業務データを自動生成

### BigQuery Mock (`MockBigQueryClient`)
- **クエリ実行**: SELECT/MERGE文のシミュレート
- **メタデータ管理**: sync_metadataテーブルの状態管理
- **タイムスタンプ追跡**: 前回同期時刻の記録と取得

### Cloud Storage Mock (`MockStorageClient`)
- **ファイルアップロード**: CSVデータのメモリ内保存
- **バケット管理**: 複数バケットの管理
- **ログ出力**: アップロードサイズと内容の詳細表示

## カスタマイズ方法

### 1. テストデータの変更
`main_hardcoded.py`の`_generate_mock_data()`メソッドを編集：

```python
def _generate_mock_data(self) -> Dict[str, pd.DataFrame]:
    # データ件数を変更
    for i in range(500):  # 元: 150件 → 500件に増加
        orders_data.append({
            # データ内容をカスタマイズ
        })
```

### 2. 設定値の変更
`HARDCODED_CONFIG`辞書を編集：

```python
HARDCODED_CONFIG = {
    "USE_MOCK": True,
    "BIGQUERY_PROJECT": "your-test-project",  # プロジェクト名変更
    "GCS_BUCKET": "your-test-bucket",         # バケット名変更
    # 新しいテーブルを追加
    "SYNC_TABLES_CONFIG": {
        "your_new_table": {
            "timestamp_column": "your_timestamp_column"
        }
    }
}
```

### 3. 新しいテーブルの追加
1. `_generate_mock_data()`でテストデータを定義
2. `SYNC_TABLES_CONFIG`に設定を追加
3. テスト実行で動作確認

## トラブルシューティング

### よくある問題と解決方法

#### 1. ImportError: No module named 'pandas'
```bash
pip install pandas pytz
```

#### 2. メモリ不足エラー
テストデータ件数を減らしてください：
```python
# _generate_mock_data()内で件数を調整
for i in range(50):  # 元: 150 → 50に減少
```

#### 3. 文字化け（Windowsの場合）
コンソールのエンコーディングを設定：
```bash
chcp 65001  # UTF-8に設定
python test_execution.py
```

#### 4. タイムゾーンエラー
pytzパッケージが正しくインストールされているか確認：
```bash
pip install --upgrade pytz
```

## 実際のシステムとの違い

| 項目 | Mock環境 | 実際のシステム |
|------|----------|----------------|
| SQL Server接続 | メモリ内データ | 実際のTCP接続 |
| BigQueryクエリ | ログ出力のみ | 実際のクエリ実行 |
| CSV保存 | メモリ内保存 | GCSへの実際のアップロード |
| 認証 | 不要 | サービスアカウント必須 |
| ネットワーク | 不要 | インターネット接続必須 |

## 次のステップ

Mock環境でのテストが成功したら：

1. **実際の環境設定**: `HARDCODED_CONFIG["USE_MOCK"] = False`に変更
2. **認証設定**: Google Cloudサービスアカウントの設定
3. **ネットワーク設定**: SQL Server接続とファイアウォール設定
4. **デプロイ**: Google Cloud Functionsへのデプロイ

## サポート

Mock環境での問題やカスタマイズに関するご質問は、以下を確認してください：

1. エラーメッセージの詳細確認
2. Python環境とパッケージバージョンの確認
3. テストデータ生成ロジックの確認
4. ログ出力の詳細分析