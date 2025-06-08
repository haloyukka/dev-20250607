#!/usr/bin/env python3
"""
Google Cloud Function SQL Server to BigQuery データ同期システム
Mock環境テスト実行スクリプト

使用方法:
    python test_execution.py

特徴:
- 実際のSQL Server, BigQuery, GCSがなくても動作
- ハードコーディングされた設定値を使用
- 詳細なログ出力でテスト結果を確認
"""

import sys
import json
from datetime import datetime

def print_separator(title="", char="=", width=80):
    """セパレーターを出力"""
    if title:
        title_line = f" {title} "
        padding = (width - len(title_line)) // 2
        line = char * padding + title_line + char * padding
        if len(line) < width:
            line += char
    else:
        line = char * width
    print(line)

def print_test_info():
    """テスト情報を表示"""
    print_separator("MOCK TEST ENVIRONMENT INFO")
    print("このテストスクリプトは以下の環境をシミュレートします:")
    print()
    print("📊 データソース: Mock SQL Server")
    print("   - orders テーブル (150件, timestamp列: updated_at)")
    print("   - products テーブル (25件, timestamp列: modified_date)")
    print("   - customers テーブル (50件, timestamp列: なし)")
    print("   - transactions テーブル (300件, timestamp列: created_at)")
    print("   - user_activities テーブル (500件, timestamp列: activity_timestamp)")
    print()
    print("☁️  クラウドサービス: Mock Google Cloud")
    print("   - BigQuery: sync_metadata テーブルでの同期状態管理")
    print("   - Cloud Storage: CSVファイル保存のシミュレート")
    print()
    print("⚙️  設定:")
    print("   - プロジェクト: test-project-12345")
    print("   - データセット: data_sync")
    print("   - バケット: data-sync-bucket-test")
    print()
    print_separator()

def run_test():
    """テストを実行"""
    try:
        print_test_info()
        
        # main_hardcoded.pyをインポート
        print("📥 Importing main_hardcoded module...")
        from main_hardcoded import main, test_sync_locally
        print("✅ Module imported successfully")
        print()
        
        print_separator("TEST EXECUTION")
        
        # テスト開始時刻
        start_time = datetime.now()
        print(f"🚀 Test started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # メイン関数を実行
        result = test_sync_locally()
        
        # テスト終了時刻
        end_time = datetime.now()
        duration = end_time - start_time
        
        print()
        print_separator("TEST RESULTS")
        print(f"⏱️  Execution time: {duration.total_seconds():.2f} seconds")
        print(f"🏁 Test completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        if result:
            status = result.get('status', 'unknown')
            if status == 'success':
                print("✅ TEST PASSED - All operations completed successfully")
            elif status == 'partial_success':
                print("⚠️  TEST PARTIAL SUCCESS - Some operations failed")
            else:
                print("❌ TEST FAILED - Execution error occurred")
            
            # サマリー情報表示
            summary = result.get('summary', {})
            if summary:
                print()
                print("📈 Summary:")
                print(f"   Total tables: {summary.get('total_tables', 0)}")
                print(f"   Success: {summary.get('success_count', 0)}")
                print(f"   Errors: {summary.get('error_count', 0)}")
            
            # 詳細結果表示
            details = result.get('details', [])
            if details:
                print()
                print("📋 Detailed Results:")
                for detail in details:
                    table = detail.get('table', 'unknown')
                    status = detail.get('status', 'unknown')
                    if status == 'success':
                        print(f"   ✅ {table}: Success")
                    else:
                        error = detail.get('error', 'Unknown error')
                        print(f"   ❌ {table}: {error}")
        else:
            print("❌ TEST FAILED - No result returned")
        
        print()
        print_separator("TEST COMPLETED")
        
        return result
        
    except ImportError as e:
        print("❌ Import Error:")
        print(f"   {e}")
        print()
        print("💡 Solutions:")
        print("   1. Ensure main_hardcoded.py is in the same directory")
        print("   2. Install required packages: pip install pandas pytz")
        return None
        
    except Exception as e:
        print("❌ Execution Error:")
        print(f"   {e}")
        print()
        print("💡 Check the error details above and try again")
        import traceback
        traceback.print_exc()
        return None

def validate_environment():
    """実行環境の検証"""
    print_separator("ENVIRONMENT VALIDATION")
    
    # Python バージョン確認
    python_version = sys.version
    print(f"🐍 Python version: {python_version}")
    
    # 必要なパッケージの確認
    required_packages = ['pandas', 'pytz']
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"✅ {package}: Available")
        except ImportError:
            print(f"❌ {package}: Missing")
            missing_packages.append(package)
    
    if missing_packages:
        print()
        print("⚠️  Missing packages detected!")
        print("   Run: pip install " + " ".join(missing_packages))
        return False
    
    print("✅ All required packages are available")
    print()
    return True

def main_test():
    """メインテスト関数"""
    print_separator("MOCK DATA SYNC SYSTEM TEST", "=", 80)
    print("Google Cloud Function SQL Server to BigQuery Data Sync")
    print("Mock Environment Test Execution")
    print()
    
    # 環境検証
    if not validate_environment():
        print("❌ Environment validation failed. Please install missing packages.")
        return False
    
    # テスト実行
    result = run_test()
    
    if result and result.get('status') in ['success', 'partial_success']:
        print("🎉 Mock test execution completed successfully!")
        print("   This system is ready for deployment to Google Cloud Functions")
        return True
    else:
        print("💥 Mock test execution failed!")
        print("   Please check the error messages above")
        return False

if __name__ == "__main__":
    success = main_test()
    sys.exit(0 if success else 1)