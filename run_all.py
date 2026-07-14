import subprocess
import sys
import time

# Danh sách các bước chạy theo thứ tự
STEPS = [
    ("Ingestion - Tạo thư mục & nạp data vào HDFS", "spark_jobs/ingestion.py"),
    ("ETL", "spark_jobs/etl.py"),
    ("ML Models", "spark_jobs/ml_models.py"),
    ("Export to MongoDB", "spark_jobs/export_to_mongo.py"),
]

def run_step(name, script_path):
    print(f"\n{'='*60}")
    print(f"BẮT ĐẦU: {name} ({script_path})")
    print(f"{'='*60}\n")

    start = time.time()
    result = subprocess.run([sys.executable, script_path])
    elapsed = time.time() - start

    if result.returncode != 0:
        print(f"\n LỖI tại bước: {name} (mã lỗi {result.returncode})")
        print("Dừng pipeline. Sửa lỗi rồi chạy lại.")
        sys.exit(1)
    else:
        print(f"\n XONG: {name} ({elapsed:.1f}s)")

if __name__ == "__main__":
    print("BẮT ĐẦU CHẠY TOÀN BỘ PIPELINE\n")
    overall_start = time.time()

    for name, path in STEPS:
        run_step(name, path)

    total = time.time() - overall_start
    print(f"\nHOÀN TẤT TOÀN BỘ PIPELINE trong {total/60:.1f} phút")