# Upload 9 file CSV tu thu muc data/ len HDFS
# Chay: powershell -ExecutionPolicy Bypass -File upload_to_hdfs.ps1

$env:JAVA_HOME = "C:\Java\jdk1.8.0_491"
$env:HADOOP_HOME = "C:\hadoop"

$DATA_DIR = "C:\Users\Admin\.gemini\antigravity\scratch\olist-bigdata-project\data"
$HDFS_BASE = "/user/bigdata/olist/raw"

Write-Host "Upload du lieu len HDFS..."

if (-not (Test-Path $DATA_DIR)) {
    Write-Host "Khong tim thay thu muc data/ - Hay download dataset Olist tu Kaggle"
    exit 1
}

$filemap = @{}
$filemap["olist_orders_dataset.csv"] = "orders"
$filemap["olist_order_items_dataset.csv"] = "order_items"
$filemap["olist_order_payments_dataset.csv"] = "order_payments"
$filemap["olist_order_reviews_dataset.csv"] = "order_reviews"
$filemap["olist_customers_dataset.csv"] = "customers"
$filemap["olist_products_dataset.csv"] = "products"
$filemap["olist_sellers_dataset.csv"] = "sellers"
$filemap["olist_geolocation_dataset.csv"] = "geolocation"
$filemap["product_category_name_translation.csv"] = "category_translation"

foreach ($key in $filemap.Keys) {
    $localPath = Join-Path $DATA_DIR $key
    $hdfsPath = "$HDFS_BASE/$($filemap[$key])/"
    if (Test-Path $localPath) {
        Write-Host "  Uploading $key -> $hdfsPath"
        & hdfs dfs -put -f $localPath $hdfsPath
        Write-Host "    Done"
    } else {
        Write-Host "  Khong tim thay $key - bo qua"
    }
}

Write-Host ""
Write-Host "Upload hoan tat!"
Write-Host ""
Write-Host "Dung luong tren HDFS:"
& hdfs dfs -du -h $HDFS_BASE
