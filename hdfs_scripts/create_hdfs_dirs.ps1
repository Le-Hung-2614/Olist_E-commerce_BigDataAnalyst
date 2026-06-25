# Tao cau truc thu muc HDFS cho du an Olist
# Chay: .\hdfs_scripts\create_hdfs_dirs.ps1

$env:JAVA_HOME = "C:\Java\jdk1.8.0_491"
$env:HADOOP_HOME = "C:\hadoop"

Write-Host "Tao cau truc thu muc HDFS..."

$dirs = @(
    "/user/bigdata/olist/raw/orders"
    "/user/bigdata/olist/raw/order_items"
    "/user/bigdata/olist/raw/order_payments"
    "/user/bigdata/olist/raw/order_reviews"
    "/user/bigdata/olist/raw/customers"
    "/user/bigdata/olist/raw/products"
    "/user/bigdata/olist/raw/sellers"
    "/user/bigdata/olist/raw/geolocation"
    "/user/bigdata/olist/raw/category_translation"
    "/user/bigdata/olist/processed/merged_orders"
    "/user/bigdata/olist/processed/rfm_features"
    "/user/bigdata/olist/processed/customer_features"
    "/user/bigdata/olist/models/kmeans_segmentation"
    "/user/bigdata/olist/models/churn_classifier"
    "/user/bigdata/olist/models/review_predictor"
)

foreach ($dir in $dirs) {
    Write-Host "  Creating $dir"
    & hdfs dfs -mkdir -p $dir
}

Write-Host ""
Write-Host "Da tao xong cau truc thu muc HDFS!"
Write-Host ""
Write-Host "Kiem tra:"
& hdfs dfs -ls -R /user/bigdata/olist/
