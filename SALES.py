from pyspark.sql.functions import avg, col, mean, substring, when
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("SALES").getOrCreate()
# Đọc file csv vào DataFrame
data = spark.read.csv("data/king-county-house-sales.csv", header=True, inferSchema=True)

print('-' * 150)
print('QUESTION 3:\n')

print('Cau 3.2:')
# Lấy ra các giá trị id riêng biệt
id = data.select('id').distinct()
num_id = id.count()
# Hiển thị kết quả
print('Tong so nha dang duoc ban la:', num_id)

print('\nCau 3.3:')
# Lấy ra các giá trị zipcode riêng biệt
zipcodes = data.select('zipcode').distinct()
# Hiển thị số lượng các vùng theo zipcode
num_zipcodes = zipcodes.count()
# Hiển thị kết quả
print('So vung duoc liet ke trong data la:', num_zipcodes)
print('Liet ke cac vung:')
zipcodes.show(num_zipcodes)

print('Cau 3.4:')
print('Dinh dang ban dau:')
data.select('price', 'sqft_living', 'floors').printSchema()
# Thay đổi định dạng các cột sang kiểu dữ liệu integer
dt = data.withColumn('price', col('price').cast(IntegerType())) \
    .withColumn('sqft_living', col('sqft_living').cast(IntegerType())) \
    .withColumn('floors', col('floors').cast(IntegerType()))
# Kiểm tra lại định dạng của CSDL
print('Dinh dang sau khi thay doi:')
dt.select('price', 'sqft_living', 'floors').printSchema()

print('Cau 3.5:')
# Tính giá trung bình nhà theo vùng
avg_price_by_region = data.groupBy('zipcode').agg(avg('price').alias("Gia trung binh"))
# Hiển thị kết quả
print('Gia trung binh nha theo vung:')
avg_price_by_region.show()

print('Cau 3.6:')
# Lọc ra các nhà có 3 tầng và được xây dựng sau năm 1970
filtered_df = data.filter((data.floors == '3') & (data.yr_built > 1970))
# Hiển thị kết quả
print('Nhung nha co 3 tang duoc xay dung sau nam 1970:')
filtered_df.select('id', 'floors', 'yr_built').show(10)

print('Cau 3.7:')
# Lọc các nhà có số phòng ngủ <= 3 và giá trị <= 250000$
filtered_data = data.filter((data.bedrooms <= 3) & (data.price <= 250000))
print('Nhung nha co duoi 3 phong ngu co gia tri nho hon 250000$:')
filtered_data.select('id', 'bedrooms', 'price').show(10)
# Tính tổng số nhà theo giá và loại tầng
house_count_by_floors_price = data.groupBy(['floors', 'price']).count()
# Tính giá trung bình của các nhà theo giá từng loại tầng
avg_price_by_floors = data.groupBy('floors').agg(mean('price'))
# Hiển thị kết quả
print("Liet ke tong so nha theo gia va loai tang:")
house_count_by_floors_price.show(10)
print("Gia trung binh cua cac nha theo gia tung loai tang:")
avg_price_by_floors.show()

print('Cau 3.8:')
# Lấy ra 4 kí tự đầu của cột date
df = data.withColumn('year', substring(data['date'], 1, 4))
houses_2015 = df.filter((df.year == 2015))
# Hiển thị kết quả
print('Nhung nha duoc dang ban vao nam 2015:')
houses_2015.select('id', 'date', 'year').show(10)

print('Cau 3.9:')
# Xoay giá trị trong cột "floors" thành nhiều cột và nhóm theo số lượng phòng ngủ
pivot_df = data.groupBy('bedrooms') \
                .pivot('floors') \
                .count() \
                .orderBy(col('bedrooms').asc())
# Hiển thị kết quả
pivot_df.show()

print('Cau 3.10:')
# Tạo cột mới dựa trên điều kiện của trường "sqft_living"
df_with_size = data.withColumn('size_category',
                    when(data['sqft_living'] <= 1000, 'small')
                    .when((data['sqft_living'] > 1000) & (data['sqft_living'] < 2000), 'medium')
                    .otherwise('large'))

# Hiển thị kết quả
df_with_size.select('id', 'sqft_living', 'size_category').show()
