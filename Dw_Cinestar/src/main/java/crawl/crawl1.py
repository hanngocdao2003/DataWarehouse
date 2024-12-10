import requests
from datetime import datetime
import pandas as pd
import os
import pytz
import warnings

# Tắt tất cả các cảnh báo
warnings.filterwarnings("ignore")

output_folder = "A:/workspace_all_tools/IntelliJ_space/DW/Dw_Cinestar/src/main/fileCrawlCsv"

url = "https://cinestar.com.vn/api/showTime/"  # Thay bằng URL của API
url_area = "https://cinestar.com.vn/api/cinema/"

response = requests.get(url)
response_address = requests.get(url_area)

data = []
count = 0

if not os.path.exists(output_folder):
    os.makedirs(output_folder)

if response.status_code == 200:
    movies = response.json().get('data', [])
    addresss = response_address.json().get('data', [])

    for movie in movies:
        for schedule in movie['schedule']:
            for time in schedule['times']:
                # Mỗi lịch chiếu (schedule) và thời gian chiếu (time) sẽ được lưu thành một dòng
                for address in addresss:
                    if(address['id'] == time['theater_id']):
                        data.append({
                            "movie_name": movie['name_vn'],
                            "director": movie['director'],
                            "actor": movie['actor'],
                            "nation": movie['country_name_vn'],
                            "genre": movie['type_name_vn'],
                            "time_m": movie['time_m'],
                            "release_date": movie['release_date'],
                            "end_date": movie['end_date'],
                            "description": movie['brief_vn'],
                            "show_time": time['time'],
                            "theater_name": time['theater_name_vn'],
                            "location_name": address['address']
                        })

    # Tạo DataFrame từ danh sách data
    df = pd.DataFrame(data)
    count = count + 1

    # Thêm cột date crawl
    df["Date"] = datetime.now().strftime("%Y-%m-%d")

    # ĐỔi định dạng
    df['release_date'] = pd.to_datetime(df['release_date'], format='%m/%d/%Y %I:%M:%S %p')

    # Định dạng lại datetime thành 'YYYY-MM-DD HH:mm:ss' mà không thay đổi dữ liệu
    df['release_date'] = df['release_date'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Tương tự với end_date
    df['end_date'] = pd.to_datetime(df['end_date'], format='%m/%d/%Y %I:%M:%S %p')
    df['end_date'] = df['end_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df['show_time'] = pd.to_datetime(df['show_time']).dt.strftime('%Y-%m-%d %H:%M:%S')

    # Lấy ngày và thời gian hiện tại
    tz = pytz.timezone('Asia/Ho_Chi_Minh')  # Adjust timezone as necessary

    # Get current time in specified timezone
    current_datetime = datetime.now(tz).strftime('%Y%m%d_%H%M%S')

    # Lưu file .csv
    excel_filename = f"{output_folder}/movies_data_{current_datetime}.csv"
    df.to_csv(excel_filename, index=False)

    print(excel_filename)
else:
    print(f"Không thể lấy dữ liệu. Mã lỗi: {response.status_code}")
