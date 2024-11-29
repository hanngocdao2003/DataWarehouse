import requests
import json
import os
from datetime import datetime
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

import sys
sys.stdout.reconfigure(encoding='utf-8')

output_folder = "D:\\crawl"

url_showtime = "https://cinestar.com.vn/api/showTime/"
url_area = "https://cinestar.com.vn/api/cinema/"

response_showtime = requests.get(url_showtime)
response_address = requests.get(url_area)

data = []

# Kiểm tra nếu thư mục chưa tồn tại
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# Hàm lấy số lượng ghế đã đặt từ showtime ID
def fetch_seat_data(showtime_id):
    url = f"https://cinestar.com.vn/api/seat/?id_ShowTimes={showtime_id}&id_Server=1"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json().get('data', [])
            booked_seats = sum(1 for row in data for seat in row.get('rowseats', []) if seat.get('issold', False))
            total_seats = sum(len(row.get('rowseats', [])) for row in data)
            avaiable_seats = total_seats - booked_seats
            return booked_seats, avaiable_seats
        else:
            print(f"Failed to fetch seat data for showtime_id {showtime_id}: {response.status_code}")
            return 0, 0
    except Exception as e:
        print(f"Error fetching seat data for showtime_id {showtime_id}: {e}")
        return 0, 0

# Kiểm tra xem API có trả về dữ liệu thành công không
if response_showtime.status_code == 200 and response_address.status_code == 200:
    movies = response_showtime.json().get('data', [])
    addresss = response_address.json().get('data', [])

    # Tạo một danh sách các yêu cầu cần thực hiện
    requests_to_process = []

    for movie in movies:
        for schedule in movie['schedule']:
            for time in schedule['times']:
                # Lưu thông tin phim vào DataFrame
                for address in addresss:
                    if address['id'] == time['theater_id']:
                        requests_to_process.append({
                            "movie": movie,
                            "schedule": schedule,
                            "time": time,
                            "address": address
                        })

    # Hàm xử lý thông tin và tạo DataFrame
    def process_movie_request(request):
        movie = request["movie"]
        schedule = request["schedule"]
        time = request["time"]
        address = request["address"]

        booked_seats, avaiable_seats = fetch_seat_data(time['showtime_id'])

        return {
            "movie_name": movie['name_vn'],
            "director": movie['director'],
            "actor": movie['actor'],
            "nation": movie['country_name_vn'],
            "genre": movie['type_name_vn'],
            "time_m": movie['time_m'],
            "release_date": movie['release_date'],
            "end_date": movie['end_date'],
            "description": movie['brief_vn'],
            "date": schedule['date'],
            "show_time": time['time'],
            "theater_name": time['theater_name_vn'],
            "location_name": address['address'],
            "booked_seats": booked_seats,
            "avaiable_seats": avaiable_seats
        }

    # Sử dụng ThreadPoolExecutor để xử lý các yêu cầu đồng thời
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_request = {executor.submit(process_movie_request, request): request for request in requests_to_process}

        for future in as_completed(future_to_request):
            result = future.result()
            data.append(result)

    # Tạo DataFrame từ danh sách data
    df = pd.DataFrame(data)

    # Thêm cột date crawl
    df["Date_crawl"] = datetime.now().strftime("%Y-%m-%d")

    # Lấy ngày và thời gian hiện tại
    current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Lưu file .csv
    excel_filename = f"{output_folder}/movies_data_{current_datetime}.csv"
    df.to_csv(excel_filename, index=False, encoding="utf-8-sig")

    # Hiển thị DataFrame
    print(df.head())

else:
    print(f"Không thể lấy dữ liệu. Mã lỗi: {response_showtime.status_code}, {response_address.status_code}")
