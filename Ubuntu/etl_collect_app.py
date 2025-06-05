"""# 필요 라이브러리 import"""

import requests
import bs4
import pandas as pd
from urllib.request import Request, urlopen
from urllib.parse import urlencode, quote_plus, unquote
from sqlalchemy import create_engine, inspect

"""수정 포인트 (공공데이터 포털 API Key & DB 연결 정보)"""
# API Key 설정
API_Key = unquote("{API Key}")

# DB 연결 정보
user = '{user}'
password = '{password}'
host = '{host}'
port = '{port}'
database = "{database}"

"""# 연도별 자연재해 피해액 데이터 수집 함수"""

def collect_disaster_year(inYear):
    """
    함수정의: 행정안전부_통계연보_연도별 자연재해 피해 API를 통해 파라미터 연도의 데이터를 불러와 DB로 저장하는 함수
    입력 파라미터 : inYear -> str
    출력 결과: 없음 (결과는 'disaster_damage_by_year' 테이블에 저장됨)
    """

    # 수집중 문구 출력
    print(f"{inYear}년까지의 연도별 자연재해 피해액 데이터 수집중...")

    # 행정안전부_통계연보_연도별 자연재해 피해 API URL
    disaster_year_url = "http://apis.data.go.kr/1741000/NaturalDisasterDamageByYear/getNaturalDisasterDamageByYear"

    # 쿼리 파라미터에 API_Key 추가
    queryParams = '?' + urlencode({
                        quote_plus('ServiceKey') : API_Key,
                    })

    # requests 라이브러리를 사용하여 API로부터 XML 데이터를 가져옴
    response = requests.get(disaster_year_url+queryParams)

    # 응답 인코딩을 UTF-8로 설정
    response.encoding = "utf-8"

    # lxml-xml 파서를 사용하여 BeautifulSoup으로 XML 파싱
    disaster_year_xmlobj = bs4.BeautifulSoup(response.text, "lxml-xml")

    # 각 재해 항목별 데이터를 XML에서 추출해 리스트로 저장
    year_list = disaster_year_xmlobj.find_all("wrttimeid") # 연도
    total_list = disaster_year_xmlobj.find_all("tot") # 합계
    typhoon_list = disaster_year_xmlobj.find_all("typhoon") # 태풍
    heavy_rain_list = disaster_year_xmlobj.find_all("heavy_rain") # 호우
    heavy_snow_list = disaster_year_xmlobj.find_all("heavy_snow") # 대설
    heavy_wind_list = disaster_year_xmlobj.find_all("heavy_wind") # 강풍
    wind_wave_strong_wind_list = disaster_year_xmlobj.find_all("wind_wave_strong_wind") # 풍랑·강풍
    typhoon_heavy_rain_list = disaster_year_xmlobj.find_all("typhoon_heavy_rain") # 태풍·호우
    lightning_list = disaster_year_xmlobj.find_all("lightning") # 낙뢰
    cold_wave_list = disaster_year_xmlobj.find_all("cold_wave") # 한파
    earthquak_list = disaster_year_xmlobj.find_all("earthquak") # 지진
    heatwave_list = disaster_year_xmlobj.find_all("heatwave") # 폭염

    # 행 별로 결과를 저장할 행 리스트 선언
    rowList = []

    # 데이터프레임의 컬럼명 정의
    columnNameList = ["year", "total", "typhoon", "heavy_rain", "heavy_snow", "heavy_wind", "wind_wave_strong_wind", "typhoon_heavy_rain", "lightning", "cold_wave", "earthquake", "heatwave"]

    # 각 항목별 값을 추출하여 리스트로 저장
    for i in range(len(year_list)):
        year = year_list[i].text
        total = total_list[i].text
        typhoon = typhoon_list[i].text
        heavy_rain = heavy_rain_list[i].text
        heavy_snow = heavy_snow_list[i].text
        heavy_wind = heavy_wind_list[i].text
        wind_wave_strong_wind = wind_wave_strong_wind_list[i].text
        typhoon_heavy_rain = typhoon_heavy_rain_list[i].text
        lightning = lightning_list[i].text
        cold_wave = cold_wave_list[i].text
        earthquak = earthquak_list[i].text
        heatwave = heatwave_list[i].text

        # 각 항목을 한 행으로 리스트에 추가
        rowList.append([year, total, typhoon, heavy_rain, heavy_snow, heavy_wind, wind_wave_strong_wind, typhoon_heavy_rain, lightning, cold_wave, earthquak, heatwave])

    # pandas DataFrame으로 변환
    disaster_year_result = pd.DataFrame(rowList, columns = columnNameList)

    # 모든 컬럼을 숫자형(int)으로 변환
    disaster_year_result = disaster_year_result.astype(int)

    # 수집 가능한 최소 연도 저장
    min_year = disaster_year_result["year"].min()

    # inYear보다 작거나 같은 데이터만 필터링
    disaster_year_result = disaster_year_result[disaster_year_result["year"] <= inYear]

    # 필터링 결과가 없으면 메시지 출력 후 함수 종료
    if disaster_year_result.empty:
        print(f"해당 연도의 데이터가 존재하지 않습니다. (수집 가능한 최소 연도: {min_year}년)")
        return

    # MySQL 데이터베이스 연결 설정
    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}?ssl_disabled=True')

    # 저장할 테이블 이름
    table_name = "disaster_damage_by_year"

    # DataFrame을 MySQL 테이블에 저장 (기존 테이블 존재 시 새로운 데이터 추가)
    disaster_year_result.to_sql(name = table_name, con = engine, if_exists="replace", index=False)

    # 데이터베이스 연결 종료
    engine.dispose()

    # 수집 완료 문구 출력
    print(f"{inYear}년까지의 연도별 자연재해 피해액 데이터 수집 및 DB 저장 완료!")

"""# 연도별 자연재해 복구비 데이터 수집 함수


"""

def collect_cost_year(inYear):
    """
    함수정의: 행정안전부_통계연보_연도별 자연재해 복구비 API를 통해 파라미터 연도의 데이터를 불러와 DB로 저장하는 함수
    입력 파라미터 : inYear -> str
    출력 결과: 없음 (결과는 'disaster_damage_by_year' 테이블에 저장됨)
    """

    # 수집중 문구 출력
    print(f"{inYear}년까지의 연도별 자연재해 복구비 데이터 수집중...")

    # 행정안전부_통계연보_연도별 자연재해 복구비 API URL
    cost_year_url = "http://apis.data.go.kr/1741000/AnnualDisasterRecoveryCosts/getAnnualDisasterRecoveryCosts"

    # 쿼리 파라미터에 API_Key 추가
    queryParams = '?' + urlencode({
                        quote_plus('ServiceKey') : API_Key,
                    })

    # requests 라이브러리를 사용하여 API로부터 XML 데이터를 가져옴
    response = requests.get(cost_year_url+queryParams)

    # 응답 인코딩을 UTF-8로 설정
    response.encoding = "utf-8"

    # lxml-xml 파서를 사용하여 BeautifulSoup으로 XML 파싱
    cost_year_xmlobj = bs4.BeautifulSoup(response.text, "lxml-xml")

    # 각 재해 항목별 데이터를 XML에서 추출해 리스트로 저장
    year_list = cost_year_xmlobj.find_all("wrttimeid") # 연도
    total_list = cost_year_xmlobj.find_all("total") # 합계
    typhoon_list = cost_year_xmlobj.find_all("typhoon") # 태풍
    heavy_rain_list = cost_year_xmlobj.find_all("heavy_rain") # 호우
    heavy_snow_list = cost_year_xmlobj.find_all("heavy_snow") # 대설
    heavy_wind_list = cost_year_xmlobj.find_all("strong_wind") # 강풍
    wind_wave_strong_wind_list = cost_year_xmlobj.find_all("wind_wave_strong_wind") # 풍랑·강풍
    typhoon_heavy_rain_list = cost_year_xmlobj.find_all("typhoon_heavy_rain") # 태풍·호우
    lightning_list = cost_year_xmlobj.find_all("lightning") # 낙뢰
    cold_wave_list = cost_year_xmlobj.find_all("cold_wave") # 한파
    earthquak_list = cost_year_xmlobj.find_all("earthquake") # 지진
    heatwave_list = cost_year_xmlobj.find_all("heatwave") # 폭염

    # 행 별로 결과를 저장할 행 리스트 선언
    rowList = []

    # 데이터프레임의 컬럼명 정의
    columnNameList = ["year", "total", "typhoon", "heavy_rain", "heavy_snow", "heavy_wind", "wind_wave_strong_wind", "typhoon_heavy_rain", "lightning", "cold_wave", "earthquake", "heatwave"]

    # 각 항목별 값을 추출하여 리스트로 저장
    for i in range(len(year_list)):
        year = year_list[i].text
        total = total_list[i].text
        typhoon = typhoon_list[i].text
        heavy_rain = heavy_rain_list[i].text
        heavy_snow = heavy_snow_list[i].text
        heavy_wind = heavy_wind_list[i].text
        wind_wave_strong_wind = wind_wave_strong_wind_list[i].text
        typhoon_heavy_rain = typhoon_heavy_rain_list[i].text
        lightning = lightning_list[i].text
        cold_wave = cold_wave_list[i].text
        earthquak = earthquak_list[i].text
        heatwave = heatwave_list[i].text

        # 각 항목을 한 행으로 리스트에 추가
        rowList.append([year, total, typhoon, heavy_rain, heavy_snow, heavy_wind, wind_wave_strong_wind, typhoon_heavy_rain, lightning, cold_wave, earthquak, heatwave])

    # pandas DataFrame으로 변환
    cost_year_result = pd.DataFrame(rowList, columns = columnNameList)

    # 모든 컬럼을 숫자형(int)으로 변환
    cost_year_result = cost_year_result.astype(int)

    # 수집 가능한 최소 연도 저장
    min_year = cost_year_result["year"].min()

    # inYear보다 작거나 같은 데이터만 필터링
    cost_year_result = cost_year_result[cost_year_result["year"] <= inYear]

    # 필터링 결과가 없으면 메시지 출력 후 함수 종료
    if cost_year_result.empty:
        print(f"해당 연도의 데이터가 존재하지 않습니다. (수집 가능한 최소 연도: {min_year}년)")
        return

    # MySQL 데이터베이스 연결 설정
    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}?ssl_disabled=True')

    # 저장할 테이블 이름
    table_name = "recovery_cost_by_year"

    # DataFrame을 MySQL 테이블에 저장 (기존 테이블 존재 시 새로운 데이터 추가)
    cost_year_result.to_sql(name = table_name, con = engine, if_exists="replace", index=False)

    # 데이터베이스 연결 종료
    engine.dispose()

    # 수집 완료 문구 출력
    print(f"{inYear}년까지의 연도별 자연재해 복구비 데이터 수집 및 DB 저장 완료!")

"""# 지역별 자연재해 복구비 데이터 수집 함수"""

def collect_cost_region(inYear):
    """
    함수정의: 행정안전부_통계연보_지역별 자연재해 복구비 API를 통해 파라미터 연도의 데이터를 불러와 DB로 저장하는 함수
    입력 파라미터 : inYear -> str
    출력 결과: 없음 (결과는 'disaster_damage_by_year' 테이블에 저장됨)
    """

    # 수집중 문구 출력
    print(f"{inYear}년까지의 지역별 자연재해 복구비 데이터 수집중...")

    # 행정안전부_통계연보_지역별 자연재해 복구비 API URL
    cost_region_url = "http://apis.data.go.kr/1741000/RegionDisasterRecoveryCosts/getRegionDisasterRecoveryCosts"

    # 쿼리 파라미터에 API_Key 추가
    queryParams = '?' + urlencode({
                        quote_plus('ServiceKey') : API_Key,
                    })

    # requests 라이브러리를 사용하여 API로부터 XML 데이터를 가져옴
    response = requests.get(cost_region_url+queryParams)

    # 응답 인코딩을 UTF-8로 설정
    response.encoding = "utf-8"

    # lxml-xml 파서를 사용하여 BeautifulSoup으로 XML 파싱
    cost_region_xmlobj = bs4.BeautifulSoup(response.text, "lxml-xml")

    # 각 재해 항목별 데이터를 XML에서 추출해 리스트로 저장
    year_list = cost_region_xmlobj.find_all("wrttimeid") # 연도
    region_list = cost_region_xmlobj.find_all("region") # 지역
    total_list = cost_region_xmlobj.find_all("tot") # 합계
    typhoon_list = cost_region_xmlobj.find_all("typhoon") # 태풍
    heavy_rain_list = cost_region_xmlobj.find_all("heavy_rain") # 호우
    heavy_snow_list = cost_region_xmlobj.find_all("heavy_snow") # 대설
    heavy_wind_list = cost_region_xmlobj.find_all("heavy_wind") # 강풍
    wind_wave_strong_wind_list = cost_region_xmlobj.find_all("wind_wave_strong_wind") # 풍랑·강풍
    typhoon_heavy_rain_list = cost_region_xmlobj.find_all("typhoon_heavy_rain") # 태풍·호우
    lightning_list = cost_region_xmlobj.find_all("lightning") # 낙뢰
    cold_wave_list = cost_region_xmlobj.find_all("cold_wave") # 한파
    earthquak_list = cost_region_xmlobj.find_all("earthquak") # 지진
    heatwave_list = cost_region_xmlobj.find_all("heatwave") # 폭염

    # 행 별로 결과를 저장할 행 리스트 선언
    rowList = []

    # 데이터프레임의 컬럼명 정의
    columnNameList = ["year", "region", "total", "typhoon", "heavy_rain", "heavy_snow", "heavy_wind", "wind_wave_strong_wind", "typhoon_heavy_rain", "lightning", "cold_wave", "earthquake", "heatwave"]

    # 각 항목별 값을 추출하여 리스트로 저장
    for i in range(len(year_list)):
        year = year_list[i].text
        region = region_list[i].text
        total = total_list[i].text
        typhoon = typhoon_list[i].text
        heavy_rain = heavy_rain_list[i].text
        heavy_snow = heavy_snow_list[i].text
        heavy_wind = heavy_wind_list[i].text
        wind_wave_strong_wind = wind_wave_strong_wind_list[i].text
        typhoon_heavy_rain = typhoon_heavy_rain_list[i].text
        lightning = lightning_list[i].text
        cold_wave = cold_wave_list[i].text
        earthquak = earthquak_list[i].text
        heatwave = heatwave_list[i].text

        # 각 항목을 한 행으로 리스트에 추가
        rowList.append([year, region, total, typhoon, heavy_rain, heavy_snow, heavy_wind, wind_wave_strong_wind, typhoon_heavy_rain, lightning, cold_wave, earthquak, heatwave])

    # pandas DataFrame으로 변환
    cost_region_result = pd.DataFrame(rowList, columns = columnNameList)

    # region 컬럼을 제외한 나머지 컬럼을 숫자형으로 변환
    cols_to_convert = cost_region_result.columns.difference(['region'])
    cost_region_result[cols_to_convert] = cost_region_result[cols_to_convert].apply(pd.to_numeric, errors='coerce')


    # 수집 가능한 최소 연도 저장
    min_year = cost_region_result["year"].min()

    # inYear보다 작거나 같은 데이터만 필터링
    cost_region_result = cost_region_result[cost_region_result["year"] <= inYear]

    # 필터링 결과가 없으면 메시지 출력 후 함수 종료
    if cost_region_result.empty:
        print(f"해당 연도의 데이터가 존재하지 않습니다. (수집 가능한 최소 연도: {min_year}년)")
        return

    # MySQL 데이터베이스 연결 설정
    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}?ssl_disabled=True')

    # 저장할 테이블 이름
    table_name = "recovery_cost_by_region"

    # DataFrame을 MySQL 테이블에 저장 (기존 테이블 존재 시 새로운 데이터 추가)
    cost_region_result.to_sql(name = table_name, con = engine, if_exists="replace", index=False)

    # 데이터베이스 연결 종료
    engine.dispose()

    # 수집 완료 문구 출력
    print(f"{inYear}년까지의 지역별 자연재해 복구비 데이터 수집 및 DB 저장 완료!")

"""# 지역별 자연재해 피해액 데이터 수집 함수"""

def collect_disaster_region(inYear):
    """
    함수정의: 행정안전부_통계연보_지역별 자연재해 피해 API를 통해 파라미터 연도의 데이터를 불러와 DB로 저장하는 함수
    입력 파라미터 : inYear -> str
    출력 결과:
    """

    # 수집중 문구 출력
    print(f"{inYear}년까지의 지역별 자연재해 피해액 데이터 수집중...")

    # 행정안전부_통계연보_지역별 자연재해 피해 API URL
    disaster_region_url = "http://apis.data.go.kr/1741000/RegionalNaturalDisasterDamage/getRegionalNaturalDisasterDamage"

    # 쿼리 파라미터에 API_Key 추가
    queryParams = '?' + urlencode({
                        quote_plus('ServiceKey') : API_Key,
                    })

    # requests 라이브러리를 사용하여 API로부터 XML 데이터를 가져옴
    response = requests.get(disaster_region_url+queryParams)

    # 응답 인코딩을 UTF-8로 설정
    response.encoding = "utf-8"

    # lxml-xml 파서를 사용하여 BeautifulSoup으로 XML 파싱
    disaster_region_xmlobj = bs4.BeautifulSoup(response.text, "lxml-xml")

    # 각 재해 항목별 데이터를 XML에서 추출해 리스트로 저장
    year_list = disaster_region_xmlobj.find_all("wrttimeid") # 연도
    region_list = disaster_region_xmlobj.find_all("region") # 지역

    prop_tot_list = disaster_region_xmlobj.find_all("prop_tot") # 합계_재산
    life_tot_list = disaster_region_xmlobj.find_all("life_tot") # 합계_인명

    typhoon_prop_list = disaster_region_xmlobj.find_all("typhoon_property_damage") # 태풍_재산
    typhoon_life_list = disaster_region_xmlobj.find_all("typhoon_life_damage") # 태풍_인명

    heavy_rain_prop_list = disaster_region_xmlobj.find_all("heavy_rain_property_damage") # 호우_재산
    heavy_rain_life_list = disaster_region_xmlobj.find_all("heavy_rain_life_damage") # 호우_인명

    heavy_snow_prop_list = disaster_region_xmlobj.find_all("heavy_snow_property_damage") # 대설_재산
    heavy_snow_life_list = disaster_region_xmlobj.find_all("heavy_snow_life_damage") # 대설_인명

    strong_wind_prop_list = disaster_region_xmlobj.find_all("strong_wind_property_damage") # 강풍_재산
    strong_wind_life_list = disaster_region_xmlobj.find_all("strong_wind_life_damage") # 강풍_인명

    wind_wave_strong_wind_prop_list = disaster_region_xmlobj.find_all("wind_wave_strong_wind_property_damage") # 풍랑·강풍_재산
    wind_wave_strong_wind_life_list = disaster_region_xmlobj.find_all("wind_wave_strong_wind_life_damage") # 풍랑·강풍_인명

    typhoon_heavy_rain_prop_list = disaster_region_xmlobj.find_all("typhoon_heavy_rain_property_damage") # 태풍·호우_재산
    typhoon_heavy_rain_life_list = disaster_region_xmlobj.find_all("typhoon_heavy_rain_life_damage") # 태풍·호우_인명

    lightning_prop_list = disaster_region_xmlobj.find_all("lightning_property_damage") # 낙뢰_재산
    lightning_life_list = disaster_region_xmlobj.find_all("lightning_life_damage") # 낙뢰_인명

    cold_wave_prop_list = disaster_region_xmlobj.find_all("cold_wave_property_damage") # 한파_재산
    cold_wave_life_list = disaster_region_xmlobj.find_all("cold_wave_life_damage") # 한파_인명

    earthquake_prop_list = disaster_region_xmlobj.find_all("earthquake_property_damage") # 지진_재산
    earthquake_life_list = disaster_region_xmlobj.find_all("earthquake_life_damage") # 지진_인명

    heatwave_prop_list = disaster_region_xmlobj.find_all("heatwave_property_damage") # 폭염_재산
    heatwave_life_list = disaster_region_xmlobj.find_all("heatwave_life_damage") # 폭염_인명

    # 행 별로 결과를 저장할 행 리스트 선언
    rowList = []

    # 데이터프레임의 컬럼명 정의
    columnNameList = ["year", "region", "total", "typhoon", "heavy_rain", "heavy_snow", "heavy_wind", "wind_wave_strong_wind", "typhoon_heavy_rain", "lightning", "cold_wave", "earthquake", "heatwave"]

    # 각 항목별 값을 추출하여 리스트로 저장
    for i in range(len(year_list)):
        year = year_list[i].text
        region = region_list[i].text
        total = str(float(prop_tot_list[i].text) + float(life_tot_list[i].text))

        # 각 재해별 총합 (property + life)
        typhoon = str(float(typhoon_prop_list[i].text) + float(typhoon_life_list[i].text))
        heavy_rain = str(float(heavy_rain_prop_list[i].text) + float(heavy_rain_life_list[i].text))
        heavy_snow = str(float(heavy_snow_prop_list[i].text) + float(heavy_snow_life_list[i].text))
        heavy_wind = str(float(strong_wind_prop_list[i].text) + float(strong_wind_life_list[i].text))
        wind_wave_strong_wind = str(float(wind_wave_strong_wind_prop_list[i].text) + float(wind_wave_strong_wind_life_list[i].text))
        typhoon_heavy_rain = str(float(typhoon_heavy_rain_prop_list[i].text) + float(typhoon_heavy_rain_life_list[i].text))
        lightning = str(float(lightning_prop_list[i].text) + float(lightning_life_list[i].text))
        cold_wave = str(float(cold_wave_prop_list[i].text) + float(cold_wave_life_list[i].text))
        earthquake = str(float(earthquake_prop_list[i].text) + float(earthquake_life_list[i].text))
        heatwave = str(float(heatwave_prop_list[i].text) + float(heatwave_life_list[i].text))

        # 각 항목을 한 행으로 리스트에 추가
        rowList.append([year, region, total, typhoon, heavy_rain, heavy_snow, heavy_wind, wind_wave_strong_wind, typhoon_heavy_rain, lightning, cold_wave, earthquake, heatwave])

    # pandas DataFrame으로 변환
    disaster_region_result = pd.DataFrame(rowList, columns = columnNameList)

    # region 컬럼을 제외한 나머지 컬럼을 숫자형으로 변환
    cols_to_convert = disaster_region_result.columns.difference(['region'])
    disaster_region_result[cols_to_convert] = disaster_region_result[cols_to_convert].apply(pd.to_numeric, errors='coerce')

    # 수집 가능한 최소 연도 저장
    min_year = disaster_region_result["year"].min()

    # inYear보다 작거나 같은 데이터만 필터링
    disaster_region_result = disaster_region_result[disaster_region_result["year"] <= inYear]

    # 필터링 결과가 없으면 메시지 출력 후 함수 종료
    if disaster_region_result.empty:
        print(f"해당 연도의 데이터가 존재하지 않습니다. (수집 가능한 최소 연도: {min_year}년)")
        return

    # MySQL 데이터베이스 연결 설정
    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}?ssl_disabled=True')

    # 저장할 테이블 이름
    table_name = "disaster_damage_by_region"

    # DataFrame을 MySQL 테이블에 저장 (기존 테이블 존재 시 새로운 데이터 추가)
    disaster_region_result.to_sql(name=table_name, con=engine, if_exists="replace", index=False)

    # 데이터베이스 연결 종료
    engine.dispose()

    # 수집 완료 문구 출력
    print(f"{inYear}년까지의 지역별 자연재해 피해액 데이터 수집 및 DB 저장 완료!")

"""# 연도 지정 데이터 수집 및 DB append"""

# 연도 지정
inYear = int(input("수집할 연도를 입력해주세요 (예 : 2016) : "))

# 출력 이쁘게 하기 위한 줄바꿈
print()

# 데이터 수집
collect_disaster_year(inYear)

print()

collect_cost_year(inYear)

print()

collect_cost_region(inYear)

print()

collect_disaster_region(inYear)