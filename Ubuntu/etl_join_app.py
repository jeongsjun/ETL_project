"""# 필요 라이브러리 import"""

from sqlalchemy import create_engine, inspect
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo

"""수정 포인트 (DB 연결 정보)"""
# DB 연결 정보
user = '{user}'
password = '{password}'
host = '{host}'
port = '{port}'
database = "{database}"

# mysql 커넥터 엔진 생성
engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}?ssl_disabled=True')

# 현재 한국 시간 설정
current_time = datetime.now(ZoneInfo("Asia/Seoul"))
current_time = current_time.strftime("%Y-%m-%d %H:%M:%S")

"""# 연도별로 각 재해의 피해 규모와 그에 따른 복구 비용 데이터 병합 및 DB append 함수"""

def join_year_damage_cost():
    """
    함수 정의: 연도별 자연재해 피해액 데이터(disaster_damage_by_year)과 복구비용 데이터(recovery_cost_by_year)를 연도 기준으로 JOIN하여 데이터프레임으로 만들고, 이를 데이터베이스에 저장하는 함수

    입력 파라미터: 없음

    출력 결과: 없음 (결과는 'year_damage_cost' 테이블에 저장됨)
    """

    # SQL 쿼리문: 연도별 피해액 데이터(disaster_damage_by_year)와 복구비용 데이터(recovery_cost_by_year)를 연도 기준으로 INNER JOIN
    year_damage_cost_joinSql = """
    SELECT
        A.year,
        A.total AS total_damage,
        B.total AS total_cost,
        A.typhoon AS typhoon_damage,
        B.typhoon AS typhoon_cost,
        A.heavy_rain AS heavy_rain_damage,
        B.heavy_rain AS heavy_rain_cost,
        A.heavy_snow AS heavy_snow_damage,
        B.heavy_snow AS heavy_snow_cost,
        A.heavy_wind AS heavy_wind_damage,
        B.heavy_wind AS heavy_wind_cost,
        A.wind_wave_strong_wind AS wind_wave_strong_wind_damage,
        B.wind_wave_strong_wind AS wind_wave_strong_wind_cost,
        A.typhoon_heavy_rain AS typhoon_heavy_rain_damage,
        B.typhoon_heavy_rain AS typhoon_heavy_rain_cost,
        A.lightning AS lightning_damage,
        B.lightning AS lightning_cost,
        A.cold_wave AS cold_wave_damage,
        B.cold_wave AS cold_wave_cost,
        A.earthquake AS earthquake_damage,
        B.earthquake AS earthquake_cost,
        A.heatwave AS heatwave_damage,
        B.heatwave AS heatwave_cost
    FROM disaster_damage_by_year A
    INNER JOIN recovery_cost_by_year B
    ON A.year = B.year;
    """

    # SQL 쿼리를 실행하여 데이터프레임으로 변환
    year_damage_cost_result = pd.read_sql_query(sql = year_damage_cost_joinSql, con = engine)

    # 현재 시각(current_time)을 'timetag' 컬럼으로 데이터프레임에 추가
    year_damage_cost_result.insert(0, 'timetag', current_time)

    # 저장할 테이블 이름 지정
    table_name = "year_damage_cost"

    # 데이터프레임을 데이터베이스에 저장 (기존 데이터는 유지하고 append 방식으로 추가)
    year_damage_cost_result.to_sql(name = table_name, con = engine, if_exists="append", index=False)

"""# 연도별 재해 유형별 피해 대비 복구 비용 비율 데이터 병합 및 DB append 함수"""

def join_year_cost_ratio():
    """
    함수 정의: 연도별 자연재해 피해액 데이터(disaster_damage_by_year)과 복구비용 데이터(recovery_cost_by_year)를
              연도 기준으로 JOIN하여 연도별 자연재해 유형별 피해액 대비 복구비용 비율을 계산하여
              데이터프레임으로 생성하고 데이터베이스에 저장하는 함수

    입력 파라미터: 없음

    출력 결과: 없음 (결과는 'year_cost_ratio' 테이블에 저장됨)
    """

    # SQL 쿼리문: 연도별 피해액(disaster_damage_by_year)과 복구비용(recovery_cost_by_year)을 연도 기준으로 JOIN하고, 복구비용/피해액 비율 계산
    # NULL 또는 0으로 나눌 경우를 방지하기 위해 CASE 문 사용
    year_cost_ratio_joinSql = """
    SELECT
        A.year,
        A.typhoon AS typhoon_damage,
        B.typhoon AS typhoon_cost,
        CASE WHEN B.typhoon/A.typhoon IS NULL THEN 0
        ELSE B.typhoon/A.typhoon END AS 'typhoon_cost/damage_ratio',
        A.heavy_rain AS heavy_rain_damage,
        B.heavy_rain AS heavy_rain_cost,
        CASE WHEN B.heavy_rain/A.heavy_rain IS NULL THEN 0
        ELSE B.heavy_rain/A.heavy_rain END AS 'heavy_rain_cost/damage_ratio',
        A.heavy_snow AS heavy_snow_damage,
        B.heavy_snow AS heavy_snow_cost,
        CASE WHEN B.heavy_snow/A.heavy_snow IS NULL THEN 0
        ELSE B.heavy_snow/A.heavy_snow END AS 'heavy_snow_cost/damage_ratio',
        A.heavy_wind AS heavy_wind_damage,
        B.heavy_wind AS heavy_wind_cost,
        CASE WHEN B.heavy_wind/A.heavy_wind IS NULL THEN 0
        ELSE B.heavy_wind/A.heavy_wind END AS 'heavy_wind_cost/damage_ratio',
        A.wind_wave_strong_wind AS wind_wave_strong_wind_damage,
        B.wind_wave_strong_wind AS wind_wave_strong_wind_cost,
        CASE WHEN B.wind_wave_strong_wind/A.wind_wave_strong_wind IS NULL THEN 0
        ELSE B.wind_wave_strong_wind/A.wind_wave_strong_wind END AS 'wind_wave_strong_wind_cost/damage_ratio',
        A.typhoon_heavy_rain AS typhoon_heavy_rain_damage,
        B.typhoon_heavy_rain AS typhoon_heavy_rain_cost,
        CASE WHEN B.typhoon_heavy_rain/A.typhoon_heavy_rain IS NULL THEN 0
        ELSE B.typhoon_heavy_rain/A.typhoon_heavy_rain END AS 'typhoon_heavy_rain_cost/damage_ratio',
        A.lightning AS lightning_damage,
        B.lightning AS lightning_cost,
        CASE WHEN B.lightning/A.lightning IS NULL THEN 0
        ELSE B.lightning/A.lightning END AS 'lightning_cost/damage_ratio',
        A.cold_wave AS cold_wave_damage,
        B.cold_wave AS cold_wave_cost,
        CASE WHEN B.cold_wave/A.cold_wave IS NULL THEN 0
        ELSE B.cold_wave/A.cold_wave END AS 'cold_wave_cost/damage_ratio',
        A.earthquake AS earthquake_damage,
        B.earthquake AS earthquake_cost,
        CASE WHEN B.earthquake/A.earthquake IS NULL THEN 0
        ELSE B.earthquake/A.earthquake END AS 'earthquake_cost/damage_ratio',
        A.heatwave AS heatwave_damage,
        B.heatwave AS heatwave_cost,
        CASE WHEN B.heatwave/A.heatwave IS NULL THEN 0
        ELSE B.heatwave/A.heatwave END AS 'heatwave_cost/damage_ratio'
    FROM disaster_damage_by_year A
    INNER JOIN recovery_cost_by_year B
    ON A.year = B.year;
    """

    # SQL 쿼리를 실행하여 데이터프레임으로 변환
    year_cost_ratio_result = pd.read_sql_query(sql = year_cost_ratio_joinSql, con = engine)

    # 현재 시각(current_time)을 'timetag' 컬럼으로 데이터프레임에 추가
    year_cost_ratio_result.insert(0, 'timetag', current_time)

    # 저장할 테이블 이름 지정
    table_name = "year_cost_ratio"

    # 데이터프레임을 데이터베이스에 저장 (기존 데이터는 유지하고 append 방식으로 추가)
    year_cost_ratio_result.to_sql(name = table_name, con = engine, if_exists="append", index=False)

"""# 지역별로 각 재해의 피해 규모와 그에 따른 복구 비용 데이터 병합 및 DB append 함수"""

def join_region_damage_cost():
    """
    함수 정의: 지역별 자연재해 피해액 데이터(disaster_damage_by_region)과 복구비용 데이터(recovery_cost_by_region)를
              연도와 지역 기준으로 JOIN하여 데이터프레임으로 생성하고 데이터베이스에 저장하는 함수

    입력 파라미터: 없음

    출력 결과: 없음 (결과는 'region_damage_cost' 테이블에 저장됨)
    """
    region_damage_cost_joinSql = """
        SELECT
        MAX(A.year) AS until_year,
        A.region,
        SUM(A.typhoon) AS typhoon_damage,
        SUM(B.typhoon) AS typhoon_cost,
        SUM(A.heavy_rain) AS heavy_rain_damage,
        SUM(B.heavy_rain) AS heavy_rain_cost,
        SUM(A.heavy_snow) AS heavy_snow_damage,
        SUM(B.heavy_snow) AS heavy_snow_cost,
        SUM(A.heavy_wind) AS heavy_wind_damage,
        SUM(B.heavy_wind) AS heavy_wind_cost,
        SUM(A.wind_wave_strong_wind) AS wind_wave_strong_wind_damage,
        SUM(B.wind_wave_strong_wind) AS wind_wave_strong_wind_cost,
        SUM(A.typhoon_heavy_rain) AS typhoon_heavy_rain_damage,
        SUM(B.typhoon_heavy_rain) AS typhoon_heavy_rain_cost,
        SUM(A.lightning) AS lightning_damage,
        SUM(B.lightning) AS lightning_cost,
        SUM(A.cold_wave) AS cold_wave_damage,
        SUM(B.cold_wave) AS cold_wave_cost,
        SUM(A.earthquake) AS earthquake_damage,
        SUM(B.earthquake) AS earthquake_cost,
        SUM(A.heatwave) AS heatwave_damage,
        SUM(B.heatwave) AS heatwave_cost
    FROM disaster_damage_by_region A
    INNER JOIN recovery_cost_by_region B
    ON A.region = B.region
    AND A.year = B.year
    GROUP BY A.region;
    """

    # SQL 쿼리를 실행하여 데이터프레임으로 변환
    region_damage_cost_result = pd.read_sql_query(sql = region_damage_cost_joinSql, con = engine)

    # 현재 시각(current_time)을 'timetag' 컬럼으로 데이터프레임에 추가
    region_damage_cost_result.insert(0, 'timetag', current_time)

    # 저장할 테이블 이름 지정
    table_name = "region_damage_cost"

    # 데이터프레임을 데이터베이스에 저장 (기존 데이터는 유지하고 append 방식으로 추가)
    region_damage_cost_result.to_sql(name = table_name, con = engine, if_exists="append", index=False)

"""# 지역별 재해 유형별 피해 대비 복구 비용 비율 데이터 병합 및 DB append 함수"""

def join_region_cost_ratio():
    """
    함수 정의: 연도별 자연재해 피해액 데이터(disaster_damage_by_year)과 복구비용 데이터(recovery_cost_by_year)를
              연도와 지역 기준으로 JOIN하여 지역별 자연재해 유형별 피해액 대비 복구비용 비율을 계산하여
              데이터프레임으로 생성하고 데이터베이스에 저장하는 함수

    입력 파라미터: 없음

    출력 결과: 없음 (결과는 'region_cost_ratio' 테이블에 저장됨)
    """
    region_cost_ratio_joinSql = """
    SELECT
        MAX(A.year) AS until_year,
        A.region,
        SUM(A.typhoon) AS typhoon_damage,
        SUM(B.typhoon) AS typhoon_cost,
        CASE WHEN SUM(B.typhoon)/SUM(A.typhoon) IS NULL THEN 0
        ELSE SUM(B.typhoon)/SUM(A.typhoon) END AS 'typhoon_cost/damage_ratio',
        SUM(A.heavy_rain) AS heavy_rain_damage,
        SUM(B.heavy_rain) AS heavy_rain_cost,
        CASE WHEN SUM(B.heavy_rain)/SUM(A.heavy_rain) IS NULL THEN 0
        ELSE SUM(B.heavy_rain)/SUM(A.heavy_rain) END AS 'heavy_rain_cost/damage_ratio',
        SUM(A.heavy_snow) AS heavy_snow_damage,
        SUM(B.heavy_snow) AS heavy_snow_cost,
        CASE WHEN SUM(B.heavy_snow)/SUM(A.heavy_snow) IS NULL THEN 0
        ELSE SUM(B.heavy_snow)/SUM(A.heavy_snow) END AS 'heavy_snow_cost/damage_ratio',
        SUM(A.heavy_wind) AS heavy_wind_damage,
        SUM(B.heavy_wind) AS heavy_wind_cost,
        CASE WHEN SUM(B.heavy_wind)/SUM(A.heavy_wind) IS NULL THEN 0
        ELSE SUM(B.heavy_wind)/SUM(A.heavy_wind) END AS 'heavy_wind_cost/damage_ratio',
        SUM(A.wind_wave_strong_wind) AS wind_wave_strong_wind_damage,
        SUM(B.wind_wave_strong_wind) AS wind_wave_strong_wind_cost,
        CASE WHEN SUM(B.wind_wave_strong_wind)/SUM(A.wind_wave_strong_wind) IS NULL THEN 0
        ELSE SUM(B.wind_wave_strong_wind)/SUM(A.wind_wave_strong_wind) END AS 'wind_wave_strong_wind_cost/damage_ratio',
        SUM(A.typhoon_heavy_rain) AS typhoon_heavy_rain_damage,
        SUM(B.typhoon_heavy_rain) AS typhoon_heavy_rain_cost,
        CASE WHEN SUM(B.typhoon_heavy_rain)/SUM(A.typhoon_heavy_rain) IS NULL THEN 0
        ELSE SUM(B.typhoon_heavy_rain)/SUM(A.typhoon_heavy_rain) END AS 'typhoon_heavy_rain_cost/damage_ratio',
        SUM(A.lightning) AS lightning_damage,
        SUM(B.lightning) AS lightning_cost,
        CASE WHEN SUM(B.lightning)/SUM(A.lightning) IS NULL THEN 0
        ELSE SUM(B.lightning)/SUM(A.lightning) END AS 'lightning_cost/damage_ratio',
        SUM(A.cold_wave) AS cold_wave_damage,
        SUM(B.cold_wave) AS cold_wave_cost,
        CASE WHEN SUM(B.cold_wave)/SUM(A.cold_wave) IS NULL THEN 0
        ELSE SUM(B.cold_wave)/SUM(A.cold_wave) END AS 'cold_wave_cost/damage_ratio',
        SUM(A.earthquake) AS earthquake_damage,
        SUM(B.earthquake) AS earthquake_cost,
        CASE WHEN SUM(B.earthquake)/SUM(A.earthquake) IS NULL THEN 0
        ELSE SUM(B.earthquake)/SUM(A.earthquake) END AS 'earthquake_cost/damage_ratio',
        SUM(A.heatwave) AS heatwave_damage,
        SUM(B.heatwave) AS heatwave_cost,
        CASE WHEN SUM(B.heatwave)/SUM(A.heatwave) IS NULL THEN 0
        ELSE SUM(B.heatwave)/SUM(A.heatwave) END AS 'heatwave_cost/damage_ratio'
    FROM disaster_damage_by_region A
    INNER JOIN recovery_cost_by_region B
    ON A.region = B.region
    AND A.year = B.year
    GROUP BY A.region;
    """

    # SQL 쿼리를 실행하여 데이터프레임으로 변환
    region_cost_ratio_result = pd.read_sql_query(sql = region_cost_ratio_joinSql, con = engine)

    # 현재 시각(current_time)을 'timetag' 컬럼으로 데이터프레임에 추가
    region_cost_ratio_result.insert(0, 'timetag', current_time)

    # 저장할 테이블 이름 지정
    table_name = "region_cost_ratio"

    # 데이터프레임을 데이터베이스에 저장 (기존 데이터는 유지하고 append 방식으로 추가)
    region_cost_ratio_result.to_sql(name = table_name, con = engine, if_exists="append", index=False)

"""# 데이터 검증 로직"""

def verify_count():
    """
    함수 정의: 데이터베이스 내 네 개의 테이블(year_damage_cost, year_cost_ratio, region_damage_cost, region_cost_ratio)의
              행 개수를 확인하여 출력하는 함수

    입력 파라미터: 없음

    출력 결과: 각 테이블별 행 개수를 콘솔에 출력(테이블이 존재하지 않거나 에러 발생 시 0으로 출력)
    """

    # year_damage_cost 테이블의 데이터 개수 확인
    year_damage_cost_verifySql = """
    SELECT count(*) AS count
    FROM year_damage_cost;
    """

    # 예외 처리 (테이블이 존재하지 않는 경우 테이블 행의 개수를 0으로 출력)
    try:
        # SQL 실행 후 결과를 데이터프레임으로 변환
        result = pd.read_sql_query(sql = year_damage_cost_verifySql, con = engine)

        # count 값을 출력
        print(f"year_damage_cost : {result['count'][0]}")

    except:
        # 테이블이 없거나 오류 발생 시 0 출력
        print("year_damage_cost : 0")


    # year_cost_ratio 테이블의 데이터 개수 확인
    year_cost_ratio_verifySql = """
    SELECT count(*) AS count
    FROM year_cost_ratio;
    """

    # 예외 처리 (테이블이 존재하지 않는 경우 테이블 행의 개수를 0으로 출력)
    try:
        # SQL 실행 후 결과를 데이터프레임으로 변환
        result = pd.read_sql_query(sql = year_cost_ratio_verifySql, con = engine)

        # count 값을 출력
        print(f"year_cost_ratio : {result['count'][0]}")

    except:
        # 테이블이 없거나 오류 발생 시 0 출력
        print("year_cost_ratio : 0")

    # region_damage_cost 테이블의 데이터 개수 확인
    region_damage_cost_verifySql = """
    SELECT count(*) AS count
    FROM region_damage_cost;
    """

    # 예외 처리 (테이블이 존재하지 않는 경우 테이블 행의 개수를 0으로 출력)
    try:
        # SQL 실행 후 결과를 데이터프레임으로 변환
        result = pd.read_sql_query(sql = region_damage_cost_verifySql, con = engine)

        # count 값을 출력
        print(f"region_damage_cost : {result['count'][0]}")

    except:
        # 테이블이 없거나 오류 발생 시 0 출력
        print("region_damage_cost : 0")

    # region_cost_ratio 테이블의 데이터 개수 확인
    region_cost_ratio_verifySql = """
    SELECT count(*) AS count
    FROM region_cost_ratio;
    """

    # 예외 처리 (테이블이 존재하지 않는 경우 테이블 행의 개수를 0으로 출력)
    try:
        # SQL 실행 후 결과를 데이터프레임으로 변환
        result = pd.read_sql_query(sql = region_cost_ratio_verifySql, con = engine)

        # count 값을 출력
        print(f"region_cost_ratio : {result['count'][0]}")

    except:
        # 테이블이 없거나 오류 발생 시 0 출력
        print("region_cost_ratio : 0")

"""# 데이터 병합 및 검증 결과 출력"""

# 빈 줄 출력 (가독성을 위한 줄바꿈)
print()

# 현재 시각 기준으로 작업 기록 시작을 알리는 메시지 출력
print(f"====== {current_time} 기록 ======")

# 데이터 JOIN 수행 전 테이블 상태 확인
print("데이터 JOIN 전 테이블 COUNT")
verify_count() # 현재 데이터베이스에 저장된 행 수 출력
print() # 줄바꿈

# 연도별 피해 및 복구 비용 데이터를 JOIN하여 year_damage_cost 테이블 생성/추가
join_year_damage_cost()

# 연도별 재해 유형별 피해액 대비 복구비 비율을 계산하여 year_cost_ratio 테이블 생성/추가
join_year_cost_ratio()

# 지역별 피해 및 복구 비용 데이터를 JOIN하여 region_damage_cost 테이블 생성/추가
join_region_damage_cost()

# 지역별 재해 유형별 피해액 대비 복구비 비율을 계산하여 region_cost_ratio 테이블 생성/추가
join_region_cost_ratio()

# 데이터 JOIN 후 테이블 상태 확인
print("데이터 JOIN 후 테이블 COUNT")
verify_count() # JOIN 후 데이터베이스에 저장된 행 수 출력
print() # 줄바꿈

