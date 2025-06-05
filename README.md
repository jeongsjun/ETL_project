# 🌩 자연재해 통계 시스템

이 프로젝트는 **ETL**을 주제로 개발한 **자연재해 통계 시스템** 입니다. 정해진 시간마다 데이터 분석 결과를 업데이트할 수 있는 것이 컨셉입니다.
---

## 📌 주요 기능

- ✅ **데이터 수집**
  - 공공데이터포털에서 데이터를 수집
- ✅ **데이터 저장**
  - 수집한 데이터를 DB에 저장
- ✅ **데이터 업데이트**
  - 정해진 시간마다 수집한 데이터를 분석해 분석 데이터 업데이트
  
---

## 📁 파일 구성
```
ETL_project/
├── Python(ipynb)                  # 파이썬 파일(ipynb) 디렉터리
  ├── ETL_collect_app.ipynb        # 데이터 수집 ipynb
  ├── ETL_join_app.ipynb           # 데이터 분석 ipynb
├── Ubuntu                         # Ubuntu 관련 파일 디렉터리
  ├── auto_join.sh                 # 데이터 분석 코드를 실행하기 위한 쉘 스크립트
  ├── crontab.txt                  # crontab 코드
  ├── etl_collect_app.py           # 데이터 수집 코드
  ├── etl_join_app.py              # 데이터 분석 코드
  ├── requirements.txt             # 필요 라이브러리
└── ...
```

---

## 🖥️ 실행 방법

### 🛠️ 개발 환경 및 개발 도구
- 개발 환경
  - Window
  - Ubuntu 24.04
- 언어 및 프레임워크
  - Python 3.12
  - Mysql 8.4
- 개발 도구
  - Colab
  - Oracle Virtualbox

### ▶️ 실행
#### 1. 파일 이관
Ubuntu 환경에 파일 이관 (ex : home/ubuntu/pywork_etl/codeset
1) etl_collect_app.py
2) etl_join_app.py
3) requirements.txt

#### 2. Ubuntu 환경에서 파이썬 가상환경 설치 및 가상환경 실행
2-1. 가상환경을 위한 설치
```
sudo apt install python3.12-venv
```
2-2. 가상환경 생성
```
python3 -m venv .etlcore
```
2-3. 가상환경 실행
```
source .etlcore/bin/activate
```

#### 3. 가상환경에 필요 라이브러리 설치
```
pip3 install -r requirements.txt
```

#### 4. 테스트 (수정 포인트 수정되었다는 가정)
4-1. 데이터 수집
```
python3 etl_collect_app.py
```
4-2. 데이터 분석
```
python3 etl_join_app.py
```

#### 5. 자동화
5-1. Ubuntu 환경에서 (ex : /home/ubuntu/pywork_etl/)에서 쉘스크립트(auto_join.sh) 작성
```
#!/bin/bash

source /home/ubuntu/pywork_etl/.etlcore/bin/activate
python3 /home/ubuntu/pywork_etl/codeset/etl_join_app.py
```
5-2. 작성 후 실행 권한 부여
```
chmod 775 auto_join.sh
```
5-3. crontab 작업
```
crontab -e
```
```
// crontab
// 예시 : 매분마다 실행
* * * * * /home/ubuntu/pywork_etl/auto_join.sh >> /home/ubuntu/pywork_etl/auto_join_out.log 2>&1
```
