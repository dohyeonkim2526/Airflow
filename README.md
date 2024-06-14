## 파이프라인 구축 작업일지

### [인프라 구축]
**24년 1-2월**
* (1,2주차) Docker로 Airflow 설치 (volumes, networks 설정)
* (3주차) Docker로 MariaDB 설치 (고정 IP 할당, port, volumes 설정)
* (3주차) MariaDB 컨테이너 정보 이용하여 DBeaver 연결
* (4주차) Docker로 Jupyter 설치 (bind mount, MariaDB 컨테이너 연결)

<br/></br>
**24년 6월 2주차**
* Jupyter에서 Airflow DAG 볼륨 마운트 --> Jupyter에서 코드 수정시 Airflow 실시간 반영
* Airflow DAG로 수집한 데이터 DB 적재 테스트 완료
* 병렬 Task로 API 데이터 동시에 수집하여 merge하여 DB 적재 테스트 완료

<br/></br>
**다음 계획**
* Task를 조금 더 분업할 수 있을지 고민해보기
* 일반적인 회사에서 수집한 raw 데이터를 어떻게 적재 및 사용하는지 사례 파악 (raw 적재에 어떤 DB, Tool이 사용되는지)
* 데이터를 어떤 식으로 DW/DM 만들지? (with Jupyter Notebook)


<br/></br>
### [프로젝트 구상방안]
* 데이터셋: 국토교통부 실거래가 정보 OpenAPI
* 서울시 지역구(25개)에서 2022-2024년 데이터 수집 (일배치로 매일 데이터 업데이트)
* 수집된 raw 데이터 이용하여 DW/DM 마트 구축 (Jupyter Notebook 사용 예정)

