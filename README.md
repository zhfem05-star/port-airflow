# CatchData-Airflow

Airflow 인스턴스와 관련된 여러 파일들을 보관한 곳

깃 워크 플로우로 ruff check와 git action을 통하여 airflow 인스턴스 동기화


프로젝트에서 작성자 본인이 작성한 DAG와 그 외 사이드로 진행한 DAG에 대한 파일이 담겨져 있음


처음에 작업한 것은 mise_EL.py로 미세먼지 데이터를 바로 json으로 받아 snowflake에 적재하는 DAG

그 다음으론 mise_s3_ETL과 mise_s3_to_sn.py로 기존 방식과 다르게 s3를 한 번 거쳐가는 단계를 만들어

raw_data의 백업과 sensor를 통한 dag간의 의존성을 테스트