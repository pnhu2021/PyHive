from pyhive import presto
from requests import Session
import requests
from requests.auth import HTTPBasicAuth
requests.packages.urllib3.disable_warnings()

# req_kw = {
#     'auth': HTTPBasicAuth('your_username', 'your_password'),
#     'verify': '/path/to/cacerts.pem'
# }

req_kw = {
    'auth': HTTPBasicAuth('hu.peng', 'pnhu2021@NIO'),
    'verify': False # 不验证证书
}

req_session = Session()

conn = presto.connect(
    host='rey-presto.nioint.com',
    port=443,
    protocol='https',
    catalog='hive',
    schema='default',
    username='hu.peng',
    source='my_project#ETL',
    requests_session=req_session,
    requests_kwargs=req_kw,
)

cursor = conn.cursor()

# 时区问题请参考 https://niohome.feishu.cn/docs/doccnjd6a6MMpflCb15fuOq24Xc
# server time是timestamp类型
req_session.headers.update({'X-Presto-Time-Zone': 'UTC'}) # 设置session时区
cursor.execute("select *
              ,row_number() over(partition by month order by concat(name,employee_id))  as rank
                  from uds_apollo_metrics_dws_prod.acc_five_star_fellow_score_prod_1d
                 where vol='202109'  ---期数
                   and month='202107'")
result = cursor.fetchone()
print(result) # ('2020-12-22 01:57:59.000',)
