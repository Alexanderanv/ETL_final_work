ydb `
--endpoint grpcs://ydb.serverless.yandexcloud.net:2135 `
--database /ru-central1/DATABASE_PATH `
--sa-key-file "./service/authorized_key.json" `
import file csv `
--path online_retail `
--delimiter "," `
--skip-rows 1 `
--null-value "" `
online_retail_2009_2010.csv