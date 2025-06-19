-- Таблица на движке Kafka
CREATE TABLE IF NOT EXISTS sales.online_retail
(
    invoice String,
    stock_code String,
    description String,
    quantity Int32,
    invoice_date DateTime64(3, 'UTC'),
    price Decimal32(2),
    id Int32,
    country String
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'KAFKA_HOST_FQDN:9091',
    kafka_topic_list = 'online-retail',
    kafka_group_name = 'sample_group',
    kafka_format = 'JSONEachRow',
    date_time_input_format = 'best_effort';

-- Из таблицы свзяанной с топиком кафки можно прочитать только 1 один раз,
-- поэтому перебрасываем данные сюда
CREATE TABLE sales.temp_online_retail
(
    invoice String,
    stock_code String,
    description String,
    quantity Int32,
    invoice_date DateTime64(3, 'UTC'),
    price Decimal32(2),
    id Int32,
    country String
) ENGINE = MergeTree()
ORDER BY (invoice, stock_code);

-- Создание представления
CREATE MATERIALIZED VIEW sales.online_retail_v TO sales.temp_online_retail
    AS SELECT * FROM sales.online_retail;