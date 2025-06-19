-- Запрос 1
ALTER TABLE online_retail ADD COLUMN invoice_date_final Datetime;

-- Запрос 2
$parse1 = DateTime::Parse("%m/%d/%Y %H:%M");

UPDATE online_retail
SET invoice_date_final = DateTime::MakeDatetime($parse1(invoice_date));

-- Запрос 3
ALTER TABLE online_retail DROP COLUMN invoice_date;
