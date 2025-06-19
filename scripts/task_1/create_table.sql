CREATE TABLE online_retail (
      invoice Utf8,
      stock_code Utf8,
      description Utf8,
      quantity Int32,
      invoice_date Utf8,
      price Decimal(22, 9),
      customer Int32,
      country Utf8,
      PRIMARY KEY (invoice, stock_code)
  );