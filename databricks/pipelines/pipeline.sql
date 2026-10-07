-- ----------------------------------------------------------------------------------------------------
-- Team 4 – Supply Chain Disruption Analytics: ETL pipeline script (Lakeflow Declarative Pipelines, SQL)
-- Raw files -> Bronze -> Silver -> Gold, one pipeline. Catalog data2026acc123456, schema team4.
--
-- Pipeline configuration (Settings -> Configuration):
--   bronze.landing_root   /Volumes/data2026acc123456/team4/landing
--   gold.as_of_date       optional yyyy-MM-dd = business brief snapshot_date (empty = today)
-- Set the pipeline to UTC (spark.sql.session.timeZone = UTC).
-- ----------------------------------------------------------------------------------------------------


-- ----------------------------------------------------------------------------------------------------
-- Bronze layer: raw source records as text, with audit columns (assignment 6.1)
-- ----------------------------------------------------------------------------------------------------

-- bronze_suppliers: one row per CSV data line, columns mapped by header name
CREATE OR REFRESH STREAMING LIVE TABLE bronze_suppliers
(
  CONSTRAINT no_schema_drift     EXPECT (_rescued_data IS NULL),
  CONSTRAINT primary_key_present EXPECT (supplier_id IS NOT NULL),
  CONSTRAINT parseable_record    EXPECT (load_status <> 'MALFORMED')
)
COMMENT 'Raw supplier master data from suppliers*.csv (full snapshot per delivery).'
TBLPROPERTIES ('quality' = 'bronze')
AS
WITH files AS (
  SELECT
    _metadata AS file,
    split(regexp_replace(value, '^\\uFEFF', ''), '\\r?\\n') AS lines
  FROM STREAM(read_files('${bronze.landing_root}/suppliers/', format => 'text', wholeText => true, pathGlobFilter => '*.csv'))
  WHERE NOT (endswith(_metadata.file_name, '_errors.csv') OR endswith(_metadata.file_name, '_errors.json'))
),
headers AS (
  SELECT file, lines, element_at(lines, 1) AS header_line FROM files
),
exploded AS (
  SELECT file, header_line, posexplode(lines) AS (pos, raw_payload) FROM headers
),
fields AS (
  SELECT
    file, raw_payload,
    CAST(pos + 1 AS INT) AS source_line_number,
    filter(transform(transform(split(header_line, ',(?=(?:[^"]*"[^"]*")*[^"]*$)', -1),
        f -> nullif(regexp_replace(regexp_replace(f, '^"(.*)"$', '$1'), '""', '"'), '')), h -> trim(h)), h -> h IS NOT NULL) AS hdr,
    transform(split(raw_payload, ',(?=(?:[^"]*"[^"]*")*[^"]*$)', -1),
        f -> nullif(regexp_replace(regexp_replace(f, '^"(.*)"$', '$1'), '""', '"'), '')) AS vals,
    size(split(raw_payload, ',(?=(?:[^"]*"[^"]*")*[^"]*$)', -1)) AS n_fields
  FROM exploded
  WHERE pos > 0 AND trim(raw_payload) <> ''
),
mapped AS (
  SELECT
    CASE WHEN array_position(hdr, 'supplier_id') > 0 THEN try_element_at(vals, CAST(array_position(hdr, 'supplier_id') AS INT)) END AS supplier_id,
    CASE WHEN array_position(hdr, 'supplier_name') > 0 THEN try_element_at(vals, CAST(array_position(hdr, 'supplier_name') AS INT)) END AS supplier_name,
    CASE WHEN array_position(hdr, 'country_code') > 0 THEN try_element_at(vals, CAST(array_position(hdr, 'country_code') AS INT)) END AS country_code,
    CASE WHEN array_position(hdr, 'region') > 0 THEN try_element_at(vals, CAST(array_position(hdr, 'region') AS INT)) END AS region,
    CASE WHEN array_position(hdr, 'default_currency') > 0 THEN try_element_at(vals, CAST(array_position(hdr, 'default_currency') AS INT)) END AS default_currency,
    CASE WHEN array_position(hdr, 'contract_lead_time_days') > 0 THEN try_element_at(vals, CAST(array_position(hdr, 'contract_lead_time_days') AS INT)) END AS contract_lead_time_days,
    CASE WHEN array_position(hdr, 'risk_rating') > 0 THEN try_element_at(vals, CAST(array_position(hdr, 'risk_rating') AS INT)) END AS risk_rating,
    CASE WHEN array_position(hdr, 'is_active') > 0 THEN try_element_at(vals, CAST(array_position(hdr, 'is_active') AS INT)) END AS is_active,
    CASE WHEN array_position(hdr, 'onboarded_date') > 0 THEN try_element_at(vals, CAST(array_position(hdr, 'onboarded_date') AS INT)) END AS onboarded_date,
    CASE WHEN size(array_except(hdr, array('supplier_id', 'supplier_name', 'country_code', 'region', 'default_currency', 'contract_lead_time_days', 'risk_rating', 'is_active', 'onboarded_date'))) > 0 OR size(array_except(array('supplier_id', 'supplier_name', 'country_code', 'region', 'default_currency', 'contract_lead_time_days', 'risk_rating', 'is_active', 'onboarded_date'), hdr)) > 0
         THEN to_json(named_struct(
                'extra_columns', map_from_arrays(array_except(hdr, array('supplier_id', 'supplier_name', 'country_code', 'region', 'default_currency', 'contract_lead_time_days', 'risk_rating', 'is_active', 'onboarded_date')),
                   transform(array_except(hdr, array('supplier_id', 'supplier_name', 'country_code', 'region', 'default_currency', 'contract_lead_time_days', 'risk_rating', 'is_active', 'onboarded_date')), n -> try_element_at(vals, CAST(array_position(hdr, n) AS INT)))),
                'missing_columns', array_except(array('supplier_id', 'supplier_name', 'country_code', 'region', 'default_currency', 'contract_lead_time_days', 'risk_rating', 'is_active', 'onboarded_date'), hdr))) END AS _rescued_data,
    n_fields <> size(hdr) AS malformed,
    raw_payload, file, source_line_number
  FROM fields
)
SELECT
  supplier_id, supplier_name, country_code, region, default_currency, contract_lead_time_days, risk_rating, is_active, onboarded_date,
  _rescued_data,
  raw_payload,
  file.file_path AS source_file_name,
  source_line_number,
  current_timestamp() AS ingestion_ts,
  concat_ws('-', 'suppliers', date_format(file.file_modification_time, "yyyyMMdd'T'HHmmss"),
            substring(sha2(file.file_path, 256), 1, 8)) AS batch_id,
  'ERP_MASTER_DATA' AS source_system,
  sha2(concat_ws(chr(1),
    coalesce(CAST(supplier_id AS STRING), '<NULL>'),
    coalesce(CAST(supplier_name AS STRING), '<NULL>'),
    coalesce(CAST(country_code AS STRING), '<NULL>'),
    coalesce(CAST(region AS STRING), '<NULL>'),
    coalesce(CAST(default_currency AS STRING), '<NULL>'),
    coalesce(CAST(contract_lead_time_days AS STRING), '<NULL>'),
    coalesce(CAST(risk_rating AS STRING), '<NULL>'),
    coalesce(CAST(is_active AS STRING), '<NULL>'),
    coalesce(CAST(onboarded_date AS STRING), '<NULL>'),
    coalesce(_rescued_data, '<NULL>')), 256) AS record_hash,
  CASE WHEN malformed THEN 'MALFORMED'
       WHEN _rescued_data IS NOT NULL THEN 'SCHEMA_DRIFT'
       ELSE 'LOADED' END AS load_status
FROM mapped;

-- bronze_products: one row per CSV data line, columns mapped by header name
CREATE OR REFRESH STREAMING LIVE TABLE bronze_products
(
  CONSTRAINT no_schema_drift     EXPECT (_rescued_data IS NULL),
  CONSTRAINT primary_key_present EXPECT (product_id IS NOT NULL),
  CONSTRAINT parseable_record    EXPECT (load_status <> 'MALFORMED')
)
COMMENT 'Raw product master data from products*.csv (full snapshot per delivery).'
TBLPROPERTIES ('quality' = 'bronze')
AS
WITH files AS (
  SELECT
    _metadata AS file,
    split(regexp_replace(value, '^\\uFEFF', ''), '\\r?\\n') AS lines
  FROM STREAM(read_files('${bronze.landing_root}/products/', format => 'text', wholeText => true, pathGlobFilter => '*.csv'))
  WHERE NOT (endswith(_metadata.file_name, '_errors.csv') OR endswith(_metadata.file_name, '_errors.json'))
),
headers AS (
  SELECT file, lines, element_at(lines, 1) AS header_line FROM files
),
exploded AS (
  SELECT file, header_line, posexplode(lines) AS (pos, raw_payload) FROM headers
),
fields AS (
  SELECT
    file, raw_payload,
    CAST(pos + 1 AS INT) AS source_line_number,
    filter(transform(transform(split(header_line, ',(?=(?:[^"]*"[^"]*")*[^"]*$)', -1),
        f -> nullif(regexp_replace(regexp_replace(f, '^"(.*)"$', '$1'), '""', '"'), '')), h -> trim(h)), h -> h IS NOT NULL) AS hdr,
    transform(split(raw_payload, ',(?=(?:[^"]*"[^"]*")*[^"]*$)', -1),
        f -> nullif(regexp_replace(regexp_replace(f, '^"(.*)"$', '$1'), '""', '"'), '')) AS vals,
    size(split(raw_payload, ',(?=(?:[^"]*"[^"]*")*[^"]*$)', -1)) AS n_fields
  FROM exploded
  WHERE pos > 0 AND trim(raw_payload) <> ''
),
mapped AS (
  SELECT
    CASE WHEN array_position(hdr, 'product_id') > 0 THEN try_element_at(vals, CAST(array_position(hdr, 'product_id') AS INT)) END AS product_id,
    CASE WHEN array_position(hdr, 'product_name') > 0 THEN try_element_at(vals, CAST(array_position(hdr, 'product_name') AS INT)) END AS product_name,
    CASE WHEN array_position(hdr, 'category') > 0 THEN try_element_at(vals, CAST(array_position(hdr, 'category') AS INT)) END AS category,
    CASE WHEN array_position(hdr, 'unit_of_measure') > 0 THEN try_element_at(vals, CAST(array_position(hdr, 'unit_of_measure') AS INT)) END AS unit_of_measure,
    CASE WHEN array_position(hdr, 'unit_cost') > 0 THEN try_element_at(vals, CAST(array_position(hdr, 'unit_cost') AS INT)) END AS unit_cost,
    CASE WHEN array_position(hdr, 'cost_currency') > 0 THEN try_element_at(vals, CAST(array_position(hdr, 'cost_currency') AS INT)) END AS cost_currency,
    CASE WHEN array_position(hdr, 'criticality') > 0 THEN try_element_at(vals, CAST(array_position(hdr, 'criticality') AS INT)) END AS criticality,
    CASE WHEN array_position(hdr, 'primary_supplier_id') > 0 THEN try_element_at(vals, CAST(array_position(hdr, 'primary_supplier_id') AS INT)) END AS primary_supplier_id,
    CASE WHEN array_position(hdr, 'alternate_supplier_id') > 0 THEN try_element_at(vals, CAST(array_position(hdr, 'alternate_supplier_id') AS INT)) END AS alternate_supplier_id,
    CASE WHEN array_position(hdr, 'stocking_location_id') > 0 THEN try_element_at(vals, CAST(array_position(hdr, 'stocking_location_id') AS INT)) END AS stocking_location_id,
    CASE WHEN array_position(hdr, 'on_hand_qty') > 0 THEN try_element_at(vals, CAST(array_position(hdr, 'on_hand_qty') AS INT)) END AS on_hand_qty,
    CASE WHEN array_position(hdr, 'safety_stock_qty') > 0 THEN try_element_at(vals, CAST(array_position(hdr, 'safety_stock_qty') AS INT)) END AS safety_stock_qty,
    CASE WHEN array_position(hdr, 'avg_daily_demand') > 0 THEN try_element_at(vals, CAST(array_position(hdr, 'avg_daily_demand') AS INT)) END AS avg_daily_demand,
    CASE WHEN size(array_except(hdr, array('product_id', 'product_name', 'category', 'unit_of_measure', 'unit_cost', 'cost_currency', 'criticality', 'primary_supplier_id', 'alternate_supplier_id', 'stocking_location_id', 'on_hand_qty', 'safety_stock_qty', 'avg_daily_demand'))) > 0 OR size(array_except(array('product_id', 'product_name', 'category', 'unit_of_measure', 'unit_cost', 'cost_currency', 'criticality', 'primary_supplier_id', 'alternate_supplier_id', 'stocking_location_id', 'on_hand_qty', 'safety_stock_qty', 'avg_daily_demand'), hdr)) > 0
         THEN to_json(named_struct(
                'extra_columns', map_from_arrays(array_except(hdr, array('product_id', 'product_name', 'category', 'unit_of_measure', 'unit_cost', 'cost_currency', 'criticality', 'primary_supplier_id', 'alternate_supplier_id', 'stocking_location_id', 'on_hand_qty', 'safety_stock_qty', 'avg_daily_demand')),
                   transform(array_except(hdr, array('product_id', 'product_name', 'category', 'unit_of_measure', 'unit_cost', 'cost_currency', 'criticality', 'primary_supplier_id', 'alternate_supplier_id', 'stocking_location_id', 'on_hand_qty', 'safety_stock_qty', 'avg_daily_demand')), n -> try_element_at(vals, CAST(array_position(hdr, n) AS INT)))),
                'missing_columns', array_except(array('product_id', 'product_name', 'category', 'unit_of_measure', 'unit_cost', 'cost_currency', 'criticality', 'primary_supplier_id', 'alternate_supplier_id', 'stocking_location_id', 'on_hand_qty', 'safety_stock_qty', 'avg_daily_demand'), hdr))) END AS _rescued_data,
    n_fields <> size(hdr) AS malformed,
    raw_payload, file, source_line_number
  FROM fields
)
SELECT
  product_id, product_name, category, unit_of_measure, unit_cost, cost_currency, criticality, primary_supplier_id, alternate_supplier_id, stocking_location_id, on_hand_qty, safety_stock_qty, avg_daily_demand,
  _rescued_data,
  raw_payload,
  file.file_path AS source_file_name,
  source_line_number,
  current_timestamp() AS ingestion_ts,
  concat_ws('-', 'products', date_format(file.file_modification_time, "yyyyMMdd'T'HHmmss"),
            substring(sha2(file.file_path, 256), 1, 8)) AS batch_id,
  'ERP_MASTER_DATA' AS source_system,
  sha2(concat_ws(chr(1),
    coalesce(CAST(product_id AS STRING), '<NULL>'),
    coalesce(CAST(product_name AS STRING), '<NULL>'),
    coalesce(CAST(category AS STRING), '<NULL>'),
    coalesce(CAST(unit_of_measure AS STRING), '<NULL>'),
    coalesce(CAST(unit_cost AS STRING), '<NULL>'),
    coalesce(CAST(cost_currency AS STRING), '<NULL>'),
    coalesce(CAST(criticality AS STRING), '<NULL>'),
    coalesce(CAST(primary_supplier_id AS STRING), '<NULL>'),
    coalesce(CAST(alternate_supplier_id AS STRING), '<NULL>'),
    coalesce(CAST(stocking_location_id AS STRING), '<NULL>'),
    coalesce(CAST(on_hand_qty AS STRING), '<NULL>'),
    coalesce(CAST(safety_stock_qty AS STRING), '<NULL>'),
    coalesce(CAST(avg_daily_demand AS STRING), '<NULL>'),
    coalesce(_rescued_data, '<NULL>')), 256) AS record_hash,
  CASE WHEN malformed THEN 'MALFORMED'
       WHEN _rescued_data IS NOT NULL THEN 'SCHEMA_DRIFT'
       ELSE 'LOADED' END AS load_status
FROM mapped;

-- bronze_purchase_orders: one row per CSV data line, columns mapped by header name
CREATE OR REFRESH STREAMING LIVE TABLE bronze_purchase_orders
(
  CONSTRAINT no_schema_drift     EXPECT (_rescued_data IS NULL),
  CONSTRAINT primary_key_present EXPECT (po_id IS NOT NULL AND po_line_number IS NOT NULL),
  CONSTRAINT parseable_record    EXPECT (load_status <> 'MALFORMED')
)
COMMENT 'Raw purchase-order lines from purchase_orders*.csv (incremental deliveries).'
TBLPROPERTIES ('quality' = 'bronze')
AS
WITH files AS (
  SELECT
    _metadata AS file,
    split(regexp_replace(value, '^\\uFEFF', ''), '\\r?\\n') AS lines
  FROM STREAM(read_files('${bronze.landing_root}/purchase_orders/', format => 'text', wholeText => true, pathGlobFilter => '*.csv'))
  WHERE NOT (endswith(_metadata.file_name, '_errors.csv') OR endswith(_metadata.file_name, '_errors.json'))
),
headers AS (
  SELECT file, lines, element_at(lines, 1) AS header_line FROM files
),
exploded AS (
  SELECT file, header_line, posexplode(lines) AS (pos, raw_payload) FROM headers
),
fields AS (
  SELECT
    file, raw_payload,
    CAST(pos + 1 AS INT) AS source_line_number,
    filter(transform(transform(split(header_line, ',(?=(?:[^"]*"[^"]*")*[^"]*$)', -1),
        f -> nullif(regexp_replace(regexp_replace(f, '^"(.*)"$', '$1'), '""', '"'), '')), h -> trim(h)), h -> h IS NOT NULL) AS hdr,
    transform(split(raw_payload, ',(?=(?:[^"]*"[^"]*")*[^"]*$)', -1),
        f -> nullif(regexp_replace(regexp_replace(f, '^"(.*)"$', '$1'), '""', '"'), '')) AS vals,
    size(split(raw_payload, ',(?=(?:[^"]*"[^"]*")*[^"]*$)', -1)) AS n_fields
  FROM exploded
  WHERE pos > 0 AND trim(raw_payload) <> ''
),
mapped AS (
  SELECT
    CASE WHEN array_position(hdr, 'po_id') > 0 THEN try_element_at(vals, CAST(array_position(hdr, 'po_id') AS INT)) END AS po_id,
    CASE WHEN array_position(hdr, 'po_line_number') > 0 THEN try_element_at(vals, CAST(array_position(hdr, 'po_line_number') AS INT)) END AS po_line_number,
    CASE WHEN array_position(hdr, 'supplier_id') > 0 THEN try_element_at(vals, CAST(array_position(hdr, 'supplier_id') AS INT)) END AS supplier_id,
    CASE WHEN array_position(hdr, 'product_id') > 0 THEN try_element_at(vals, CAST(array_position(hdr, 'product_id') AS INT)) END AS product_id,
    CASE WHEN array_position(hdr, 'order_date') > 0 THEN try_element_at(vals, CAST(array_position(hdr, 'order_date') AS INT)) END AS order_date,
    CASE WHEN array_position(hdr, 'promised_delivery_date') > 0 THEN try_element_at(vals, CAST(array_position(hdr, 'promised_delivery_date') AS INT)) END AS promised_delivery_date,
    CASE WHEN array_position(hdr, 'ordered_qty') > 0 THEN try_element_at(vals, CAST(array_position(hdr, 'ordered_qty') AS INT)) END AS ordered_qty,
    CASE WHEN array_position(hdr, 'unit_price') > 0 THEN try_element_at(vals, CAST(array_position(hdr, 'unit_price') AS INT)) END AS unit_price,
    CASE WHEN array_position(hdr, 'currency') > 0 THEN try_element_at(vals, CAST(array_position(hdr, 'currency') AS INT)) END AS currency,
    CASE WHEN array_position(hdr, 'delivery_location_id') > 0 THEN try_element_at(vals, CAST(array_position(hdr, 'delivery_location_id') AS INT)) END AS delivery_location_id,
    CASE WHEN array_position(hdr, 'line_status') > 0 THEN try_element_at(vals, CAST(array_position(hdr, 'line_status') AS INT)) END AS line_status,
    CASE WHEN array_position(hdr, 'last_modified_ts') > 0 THEN try_element_at(vals, CAST(array_position(hdr, 'last_modified_ts') AS INT)) END AS last_modified_ts,
    CASE WHEN size(array_except(hdr, array('po_id', 'po_line_number', 'supplier_id', 'product_id', 'order_date', 'promised_delivery_date', 'ordered_qty', 'unit_price', 'currency', 'delivery_location_id', 'line_status', 'last_modified_ts'))) > 0 OR size(array_except(array('po_id', 'po_line_number', 'supplier_id', 'product_id', 'order_date', 'promised_delivery_date', 'ordered_qty', 'unit_price', 'currency', 'delivery_location_id', 'line_status', 'last_modified_ts'), hdr)) > 0
         THEN to_json(named_struct(
                'extra_columns', map_from_arrays(array_except(hdr, array('po_id', 'po_line_number', 'supplier_id', 'product_id', 'order_date', 'promised_delivery_date', 'ordered_qty', 'unit_price', 'currency', 'delivery_location_id', 'line_status', 'last_modified_ts')),
                   transform(array_except(hdr, array('po_id', 'po_line_number', 'supplier_id', 'product_id', 'order_date', 'promised_delivery_date', 'ordered_qty', 'unit_price', 'currency', 'delivery_location_id', 'line_status', 'last_modified_ts')), n -> try_element_at(vals, CAST(array_position(hdr, n) AS INT)))),
                'missing_columns', array_except(array('po_id', 'po_line_number', 'supplier_id', 'product_id', 'order_date', 'promised_delivery_date', 'ordered_qty', 'unit_price', 'currency', 'delivery_location_id', 'line_status', 'last_modified_ts'), hdr))) END AS _rescued_data,
    n_fields <> size(hdr) AS malformed,
    raw_payload, file, source_line_number
  FROM fields
)
SELECT
  po_id, po_line_number, supplier_id, product_id, order_date, promised_delivery_date, ordered_qty, unit_price, currency, delivery_location_id, line_status, last_modified_ts,
  _rescued_data,
  raw_payload,
  file.file_path AS source_file_name,
  source_line_number,
  current_timestamp() AS ingestion_ts,
  concat_ws('-', 'purchase_orders', date_format(file.file_modification_time, "yyyyMMdd'T'HHmmss"),
            substring(sha2(file.file_path, 256), 1, 8)) AS batch_id,
  'ERP_PROCUREMENT' AS source_system,
  sha2(concat_ws(chr(1),
    coalesce(CAST(po_id AS STRING), '<NULL>'),
    coalesce(CAST(po_line_number AS STRING), '<NULL>'),
    coalesce(CAST(supplier_id AS STRING), '<NULL>'),
    coalesce(CAST(product_id AS STRING), '<NULL>'),
    coalesce(CAST(order_date AS STRING), '<NULL>'),
    coalesce(CAST(promised_delivery_date AS STRING), '<NULL>'),
    coalesce(CAST(ordered_qty AS STRING), '<NULL>'),
    coalesce(CAST(unit_price AS STRING), '<NULL>'),
    coalesce(CAST(currency AS STRING), '<NULL>'),
    coalesce(CAST(delivery_location_id AS STRING), '<NULL>'),
    coalesce(CAST(line_status AS STRING), '<NULL>'),
    coalesce(CAST(last_modified_ts AS STRING), '<NULL>'),
    coalesce(_rescued_data, '<NULL>')), 256) AS record_hash,
  CASE WHEN malformed THEN 'MALFORMED'
       WHEN _rescued_data IS NOT NULL THEN 'SCHEMA_DRIFT'
       ELSE 'LOADED' END AS load_status
FROM mapped;

-- bronze_shipment_events: one row per JSON line (one event), nested structure kept
CREATE OR REFRESH STREAMING LIVE TABLE bronze_shipment_events
(
  CONSTRAINT no_schema_drift     EXPECT (_rescued_data IS NULL),
  CONSTRAINT primary_key_present EXPECT (event_id IS NOT NULL),
  CONSTRAINT parseable_record    EXPECT (load_status <> 'MALFORMED')
)
COMMENT 'Raw carrier shipment events from shipment_events*.json, one row per JSON line, nested structure preserved; original line kept in raw_payload.'
TBLPROPERTIES ('quality' = 'bronze')
AS
WITH files AS (
  SELECT
    _metadata AS file,
    split(regexp_replace(value, '^\\uFEFF', ''), '\\r?\\n') AS lines
  FROM STREAM(read_files('${bronze.landing_root}/shipment_events/', format => 'text', wholeText => true, pathGlobFilter => '*.json'))
  WHERE NOT (endswith(_metadata.file_name, '_errors.csv') OR endswith(_metadata.file_name, '_errors.json'))
),
exploded AS (
  SELECT file, posexplode(lines) AS (pos, raw_payload) FROM files
),
parsed AS (
  SELECT
    file, raw_payload,
    CAST(pos + 1 AS INT) AS source_line_number,
    json_object_keys(raw_payload) AS json_keys,
    from_json(raw_payload, 'event_id STRING, shipment_id STRING, po_id STRING, carrier STRING, status STRING, event_ts STRING, location STRUCT<location_id: STRING, name: STRING, country: STRING, type: STRING>, eta_ts STRING, items ARRAY<STRUCT<po_line_number: STRING, product_id: STRING, quantity: STRING>>, delay_reason_code STRING') AS j
  FROM exploded
  WHERE trim(raw_payload) <> ''
),
mapped AS (
  SELECT
    j.event_id AS event_id, j.shipment_id AS shipment_id, j.po_id AS po_id, j.carrier AS carrier, j.status AS status, j.event_ts AS event_ts, j.location AS location, j.eta_ts AS eta_ts, j.items AS items, j.delay_reason_code AS delay_reason_code,
    CASE WHEN size(array_except(json_keys, array('event_id', 'shipment_id', 'po_id', 'carrier', 'status', 'event_ts', 'location', 'eta_ts', 'items', 'delay_reason_code'))) > 0 THEN to_json(array_except(json_keys, array('event_id', 'shipment_id', 'po_id', 'carrier', 'status', 'event_ts', 'location', 'eta_ts', 'items', 'delay_reason_code'))) END AS _rescued_data,
    json_keys IS NULL AS malformed,
    raw_payload, file, source_line_number
  FROM parsed
)
SELECT
  event_id, shipment_id, po_id, carrier, status, event_ts, location, eta_ts, items, delay_reason_code,
  _rescued_data,
  raw_payload,
  file.file_path AS source_file_name,
  source_line_number,
  current_timestamp() AS ingestion_ts,
  concat_ws('-', 'shipment_events', date_format(file.file_modification_time, "yyyyMMdd'T'HHmmss"),
            substring(sha2(file.file_path, 256), 1, 8)) AS batch_id,
  'CARRIER_TMS' AS source_system,
  sha2(concat_ws(chr(1),
    coalesce(CAST(event_id AS STRING), '<NULL>'),
    coalesce(CAST(shipment_id AS STRING), '<NULL>'),
    coalesce(CAST(po_id AS STRING), '<NULL>'),
    coalesce(CAST(carrier AS STRING), '<NULL>'),
    coalesce(CAST(status AS STRING), '<NULL>'),
    coalesce(CAST(event_ts AS STRING), '<NULL>'),
    coalesce(to_json(location), '<NULL>'),
    coalesce(CAST(eta_ts AS STRING), '<NULL>'),
    coalesce(to_json(items), '<NULL>'),
    coalesce(CAST(delay_reason_code AS STRING), '<NULL>'),
    coalesce(_rescued_data, '<NULL>')), 256) AS record_hash,
  CASE WHEN malformed THEN 'MALFORMED'
       WHEN _rescued_data IS NOT NULL THEN 'SCHEMA_DRIFT'
       ELSE 'LOADED' END AS load_status
FROM mapped;

-- ops_bronze_ingestion_log: input / written / rejected counts per batch (assignment 6.1)
CREATE OR REFRESH LIVE TABLE ops_bronze_ingestion_log
COMMENT 'Assignment 6.1: one row per ingested file version (batch) with input, written and rejected counts. Rejected = MALFORMED rows (kept in Bronze with raw_payload, excluded from Silver). Duration and failure details: pipeline event log ops_pipeline_event_log.'
AS
SELECT
  batch_id, source_file_name, source_system,
  COUNT(1) AS input_count,
  SUM(CAST(load_status <> 'MALFORMED' AS INT)) AS written_count,
  SUM(CAST(load_status = 'MALFORMED' AS INT)) AS rejected_count,
  SUM(CAST(load_status = 'SCHEMA_DRIFT' AS INT)) AS schema_drift_count,
  MAX(ingestion_ts) AS ingestion_ts,
  'bronze_suppliers' AS target_table
FROM LIVE.bronze_suppliers
GROUP BY batch_id, source_file_name, source_system
UNION ALL
SELECT
  batch_id, source_file_name, source_system,
  COUNT(1) AS input_count,
  SUM(CAST(load_status <> 'MALFORMED' AS INT)) AS written_count,
  SUM(CAST(load_status = 'MALFORMED' AS INT)) AS rejected_count,
  SUM(CAST(load_status = 'SCHEMA_DRIFT' AS INT)) AS schema_drift_count,
  MAX(ingestion_ts) AS ingestion_ts,
  'bronze_products' AS target_table
FROM LIVE.bronze_products
GROUP BY batch_id, source_file_name, source_system
UNION ALL
SELECT
  batch_id, source_file_name, source_system,
  COUNT(1) AS input_count,
  SUM(CAST(load_status <> 'MALFORMED' AS INT)) AS written_count,
  SUM(CAST(load_status = 'MALFORMED' AS INT)) AS rejected_count,
  SUM(CAST(load_status = 'SCHEMA_DRIFT' AS INT)) AS schema_drift_count,
  MAX(ingestion_ts) AS ingestion_ts,
  'bronze_purchase_orders' AS target_table
FROM LIVE.bronze_purchase_orders
GROUP BY batch_id, source_file_name, source_system
UNION ALL
SELECT
  batch_id, source_file_name, source_system,
  COUNT(1) AS input_count,
  SUM(CAST(load_status <> 'MALFORMED' AS INT)) AS written_count,
  SUM(CAST(load_status = 'MALFORMED' AS INT)) AS rejected_count,
  SUM(CAST(load_status = 'SCHEMA_DRIFT' AS INT)) AS schema_drift_count,
  MAX(ingestion_ts) AS ingestion_ts,
  'bronze_shipment_events' AS target_table
FROM LIVE.bronze_shipment_events
GROUP BY batch_id, source_file_name, source_system;


-- ----------------------------------------------------------------------------------------------------
-- Silver layer: typed, standardised, deduplicated, validated (assignment 6.2)
-- Each *_checked table (internal) carries dq_issues = array of rule hits with an action:
--   QUARANTINE (row excluded, sent to silver_quarantine), NULLIFY (bad value set to NULL),
--   FIX (value standardised), FLAG (row kept unchanged).
-- ----------------------------------------------------------------------------------------------------

-- silver_fx_rates: business brief fx_rates seed (PLACEHOLDER rates, versioned with the code)
CREATE OR REFRESH LIVE TABLE silver_fx_rates
COMMENT 'fx_rates seed: rate_to_eur per (currency, valid_from), versioned with code'
TBLPROPERTIES ('quality' = 'silver')
AS
SELECT currency, valid_from, CAST(CAST(rate AS DOUBLE) AS DECIMAL(18,8)) AS rate_to_eur
FROM VALUES
  ('EUR', DATE'2000-01-01', 1.0),
  ('USD', DATE'2025-01-01', 0.86),
  ('GBP', DATE'2025-01-01', 1.15),
  ('CHF', DATE'2025-01-01', 1.07),
  ('CNY', DATE'2025-01-01', 0.12),
  ('INR', DATE'2025-01-01', 0.0099),
  ('JPY', DATE'2025-01-01', 0.0058),
  ('KRW', DATE'2025-01-01', 0.00062),
  ('MXN', DATE'2025-01-01', 0.047),
  ('PLN', DATE'2025-01-01', 0.235)
AS fx(currency, valid_from, rate);

-- suppliers_checked (internal): latest supplier extract, typed, every rule hit in dq_issues
CREATE OR REFRESH TEMPORARY LIVE TABLE suppliers_checked
AS
WITH src AS (
  SELECT supplier_id AS supplier_id__raw, supplier_name AS supplier_name__raw, country_code AS country_code__raw, region AS region__raw, default_currency AS default_currency__raw, contract_lead_time_days AS contract_lead_time_days__raw, risk_rating AS risk_rating__raw, is_active AS is_active__raw, onboarded_date AS onboarded_date__raw,
         load_status, _rescued_data, raw_payload, record_hash, source_file_name, source_line_number, batch_id
  FROM LIVE.bronze_suppliers
  WHERE batch_id = (SELECT max(batch_id) FROM LIVE.bronze_suppliers)
),
typed AS (
  SELECT *,
    upper(nullif(trim(supplier_id__raw), '')) AS supplier_id,
    nullif(trim(supplier_name__raw), '') AS supplier_name,
    upper(nullif(trim(country_code__raw), '')) AS country_code,
    upper(nullif(trim(region__raw), '')) AS region,
    CASE WHEN upper(nullif(trim(default_currency__raw), '')) IN ('EUR', 'USD', 'GBP', 'CHF', 'CNY', 'INR', 'JPY', 'KRW', 'MXN', 'PLN', 'TWD', 'VND', 'SEK', 'NOK', 'DKK', 'CZK', 'HUF', 'RON', 'TRY', 'CAD', 'AUD', 'BRL', 'SGD', 'HKD', 'THB', 'MYR', 'IDR', 'ZAR', 'AED', 'SAR') THEN upper(nullif(trim(default_currency__raw), '')) END AS default_currency,
    try_cast(trim(contract_lead_time_days__raw) AS INT) AS lead_int,
    upper(nullif(trim(risk_rating__raw), '')) AS risk_rating,
    CASE WHEN lower(nullif(trim(is_active__raw), '')) IN ('true', '1', 'yes', 'y') THEN true
         WHEN lower(nullif(trim(is_active__raw), '')) IN ('false', '0', 'no', 'n') THEN false END AS is_active,
    try_cast(trim(onboarded_date__raw) AS DATE) AS onboarded_date
  FROM src
),
ranked AS (
  SELECT *,
    CASE WHEN lead_int > 0 THEN lead_int END AS contract_lead_time_days,
    row_number() OVER (PARTITION BY supplier_id ORDER BY source_line_number) AS rn,
    first_value(source_line_number) OVER w AS first_line,
    first_value(record_hash) OVER w AS first_hash
  FROM typed
  WINDOW w AS (PARTITION BY supplier_id ORDER BY source_line_number
               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
)
SELECT *,
  filter(array(
    CASE WHEN load_status = 'MALFORMED' THEN named_struct('rule', 'malformed_record', 'action', 'QUARANTINE', 'column_name', '*', 'description', 'Row could not be parsed in Bronze') END,
    CASE WHEN load_status = 'SCHEMA_DRIFT' THEN named_struct('rule', 'schema_drift', 'action', 'FLAG', 'column_name', '_rescued_data', 'description', concat('Schema drift: ', _rescued_data)) END,
    CASE WHEN nullif(trim(supplier_id__raw), '') IS NULL THEN named_struct('rule', 'missing_required', 'action', 'QUARANTINE', 'column_name', 'supplier_id', 'description', 'supplier_id is empty') END,
    CASE WHEN nullif(trim(supplier_name__raw), '') IS NULL THEN named_struct('rule', 'missing_required', 'action', 'FLAG', 'column_name', 'supplier_name', 'description', 'supplier_name is empty') END,
    CASE WHEN nullif(trim(default_currency__raw), '') IS NOT NULL AND upper(nullif(trim(default_currency__raw), '')) IN ('EUR', 'USD', 'GBP', 'CHF', 'CNY', 'INR', 'JPY', 'KRW', 'MXN', 'PLN', 'TWD', 'VND', 'SEK', 'NOK', 'DKK', 'CZK', 'HUF', 'RON', 'TRY', 'CAD', 'AUD', 'BRL', 'SGD', 'HKD', 'THB', 'MYR', 'IDR', 'ZAR', 'AED', 'SAR') AND nullif(trim(default_currency__raw), '') <> upper(nullif(trim(default_currency__raw), '')) THEN named_struct('rule', 'currency_inconsistency', 'action', 'FIX', 'column_name', 'default_currency', 'description', concat('default_currency \'', nullif(trim(default_currency__raw), ''), '\' normalised to \'', upper(nullif(trim(default_currency__raw), '')), '\'')) END,
    CASE WHEN nullif(trim(default_currency__raw), '') IS NOT NULL AND NOT upper(nullif(trim(default_currency__raw), '')) IN ('EUR', 'USD', 'GBP', 'CHF', 'CNY', 'INR', 'JPY', 'KRW', 'MXN', 'PLN', 'TWD', 'VND', 'SEK', 'NOK', 'DKK', 'CZK', 'HUF', 'RON', 'TRY', 'CAD', 'AUD', 'BRL', 'SGD', 'HKD', 'THB', 'MYR', 'IDR', 'ZAR', 'AED', 'SAR') THEN named_struct('rule', 'currency_inconsistency', 'action', 'NULLIFY', 'column_name', 'default_currency', 'description', concat('default_currency \'', nullif(trim(default_currency__raw), ''), '\' is not a valid ISO 4217 code')) END,
    CASE WHEN nullif(trim(contract_lead_time_days__raw), '') IS NOT NULL AND lead_int IS NULL THEN named_struct('rule', 'invalid_type', 'action', 'NULLIFY', 'column_name', 'contract_lead_time_days', 'description', concat('contract_lead_time_days \'', contract_lead_time_days__raw, '\' is not a valid integer')) END,
    CASE WHEN lead_int <= 0 THEN named_struct('rule', 'invalid_value', 'action', 'NULLIFY', 'column_name', 'contract_lead_time_days', 'description', 'contract_lead_time_days must be > 0') END,
    CASE WHEN nullif(trim(is_active__raw), '') IS NOT NULL AND is_active IS NULL THEN named_struct('rule', 'invalid_type', 'action', 'NULLIFY', 'column_name', 'is_active', 'description', concat('is_active \'', is_active__raw, '\' is not a boolean')) END,
    CASE WHEN nullif(trim(onboarded_date__raw), '') IS NOT NULL AND onboarded_date IS NULL THEN named_struct('rule', 'invalid_type', 'action', 'NULLIFY', 'column_name', 'onboarded_date', 'description', concat('onboarded_date \'', onboarded_date__raw, '\' is not a valid date (yyyy-MM-dd)')) END,
    CASE WHEN country_code IS NOT NULL AND NOT country_code RLIKE '^[A-Z]{2}$' THEN named_struct('rule', 'invalid_domain', 'action', 'FLAG', 'column_name', 'country_code', 'description', 'country_code is not ISO 3166 alpha-2') END,
    CASE WHEN region IS NOT NULL AND NOT region IN ('EMEA', 'APAC', 'AMER') THEN named_struct('rule', 'invalid_domain', 'action', 'FLAG', 'column_name', 'region', 'description', concat('region \'', region, '\' not in allowed values')) END,
    CASE WHEN risk_rating IS NOT NULL AND NOT risk_rating IN ('LOW', 'MEDIUM', 'HIGH') THEN named_struct('rule', 'invalid_domain', 'action', 'FLAG', 'column_name', 'risk_rating', 'description', concat('risk_rating \'', risk_rating, '\' not in allowed values')) END,
    CASE WHEN rn > 1 THEN named_struct('rule', 'duplicate_supplier', 'action', 'QUARANTINE', 'column_name', 'supplier_id', 'description', concat('Duplicate supplier_id (', CAST(supplier_id AS STRING), ') - first occurrence on line ', CAST(first_line AS STRING), CASE WHEN record_hash = first_hash THEN ', exact copy' ELSE ', conflicting attributes' END)) END
  ), x -> x IS NOT NULL) AS dq_issues
FROM ranked;

-- silver_suppliers
CREATE OR REFRESH LIVE TABLE silver_suppliers
(
  CONSTRAINT supplier_id_present EXPECT (supplier_id IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'suppliers: current snapshot, validated'
TBLPROPERTIES ('quality' = 'silver')
AS
SELECT supplier_id, supplier_name, country_code, region, default_currency, contract_lead_time_days, risk_rating, is_active, onboarded_date,
       source_file_name, source_line_number, batch_id
FROM LIVE.suppliers_checked
WHERE NOT exists(dq_issues, x -> x.action = 'QUARANTINE');

-- products_checked (internal): latest product extract, typed, supplier references checked
CREATE OR REFRESH TEMPORARY LIVE TABLE products_checked
AS
WITH src AS (
  SELECT product_id AS product_id__raw, product_name AS product_name__raw, category AS category__raw, unit_of_measure AS unit_of_measure__raw, unit_cost AS unit_cost__raw, cost_currency AS cost_currency__raw, criticality AS criticality__raw, primary_supplier_id AS primary_supplier_id__raw, alternate_supplier_id AS alternate_supplier_id__raw, stocking_location_id AS stocking_location_id__raw, on_hand_qty AS on_hand_qty__raw, safety_stock_qty AS safety_stock_qty__raw, avg_daily_demand AS avg_daily_demand__raw,
         load_status, _rescued_data, raw_payload, record_hash, source_file_name, source_line_number, batch_id
  FROM LIVE.bronze_products
  WHERE batch_id = (SELECT max(batch_id) FROM LIVE.bronze_products)
),
typed AS (
  SELECT *,
    upper(nullif(trim(product_id__raw), '')) AS product_id,
    nullif(trim(product_name__raw), '') AS product_name,
    nullif(trim(category__raw), '') AS category,
    upper(nullif(trim(unit_of_measure__raw), '')) AS unit_of_measure,
    try_cast(trim(unit_cost__raw) AS DECIMAL(18,4)) AS unit_cost_num,
    CASE WHEN upper(nullif(trim(cost_currency__raw), '')) IN ('EUR', 'USD', 'GBP', 'CHF', 'CNY', 'INR', 'JPY', 'KRW', 'MXN', 'PLN', 'TWD', 'VND', 'SEK', 'NOK', 'DKK', 'CZK', 'HUF', 'RON', 'TRY', 'CAD', 'AUD', 'BRL', 'SGD', 'HKD', 'THB', 'MYR', 'IDR', 'ZAR', 'AED', 'SAR') THEN upper(nullif(trim(cost_currency__raw), '')) END AS cost_currency,
    upper(nullif(trim(criticality__raw), '')) AS criticality,
    upper(nullif(trim(primary_supplier_id__raw), '')) AS primary_supplier_id,
    upper(nullif(trim(alternate_supplier_id__raw), '')) AS alternate_supplier_in,
    upper(nullif(trim(stocking_location_id__raw), '')) AS stocking_location_id,
    try_cast(trim(on_hand_qty__raw) AS DECIMAL(18,2)) AS on_hand_qty_num,
    try_cast(trim(safety_stock_qty__raw) AS DECIMAL(18,2)) AS safety_stock_qty_num,
    try_cast(trim(avg_daily_demand__raw) AS DECIMAL(18,2)) AS avg_daily_demand_num
  FROM src
),
checked AS (
  SELECT t.*,
    p.supplier_id IS NOT NULL AS primary_ok,
    a.supplier_id IS NOT NULL AS alternate_ok
  FROM typed t
  LEFT JOIN LIVE.silver_suppliers p ON t.primary_supplier_id = p.supplier_id
  LEFT JOIN LIVE.silver_suppliers a ON t.alternate_supplier_in = a.supplier_id
),
ranked AS (
  SELECT *,
    CASE WHEN unit_cost_num >= 0 THEN unit_cost_num END AS unit_cost,
    CASE WHEN on_hand_qty_num >= 0 THEN on_hand_qty_num END AS on_hand_qty,
    CASE WHEN safety_stock_qty_num >= 0 THEN safety_stock_qty_num END AS safety_stock_qty,
    CASE WHEN avg_daily_demand_num >= 0 THEN avg_daily_demand_num END AS avg_daily_demand,
    CASE WHEN alternate_ok THEN alternate_supplier_in END AS alternate_supplier_id,
    row_number() OVER (PARTITION BY product_id ORDER BY source_line_number) AS rn,
    first_value(source_line_number) OVER w AS first_line,
    first_value(record_hash) OVER w AS first_hash
  FROM checked
  WINDOW w AS (PARTITION BY product_id ORDER BY source_line_number
               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
)
SELECT *,
  filter(array(
    CASE WHEN load_status = 'MALFORMED' THEN named_struct('rule', 'malformed_record', 'action', 'QUARANTINE', 'column_name', '*', 'description', 'Row could not be parsed in Bronze') END,
    CASE WHEN load_status = 'SCHEMA_DRIFT' THEN named_struct('rule', 'schema_drift', 'action', 'FLAG', 'column_name', '_rescued_data', 'description', concat('Schema drift: ', _rescued_data)) END,
    CASE WHEN nullif(trim(product_id__raw), '') IS NULL THEN named_struct('rule', 'missing_required', 'action', 'QUARANTINE', 'column_name', 'product_id', 'description', 'product_id is empty') END,
    CASE WHEN nullif(trim(product_name__raw), '') IS NULL THEN named_struct('rule', 'missing_required', 'action', 'FLAG', 'column_name', 'product_name', 'description', 'product_name is empty') END,
    CASE WHEN nullif(trim(unit_cost__raw), '') IS NOT NULL AND unit_cost_num IS NULL THEN named_struct('rule', 'invalid_type', 'action', 'NULLIFY', 'column_name', 'unit_cost', 'description', concat('unit_cost \'', unit_cost__raw, '\' is not a valid decimal')) END,
    CASE WHEN unit_cost_num < 0 THEN named_struct('rule', 'negative_price', 'action', 'NULLIFY', 'column_name', 'unit_cost', 'description', concat('unit_cost is negative (', unit_cost__raw, ')')) END,
    CASE WHEN nullif(trim(cost_currency__raw), '') IS NOT NULL AND upper(nullif(trim(cost_currency__raw), '')) IN ('EUR', 'USD', 'GBP', 'CHF', 'CNY', 'INR', 'JPY', 'KRW', 'MXN', 'PLN', 'TWD', 'VND', 'SEK', 'NOK', 'DKK', 'CZK', 'HUF', 'RON', 'TRY', 'CAD', 'AUD', 'BRL', 'SGD', 'HKD', 'THB', 'MYR', 'IDR', 'ZAR', 'AED', 'SAR') AND nullif(trim(cost_currency__raw), '') <> upper(nullif(trim(cost_currency__raw), '')) THEN named_struct('rule', 'currency_inconsistency', 'action', 'FIX', 'column_name', 'cost_currency', 'description', concat('cost_currency \'', nullif(trim(cost_currency__raw), ''), '\' normalised to \'', upper(nullif(trim(cost_currency__raw), '')), '\'')) END,
    CASE WHEN nullif(trim(cost_currency__raw), '') IS NOT NULL AND NOT upper(nullif(trim(cost_currency__raw), '')) IN ('EUR', 'USD', 'GBP', 'CHF', 'CNY', 'INR', 'JPY', 'KRW', 'MXN', 'PLN', 'TWD', 'VND', 'SEK', 'NOK', 'DKK', 'CZK', 'HUF', 'RON', 'TRY', 'CAD', 'AUD', 'BRL', 'SGD', 'HKD', 'THB', 'MYR', 'IDR', 'ZAR', 'AED', 'SAR') THEN named_struct('rule', 'currency_inconsistency', 'action', 'NULLIFY', 'column_name', 'cost_currency', 'description', concat('cost_currency \'', nullif(trim(cost_currency__raw), ''), '\' is not a valid ISO 4217 code')) END,
    CASE WHEN nullif(trim(on_hand_qty__raw), '') IS NOT NULL AND on_hand_qty_num IS NULL THEN named_struct('rule', 'invalid_type', 'action', 'NULLIFY', 'column_name', 'on_hand_qty', 'description', concat('on_hand_qty \'', on_hand_qty__raw, '\' is not a valid decimal')) END,
    CASE WHEN nullif(trim(safety_stock_qty__raw), '') IS NOT NULL AND safety_stock_qty_num IS NULL THEN named_struct('rule', 'invalid_type', 'action', 'NULLIFY', 'column_name', 'safety_stock_qty', 'description', concat('safety_stock_qty \'', safety_stock_qty__raw, '\' is not a valid decimal')) END,
    CASE WHEN nullif(trim(avg_daily_demand__raw), '') IS NOT NULL AND avg_daily_demand_num IS NULL THEN named_struct('rule', 'invalid_type', 'action', 'NULLIFY', 'column_name', 'avg_daily_demand', 'description', concat('avg_daily_demand \'', avg_daily_demand__raw, '\' is not a valid decimal')) END,
    CASE WHEN on_hand_qty_num < 0 THEN named_struct('rule', 'negative_quantity', 'action', 'NULLIFY', 'column_name', 'on_hand_qty', 'description', concat('on_hand_qty is negative (', on_hand_qty__raw, ')')) END,
    CASE WHEN safety_stock_qty_num < 0 THEN named_struct('rule', 'negative_quantity', 'action', 'NULLIFY', 'column_name', 'safety_stock_qty', 'description', concat('safety_stock_qty is negative (', safety_stock_qty__raw, ')')) END,
    CASE WHEN avg_daily_demand_num < 0 THEN named_struct('rule', 'negative_quantity', 'action', 'NULLIFY', 'column_name', 'avg_daily_demand', 'description', concat('avg_daily_demand is negative (', avg_daily_demand__raw, ')')) END,
    CASE WHEN category IS NOT NULL AND NOT category IN ('Raw Materials', 'Packaging', 'Electronic Components', 'Fasteners', 'Mechanical Parts', 'Chemicals') THEN named_struct('rule', 'invalid_domain', 'action', 'FLAG', 'column_name', 'category', 'description', concat('category \'', category, '\' not in allowed values')) END,
    CASE WHEN unit_of_measure IS NOT NULL AND NOT unit_of_measure IN ('EA', 'KG', 'L') THEN named_struct('rule', 'invalid_domain', 'action', 'FLAG', 'column_name', 'unit_of_measure', 'description', concat('unit_of_measure \'', unit_of_measure, '\' not in allowed values')) END,
    CASE WHEN criticality IS NOT NULL AND NOT criticality IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL') THEN named_struct('rule', 'invalid_domain', 'action', 'FLAG', 'column_name', 'criticality', 'description', concat('criticality \'', criticality, '\' not in allowed values')) END,
    CASE WHEN primary_supplier_id IS NOT NULL AND NOT primary_ok THEN named_struct('rule', 'unknown_supplier', 'action', 'FLAG', 'column_name', 'primary_supplier_id', 'description', concat('primary_supplier_id ', primary_supplier_id, ' does not exist in silver_suppliers')) END,
    CASE WHEN alternate_supplier_in IS NOT NULL AND NOT alternate_ok THEN named_struct('rule', 'unknown_supplier', 'action', 'NULLIFY', 'column_name', 'alternate_supplier_id', 'description', concat('alternate_supplier_id ', alternate_supplier_in, ' does not exist in silver_suppliers')) END,
    CASE WHEN rn > 1 THEN named_struct('rule', 'duplicate_product', 'action', 'QUARANTINE', 'column_name', 'product_id', 'description', concat('Duplicate product_id (', CAST(product_id AS STRING), ') - first occurrence on line ', CAST(first_line AS STRING), CASE WHEN record_hash = first_hash THEN ', exact copy' ELSE ', conflicting attributes' END)) END
  ), x -> x IS NOT NULL) AS dq_issues
FROM ranked;

-- silver_products
CREATE OR REFRESH LIVE TABLE silver_products
(
  CONSTRAINT product_id_present EXPECT (product_id IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'products: current snapshot, validated'
TBLPROPERTIES ('quality' = 'silver')
AS
SELECT product_id, product_name, category, unit_of_measure, unit_cost, cost_currency, criticality, primary_supplier_id, alternate_supplier_id, stocking_location_id, on_hand_qty, safety_stock_qty, avg_daily_demand,
       source_file_name, source_line_number, batch_id
FROM LIVE.products_checked
WHERE NOT exists(dq_issues, x -> x.action = 'QUARANTINE');

-- purchase_orders_typed (internal): every PO-line version, typed (also the target of the event FKs)
CREATE OR REFRESH TEMPORARY LIVE TABLE purchase_orders_typed
AS
WITH src AS (
  SELECT po_id AS po_id__raw, po_line_number AS po_line_number__raw, supplier_id AS supplier_id__raw, product_id AS product_id__raw, order_date AS order_date__raw, promised_delivery_date AS promised_delivery_date__raw, ordered_qty AS ordered_qty__raw, unit_price AS unit_price__raw, currency AS currency__raw, delivery_location_id AS delivery_location_id__raw, line_status AS line_status__raw, last_modified_ts AS last_modified_ts__raw,
         load_status, _rescued_data, raw_payload, record_hash, source_file_name, source_line_number, batch_id
  FROM LIVE.bronze_purchase_orders
)
SELECT *,
  upper(nullif(trim(po_id__raw), '')) AS po_id,
  try_cast(trim(po_line_number__raw) AS INT) AS po_line_number,
  upper(nullif(trim(supplier_id__raw), '')) AS supplier_id,
  upper(nullif(trim(product_id__raw), '')) AS product_id,
  try_cast(trim(order_date__raw) AS DATE) AS order_date,
  try_cast(trim(promised_delivery_date__raw) AS DATE) AS promised_delivery_date,
  try_cast(trim(ordered_qty__raw) AS DECIMAL(18,2)) AS ordered_qty,
  try_cast(trim(unit_price__raw) AS DECIMAL(18,4)) AS unit_price,
  CASE WHEN upper(nullif(trim(currency__raw), '')) IN ('EUR', 'USD', 'GBP', 'CHF', 'CNY', 'INR', 'JPY', 'KRW', 'MXN', 'PLN', 'TWD', 'VND', 'SEK', 'NOK', 'DKK', 'CZK', 'HUF', 'RON', 'TRY', 'CAD', 'AUD', 'BRL', 'SGD', 'HKD', 'THB', 'MYR', 'IDR', 'ZAR', 'AED', 'SAR') THEN upper(nullif(trim(currency__raw), '')) END AS currency,
  upper(nullif(trim(delivery_location_id__raw), '')) AS delivery_location_id,
  upper(nullif(trim(line_status__raw), '')) AS line_status,
  try_to_timestamp(nullif(trim(last_modified_ts__raw), ''), "yyyy-MM-dd'T'HH:mm:ssXXX") AS last_modified_ts
FROM src;

-- purchase_orders_checked (internal): latest version of each PO line, FKs, FX rate, every rule hit
CREATE OR REFRESH TEMPORARY LIVE TABLE purchase_orders_checked
AS
WITH latest AS (
  -- data dictionary: latest last_modified_ts wins (older versions are superseded, not errors)
  SELECT * FROM (
    SELECT *, dense_rank() OVER (PARTITION BY po_id, po_line_number
                                 ORDER BY last_modified_ts DESC NULLS LAST) AS version_rank
    FROM LIVE.purchase_orders_typed)
  WHERE version_rank = 1
),
ranked AS (
  SELECT *,
    row_number() OVER (PARTITION BY po_id, po_line_number ORDER BY batch_id, source_line_number) AS rn,
    first_value(source_file_name) OVER w AS first_file,
    first_value(source_line_number) OVER w AS first_line,
    first_value(record_hash) OVER w AS first_hash
  FROM latest
  WINDOW w AS (PARTITION BY po_id, po_line_number ORDER BY batch_id, source_line_number
               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
),
deduped AS (
  -- the same record re-sent in a later file is an overlapping delivery: dropped without an issue
  SELECT * FROM ranked
  WHERE NOT (rn > 1 AND record_hash = first_hash AND source_file_name <> first_file)
),
fx AS (
  SELECT currency AS fx_currency, valid_from,
         lead(valid_from) OVER (PARTITION BY currency ORDER BY valid_from) AS valid_to, rate_to_eur
  FROM LIVE.silver_fx_rates
),
joined AS (
  SELECT d.*,
    fx.rate_to_eur,
    s.default_currency AS supplier_currency,
    s.supplier_id IS NOT NULL AS supplier_ok,
    p.product_id IS NOT NULL AS product_ok
  FROM deduped d
  LEFT JOIN fx ON d.currency = fx.fx_currency AND d.order_date >= fx.valid_from
              AND (fx.valid_to IS NULL OR d.order_date < fx.valid_to)
  LEFT JOIN LIVE.silver_suppliers s ON d.supplier_id = s.supplier_id
  LEFT JOIN LIVE.silver_products p ON d.product_id = p.product_id
)
SELECT *,
  filter(array(
    CASE WHEN load_status = 'MALFORMED' THEN named_struct('rule', 'malformed_record', 'action', 'QUARANTINE', 'column_name', '*', 'description', 'Row could not be parsed in Bronze') END,
    CASE WHEN load_status = 'SCHEMA_DRIFT' THEN named_struct('rule', 'schema_drift', 'action', 'FLAG', 'column_name', '_rescued_data', 'description', concat('Schema drift: ', _rescued_data)) END,
    CASE WHEN rn > 1 THEN named_struct('rule', 'duplicate_order_line', 'action', 'QUARANTINE', 'column_name', 'po_id,po_line_number', 'description', concat(CASE WHEN record_hash = first_hash THEN 'Exact duplicate of order line (' ELSE 'Duplicate order line (' END, po_id, ', ', CAST(po_line_number AS STRING), ') with same last_modified_ts - first occurrence on line ', CAST(first_line AS STRING))) END,
    CASE WHEN nullif(trim(po_id__raw), '') IS NULL THEN named_struct('rule', 'missing_required', 'action', 'QUARANTINE', 'column_name', 'po_id', 'description', 'po_id is empty') END,
    CASE WHEN nullif(trim(po_line_number__raw), '') IS NOT NULL AND po_line_number IS NULL THEN named_struct('rule', 'invalid_type', 'action', 'QUARANTINE', 'column_name', 'po_line_number', 'description', concat('po_line_number \'', po_line_number__raw, '\' is not a valid integer')) END,
    CASE WHEN nullif(trim(po_line_number__raw), '') IS NULL THEN named_struct('rule', 'missing_required', 'action', 'QUARANTINE', 'column_name', 'po_line_number', 'description', 'po_line_number is empty') END,
    CASE WHEN nullif(trim(order_date__raw), '') IS NOT NULL AND order_date IS NULL THEN named_struct('rule', 'invalid_type', 'action', 'QUARANTINE', 'column_name', 'order_date', 'description', concat('order_date \'', order_date__raw, '\' is not a valid date (yyyy-MM-dd)')) END,
    CASE WHEN nullif(trim(order_date__raw), '') IS NULL THEN named_struct('rule', 'missing_required', 'action', 'QUARANTINE', 'column_name', 'order_date', 'description', 'order_date is empty') END,
    CASE WHEN nullif(trim(promised_delivery_date__raw), '') IS NOT NULL AND promised_delivery_date IS NULL THEN named_struct('rule', 'invalid_type', 'action', 'QUARANTINE', 'column_name', 'promised_delivery_date', 'description', concat('promised_delivery_date \'', promised_delivery_date__raw, '\' is not a valid date (yyyy-MM-dd)')) END,
    CASE WHEN nullif(trim(ordered_qty__raw), '') IS NOT NULL AND ordered_qty IS NULL THEN named_struct('rule', 'invalid_type', 'action', 'QUARANTINE', 'column_name', 'ordered_qty', 'description', concat('ordered_qty \'', ordered_qty__raw, '\' is not a valid decimal')) END,
    CASE WHEN nullif(trim(ordered_qty__raw), '') IS NULL THEN named_struct('rule', 'missing_required', 'action', 'QUARANTINE', 'column_name', 'ordered_qty', 'description', 'ordered_qty is empty') END,
    CASE WHEN nullif(trim(unit_price__raw), '') IS NOT NULL AND unit_price IS NULL THEN named_struct('rule', 'invalid_type', 'action', 'QUARANTINE', 'column_name', 'unit_price', 'description', concat('unit_price \'', unit_price__raw, '\' is not a valid decimal')) END,
    CASE WHEN nullif(trim(last_modified_ts__raw), '') IS NOT NULL AND last_modified_ts IS NULL THEN named_struct('rule', 'invalid_type', 'action', 'FLAG', 'column_name', 'last_modified_ts', 'description', concat('last_modified_ts \'', last_modified_ts__raw, '\' is not a valid ISO-8601 timestamp')) END,
    CASE WHEN nullif(trim(currency__raw), '') IS NULL THEN named_struct('rule', 'missing_required', 'action', 'QUARANTINE', 'column_name', 'currency', 'description', 'currency is empty') END,
    CASE WHEN nullif(trim(currency__raw), '') IS NOT NULL AND upper(nullif(trim(currency__raw), '')) IN ('EUR', 'USD', 'GBP', 'CHF', 'CNY', 'INR', 'JPY', 'KRW', 'MXN', 'PLN', 'TWD', 'VND', 'SEK', 'NOK', 'DKK', 'CZK', 'HUF', 'RON', 'TRY', 'CAD', 'AUD', 'BRL', 'SGD', 'HKD', 'THB', 'MYR', 'IDR', 'ZAR', 'AED', 'SAR') AND nullif(trim(currency__raw), '') <> upper(nullif(trim(currency__raw), '')) THEN named_struct('rule', 'currency_inconsistency', 'action', 'FIX', 'column_name', 'currency', 'description', concat('currency \'', nullif(trim(currency__raw), ''), '\' normalised to \'', upper(nullif(trim(currency__raw), '')), '\'')) END,
    CASE WHEN nullif(trim(currency__raw), '') IS NOT NULL AND NOT upper(nullif(trim(currency__raw), '')) IN ('EUR', 'USD', 'GBP', 'CHF', 'CNY', 'INR', 'JPY', 'KRW', 'MXN', 'PLN', 'TWD', 'VND', 'SEK', 'NOK', 'DKK', 'CZK', 'HUF', 'RON', 'TRY', 'CAD', 'AUD', 'BRL', 'SGD', 'HKD', 'THB', 'MYR', 'IDR', 'ZAR', 'AED', 'SAR') THEN named_struct('rule', 'currency_inconsistency', 'action', 'QUARANTINE', 'column_name', 'currency', 'description', concat('currency \'', nullif(trim(currency__raw), ''), '\' is not a valid ISO 4217 code')) END,
    CASE WHEN currency IS NOT NULL AND supplier_currency IS NOT NULL AND currency <> supplier_currency THEN named_struct('rule', 'currency_inconsistency', 'action', 'FLAG', 'column_name', 'currency', 'description', concat('currency \'', currency, '\' differs from supplier default_currency ', supplier_currency)) END,
    CASE WHEN currency IS NOT NULL AND order_date IS NOT NULL AND rate_to_eur IS NULL THEN named_struct('rule', 'missing_fx_rate', 'action', 'QUARANTINE', 'column_name', 'currency', 'description', concat('No FX rate for ', currency, ' on ', CAST(order_date AS STRING))) END,
    CASE WHEN ordered_qty < 0 THEN named_struct('rule', 'negative_quantity', 'action', 'QUARANTINE', 'column_name', 'ordered_qty', 'description', concat('ordered_qty is negative (', ordered_qty__raw, ')')) END,
    CASE WHEN ordered_qty = 0 THEN named_struct('rule', 'invalid_value', 'action', 'QUARANTINE', 'column_name', 'ordered_qty', 'description', 'ordered_qty must be > 0 (data dictionary)') END,
    CASE WHEN unit_price < 0 THEN named_struct('rule', 'negative_price', 'action', 'QUARANTINE', 'column_name', 'unit_price', 'description', concat('unit_price is negative (', unit_price__raw, ')')) END,
    CASE WHEN promised_delivery_date < order_date THEN named_struct('rule', 'delivery_before_order', 'action', 'QUARANTINE', 'column_name', 'promised_delivery_date', 'description', concat('promised_delivery_date ', CAST(promised_delivery_date AS STRING), ' is before order_date ', CAST(order_date AS STRING))) END,
    CASE WHEN supplier_id IS NOT NULL AND NOT supplier_ok THEN named_struct('rule', 'unknown_supplier', 'action', 'QUARANTINE', 'column_name', 'supplier_id', 'description', concat('supplier_id ', supplier_id, ' does not exist in silver_suppliers')) END,
    CASE WHEN nullif(trim(supplier_id__raw), '') IS NULL THEN named_struct('rule', 'missing_required', 'action', 'QUARANTINE', 'column_name', 'supplier_id', 'description', 'supplier_id is empty') END,
    CASE WHEN product_id IS NOT NULL AND NOT product_ok THEN named_struct('rule', 'unknown_product', 'action', 'QUARANTINE', 'column_name', 'product_id', 'description', concat('product_id ', product_id, ' does not exist in silver_products')) END,
    CASE WHEN nullif(trim(product_id__raw), '') IS NULL THEN named_struct('rule', 'missing_required', 'action', 'QUARANTINE', 'column_name', 'product_id', 'description', 'product_id is empty') END,
    CASE WHEN line_status IS NOT NULL AND NOT line_status IN ('OPEN', 'PARTIALLY_RECEIVED', 'CLOSED', 'CANCELLED') THEN named_struct('rule', 'invalid_domain', 'action', 'FLAG', 'column_name', 'line_status', 'description', concat('line_status \'', line_status, '\' not in allowed values')) END
  ), x -> x IS NOT NULL) AS dq_issues
FROM joined;

-- silver_purchase_orders
CREATE OR REFRESH LIVE TABLE silver_purchase_orders
(
  CONSTRAINT key_present EXPECT (po_id IS NOT NULL AND po_line_number IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT ordered_qty_positive EXPECT (ordered_qty > 0) ON VIOLATION FAIL UPDATE,
  CONSTRAINT rate_present EXPECT (rate_to_eur IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'purchase_orders: one row per PO line, latest last_modified_ts wins, rate_to_eur on order_date'
TBLPROPERTIES ('quality' = 'silver')
AS
SELECT po_id, po_line_number, supplier_id, product_id, order_date, promised_delivery_date, ordered_qty, unit_price, currency, rate_to_eur, delivery_location_id, line_status, last_modified_ts,
       source_file_name, source_line_number, batch_id
FROM LIVE.purchase_orders_checked
WHERE NOT exists(dq_issues, x -> x.action = 'QUARANTINE');

-- shipment_events_checked (internal): one row per event; event-level rules, dedup, status sequence
CREATE OR REFRESH TEMPORARY LIVE TABLE shipment_events_checked
AS
WITH src AS (
  SELECT event_id AS event_id__raw, shipment_id AS shipment_id__raw, po_id AS po_id__raw, carrier AS carrier__raw, status AS status__raw, event_ts AS event_ts__raw, location AS location__raw, eta_ts AS eta_ts__raw, items AS items__raw, delay_reason_code AS delay_reason_code__raw,
         load_status, _rescued_data, raw_payload, record_hash, source_file_name, source_line_number, batch_id
  FROM LIVE.bronze_shipment_events
),
po_dates AS (
  SELECT po_id, MIN(order_date) AS po_order_date
  FROM LIVE.purchase_orders_typed WHERE po_id IS NOT NULL GROUP BY po_id
),
typed AS (
  SELECT s.*,
    upper(nullif(trim(event_id__raw), '')) AS event_id,
    upper(nullif(trim(shipment_id__raw), '')) AS shipment_id,
    upper(nullif(trim(po_id__raw), '')) AS po_id,
    nullif(trim(carrier__raw), '') AS carrier,
    upper(nullif(trim(status__raw), '')) AS status,
    try_to_timestamp(nullif(trim(event_ts__raw), ''), "yyyy-MM-dd'T'HH:mm:ssXXX") AS event_ts,
    try_to_timestamp(nullif(trim(eta_ts__raw), ''), "yyyy-MM-dd'T'HH:mm:ssXXX") AS eta_ts,
    upper(trim(location__raw.location_id)) AS location_id,
    trim(location__raw.name) AS location_name,
    upper(trim(location__raw.country)) AS location_country,
    upper(trim(location__raw.type)) AS location_type,
    upper(nullif(trim(delay_reason_code__raw), '')) AS delay_reason_code
  FROM src s
),
joined AS (
  SELECT t.*, d.po_id IS NOT NULL AS po_ok, d.po_order_date
  FROM typed t LEFT JOIN po_dates d ON t.po_id = d.po_id
),
event_rules AS (
  SELECT *,
    filter(array(
      CASE WHEN load_status = 'MALFORMED' THEN named_struct('rule', 'malformed_record', 'action', 'QUARANTINE', 'column_name', '*', 'description', 'Row could not be parsed in Bronze') END,
      CASE WHEN load_status = 'SCHEMA_DRIFT' THEN named_struct('rule', 'schema_drift', 'action', 'FLAG', 'column_name', '_rescued_data', 'description', concat('Schema drift: ', _rescued_data)) END,
      CASE WHEN nullif(trim(event_id__raw), '') IS NULL THEN named_struct('rule', 'missing_required', 'action', 'QUARANTINE', 'column_name', 'event_id', 'description', 'event_id is empty') END,
      CASE WHEN nullif(trim(shipment_id__raw), '') IS NULL THEN named_struct('rule', 'missing_required', 'action', 'QUARANTINE', 'column_name', 'shipment_id', 'description', 'shipment_id is empty') END,
      CASE WHEN nullif(trim(po_id__raw), '') IS NULL THEN named_struct('rule', 'missing_required', 'action', 'QUARANTINE', 'column_name', 'po_id', 'description', 'po_id is empty') END,
      CASE WHEN nullif(trim(status__raw), '') IS NULL THEN named_struct('rule', 'missing_required', 'action', 'QUARANTINE', 'column_name', 'status', 'description', 'status is empty') END,
      CASE WHEN nullif(trim(event_ts__raw), '') IS NULL THEN named_struct('rule', 'missing_required', 'action', 'QUARANTINE', 'column_name', 'event_ts', 'description', 'event_ts is empty') END,
      CASE WHEN nullif(trim(event_ts__raw), '') IS NOT NULL AND event_ts IS NULL THEN named_struct('rule', 'invalid_type', 'action', 'QUARANTINE', 'column_name', 'event_ts', 'description', concat('event_ts \'', event_ts__raw, '\' is not a valid ISO-8601 timestamp with offset')) END,
      CASE WHEN nullif(trim(eta_ts__raw), '') IS NOT NULL AND eta_ts IS NULL THEN named_struct('rule', 'invalid_type', 'action', 'NULLIFY', 'column_name', 'eta_ts', 'description', concat('eta_ts \'', eta_ts__raw, '\' is not a valid ISO-8601 timestamp with offset')) END,
      CASE WHEN status IS NOT NULL AND NOT status IN ('CREATED', 'PICKED_UP', 'IN_TRANSIT', 'CUSTOMS', 'DELAYED', 'OUT_FOR_DELIVERY', 'DELIVERED', 'CANCELLED') THEN named_struct('rule', 'invalid_domain', 'action', 'QUARANTINE', 'column_name', 'status', 'description', concat('status \'', status, '\' not in allowed values')) END,
      CASE WHEN location_type IS NOT NULL AND NOT location_type IN ('SUPPLIER_SITE', 'HUB', 'PORT', 'WAREHOUSE') THEN named_struct('rule', 'invalid_domain', 'action', 'FLAG', 'column_name', 'location.type', 'description', concat('location.type \'', location_type, '\' not in allowed values')) END,
      CASE WHEN delay_reason_code IS NOT NULL AND NOT delay_reason_code IN ('CUSTOMS_HOLD', 'WEATHER', 'CARRIER_CAPACITY', 'VESSEL_DELAY', 'PORT_CONGESTION', 'SUPPLIER_CAPACITY', 'MATERIAL_SHORTAGE', 'DOCUMENTATION') THEN named_struct('rule', 'invalid_domain', 'action', 'FLAG', 'column_name', 'delay_reason_code', 'description', concat('delay_reason_code \'', delay_reason_code, '\' not in allowed values')) END,
      CASE WHEN size(coalesce(items__raw, array())) = 0 THEN named_struct('rule', 'missing_required', 'action', 'QUARANTINE', 'column_name', 'items', 'description', 'event has no items') END,
      CASE WHEN po_id IS NOT NULL AND NOT po_ok THEN named_struct('rule', 'unknown_purchase_order', 'action', 'QUARANTINE', 'column_name', 'po_id', 'description', concat('po_id ', po_id, ' does not exist in purchase_orders')) END,
      CASE WHEN status = 'DELIVERED' AND event_ts < po_order_date THEN named_struct('rule', 'delivery_before_order', 'action', 'QUARANTINE', 'column_name', 'event_ts', 'description', concat('DELIVERED event_ts is before PO order_date ', CAST(po_order_date AS STRING))) END
    ), x -> x IS NOT NULL) AS dq_issues
  FROM joined
),
ranked AS (
  SELECT *,
    row_number() OVER (PARTITION BY event_id ORDER BY batch_id, source_line_number) AS rn,
    first_value(source_file_name) OVER w AS first_file,
    first_value(record_hash) OVER w AS first_hash
  FROM event_rules
  WINDOW w AS (PARTITION BY event_id ORDER BY batch_id, source_line_number
               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
),
deduped AS (
  -- identical event re-sent in another file: overlapping delivery, dropped without an issue
  SELECT *, filter(array(CASE WHEN rn > 1 THEN named_struct('rule', 'duplicate_event', 'action', 'QUARANTINE', 'column_name', 'event_id', 'description', concat(CASE WHEN record_hash = first_hash THEN 'Exact duplicate of event_id ' ELSE 'Conflicting duplicate of event_id ' END, event_id)) END), x -> x IS NOT NULL) AS dup_issues
  FROM ranked
  WHERE NOT (rn > 1 AND record_hash = first_hash AND source_file_name <> first_file)
),
sequence_1 AS (
  -- a DELIVERED event before the shipment's first PICKED_UP (events not already blocked)
  SELECT *, filter(array(CASE WHEN candidate AND status = 'DELIVERED' AND first_pickup IS NOT NULL AND event_ts < first_pickup THEN named_struct('rule', 'invalid_status_sequence', 'action', 'QUARANTINE', 'column_name', 'status', 'description', 'DELIVERED event occurs before PICKED_UP for the same shipment') END), x -> x IS NOT NULL) AS seq1_issues
  FROM (
    SELECT *, NOT exists(concat(dq_issues, dup_issues), x -> x.action = 'QUARANTINE') AS candidate,
      MIN(CASE WHEN NOT exists(concat(dq_issues, dup_issues), x -> x.action = 'QUARANTINE')
               AND status = 'PICKED_UP' THEN event_ts END) OVER (PARTITION BY shipment_id) AS first_pickup
    FROM deduped)
),
sequence_2 AS (
  -- any event after the shipment's first valid DELIVERED event
  SELECT *, filter(array(CASE WHEN candidate_2 AND event_ts > first_delivered THEN named_struct('rule', 'invalid_status_sequence', 'action', 'QUARANTINE', 'column_name', 'status', 'description', concat(status, ' event occurs after DELIVERED for the same shipment')) END), x -> x IS NOT NULL) AS seq2_issues
  FROM (
    SELECT *, NOT exists(concat(dq_issues, dup_issues, seq1_issues), x -> x.action = 'QUARANTINE') AS candidate_2,
      MIN(CASE WHEN NOT exists(concat(dq_issues, dup_issues, seq1_issues), x -> x.action = 'QUARANTINE')
               AND status = 'DELIVERED' THEN event_ts END) OVER (PARTITION BY shipment_id) AS first_delivered
    FROM sequence_1)
)
SELECT event_id, shipment_id, po_id, carrier, status, event_ts, location_id, location_name, location_country,
       location_type, eta_ts, delay_reason_code, items__raw, raw_payload, source_file_name, source_line_number, batch_id,
       concat(dq_issues, dup_issues, seq1_issues, seq2_issues) AS dq_issues
FROM sequence_2;

-- shipment_event_lines_checked (internal): items[] exploded to one row per event x PO line, line rules
CREATE OR REFRESH TEMPORARY LIVE TABLE shipment_event_lines_checked
AS
WITH lines AS (
  SELECT
    event_id, shipment_id, po_id, carrier, status, event_ts, location_id, location_name,
    location_country, location_type, eta_ts, delay_reason_code,
    exists(dq_issues, x -> x.action = 'QUARANTINE') AS event_blocked,
    raw_payload, source_file_name, source_line_number, batch_id,
    explode(items__raw) AS item
  FROM LIVE.shipment_events_checked
),
typed AS (
  SELECT
    event_id, shipment_id, po_id, carrier, status, event_ts, location_id, location_name,
    location_country, location_type, eta_ts, delay_reason_code, event_blocked, raw_payload, source_file_name, source_line_number, batch_id,
    item.po_line_number AS po_line_number__raw,
    item.product_id AS product_id__raw,
    item.quantity AS quantity__raw,
    try_cast(trim(item.po_line_number) AS INT) AS po_line_number,
    upper(nullif(trim(item.product_id), '')) AS product_id,
    try_cast(trim(item.quantity) AS DECIMAL(18,2)) AS quantity
  FROM lines
),
po_lines AS (
  SELECT DISTINCT concat_ws('#', po_id, po_line_number) AS line_key
  FROM LIVE.purchase_orders_typed WHERE po_id IS NOT NULL
),
joined AS (
  SELECT t.*,
    concat_ws('#', t.po_id, t.po_line_number) AS line_key,
    p.product_id IS NOT NULL AS product_ok,
    l.line_key IS NOT NULL AS line_ok
  FROM typed t
  LEFT JOIN LIVE.silver_products p ON t.product_id = p.product_id
  LEFT JOIN po_lines l ON concat_ws('#', t.po_id, t.po_line_number) = l.line_key
)
SELECT *,
  filter(array(
    CASE WHEN nullif(trim(po_line_number__raw), '') IS NULL THEN named_struct('rule', 'missing_required', 'action', 'QUARANTINE', 'column_name', 'po_line_number', 'description', 'po_line_number is empty') END,
    CASE WHEN nullif(trim(product_id__raw), '') IS NULL THEN named_struct('rule', 'missing_required', 'action', 'QUARANTINE', 'column_name', 'product_id', 'description', 'product_id is empty') END,
    CASE WHEN nullif(trim(quantity__raw), '') IS NULL THEN named_struct('rule', 'missing_required', 'action', 'QUARANTINE', 'column_name', 'quantity', 'description', 'quantity is empty') END,
    CASE WHEN nullif(trim(po_line_number__raw), '') IS NOT NULL AND po_line_number IS NULL THEN named_struct('rule', 'invalid_type', 'action', 'QUARANTINE', 'column_name', 'po_line_number', 'description', concat('po_line_number \'', po_line_number__raw, '\' is not a valid integer')) END,
    CASE WHEN nullif(trim(quantity__raw), '') IS NOT NULL AND quantity IS NULL THEN named_struct('rule', 'invalid_type', 'action', 'QUARANTINE', 'column_name', 'quantity', 'description', concat('quantity \'', quantity__raw, '\' is not a valid decimal')) END,
    CASE WHEN quantity < 0 THEN named_struct('rule', 'negative_quantity', 'action', 'QUARANTINE', 'column_name', 'items.quantity', 'description', concat('items.quantity is negative (', quantity__raw, ') for line ', CAST(po_line_number AS STRING))) END,
    CASE WHEN product_id IS NOT NULL AND NOT product_ok THEN named_struct('rule', 'unknown_product', 'action', 'QUARANTINE', 'column_name', 'items.product_id', 'description', concat('items.product_id ', product_id, ' does not exist in silver_products')) END,
    CASE WHEN po_line_number IS NOT NULL AND NOT line_ok THEN named_struct('rule', 'unknown_order_line', 'action', 'QUARANTINE', 'column_name', 'items.po_line_number', 'description', concat('PO line ', line_key, ' does not exist in purchase_orders')) END
  ), x -> x IS NOT NULL) AS dq_issues
FROM joined;

-- silver_shipment_events (data dictionary: one row per event x PO line)
CREATE OR REFRESH LIVE TABLE silver_shipment_events
(
  CONSTRAINT key_present EXPECT (event_id IS NOT NULL AND po_line_number IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT event_ts_present EXPECT (event_ts IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT quantity_not_negative EXPECT (quantity >= 0) ON VIOLATION FAIL UPDATE
)
COMMENT 'shipment_events flattened to one row per event x PO line; timestamps in UTC'
TBLPROPERTIES ('quality' = 'silver')
AS
SELECT event_id, shipment_id, po_id, carrier, status, event_ts, location_id, location_name, location_country, location_type, eta_ts, po_line_number, product_id, quantity, delay_reason_code,
       source_file_name, source_line_number, batch_id
FROM LIVE.shipment_event_lines_checked
WHERE NOT exists(dq_issues, x -> x.action = 'QUARANTINE') AND NOT event_blocked;

-- silver_quarantine: invalid records with rule, reason, raw record and processing timestamp (6.2)
CREATE OR REFRESH LIVE TABLE silver_quarantine
COMMENT 'Invalid records: one row per blocking rule, with reason, raw payload and processing timestamp'
TBLPROPERTIES ('quality' = 'silver')
AS
SELECT 'suppliers' AS entity, CAST(supplier_id AS STRING) AS record_key, i.rule AS rule, i.description AS reason,
       raw_payload, source_file_name, source_line_number, batch_id, current_timestamp() AS processing_ts
FROM LIVE.suppliers_checked
LATERAL VIEW explode(filter(dq_issues, x -> x.action = 'QUARANTINE')) q AS i
UNION ALL
SELECT 'products' AS entity, CAST(product_id AS STRING) AS record_key, i.rule AS rule, i.description AS reason,
       raw_payload, source_file_name, source_line_number, batch_id, current_timestamp() AS processing_ts
FROM LIVE.products_checked
LATERAL VIEW explode(filter(dq_issues, x -> x.action = 'QUARANTINE')) q AS i
UNION ALL
SELECT 'purchase_orders' AS entity, concat_ws('#', po_id, po_line_number) AS record_key, i.rule AS rule, i.description AS reason,
       raw_payload, source_file_name, source_line_number, batch_id, current_timestamp() AS processing_ts
FROM LIVE.purchase_orders_checked
LATERAL VIEW explode(filter(dq_issues, x -> x.action = 'QUARANTINE')) q AS i
UNION ALL
SELECT 'shipment_events' AS entity, CAST(event_id AS STRING) AS record_key, i.rule AS rule, i.description AS reason,
       raw_payload, source_file_name, source_line_number, batch_id, current_timestamp() AS processing_ts
FROM LIVE.shipment_events_checked
LATERAL VIEW explode(filter(dq_issues, x -> x.action = 'QUARANTINE')) q AS i
UNION ALL
SELECT 'shipment_events' AS entity, concat_ws('#', event_id, po_line_number) AS record_key, i.rule AS rule, i.description AS reason,
       raw_payload, source_file_name, source_line_number, batch_id, current_timestamp() AS processing_ts
FROM LIVE.shipment_event_lines_checked
LATERAL VIEW explode(filter(dq_issues, x -> x.action = 'QUARANTINE')) q AS i
WHERE NOT event_blocked;

-- ops_dq_issues: every rule hit, whatever the disposition (quality results measurable, 6.2)
CREATE OR REFRESH LIVE TABLE ops_dq_issues
COMMENT 'Every DQ rule hit (QUARANTINE, NULLIFY, FIX, FLAG) with file and line'
AS
SELECT 'suppliers' AS entity, CAST(supplier_id AS STRING) AS record_key, i.rule AS rule, i.action AS action,
       i.column_name AS column_name, i.description AS reason, source_file_name, source_line_number, batch_id, current_timestamp() AS processing_ts
FROM LIVE.suppliers_checked
LATERAL VIEW explode(dq_issues) q AS i
UNION ALL
SELECT 'products' AS entity, CAST(product_id AS STRING) AS record_key, i.rule AS rule, i.action AS action,
       i.column_name AS column_name, i.description AS reason, source_file_name, source_line_number, batch_id, current_timestamp() AS processing_ts
FROM LIVE.products_checked
LATERAL VIEW explode(dq_issues) q AS i
UNION ALL
SELECT 'purchase_orders' AS entity, concat_ws('#', po_id, po_line_number) AS record_key, i.rule AS rule, i.action AS action,
       i.column_name AS column_name, i.description AS reason, source_file_name, source_line_number, batch_id, current_timestamp() AS processing_ts
FROM LIVE.purchase_orders_checked
LATERAL VIEW explode(dq_issues) q AS i
UNION ALL
SELECT 'shipment_events' AS entity, CAST(event_id AS STRING) AS record_key, i.rule AS rule, i.action AS action,
       i.column_name AS column_name, i.description AS reason, source_file_name, source_line_number, batch_id, current_timestamp() AS processing_ts
FROM LIVE.shipment_events_checked
LATERAL VIEW explode(dq_issues) q AS i
UNION ALL
SELECT 'shipment_events' AS entity, concat_ws('#', event_id, po_line_number) AS record_key, i.rule AS rule, i.action AS action,
       i.column_name AS column_name, i.description AS reason, source_file_name, source_line_number, batch_id, current_timestamp() AS processing_ts
FROM LIVE.shipment_event_lines_checked
LATERAL VIEW explode(dq_issues) q AS i;


-- ----------------------------------------------------------------------------------------------------
-- Gold layer: star schema of the Team 4 assignment (3 facts, 6 dimensions), business brief section 4
-- Surrogate keys = xxhash64(natural key), -1 = Unknown member; date keys = yyyyMMdd, -1 = no date.
-- The 8 KPIs are DAX measures of the semantic model over these tables.
-- ----------------------------------------------------------------------------------------------------

-- po_line_kpis (internal): non-cancelled PO lines + business brief section 4 derived fields
CREATE OR REFRESH TEMPORARY LIVE TABLE po_line_kpis
AS
WITH po AS (
  SELECT * FROM LIVE.silver_purchase_orders WHERE line_status <> 'CANCELLED'   -- brief: cancelled lines excluded
),
shipped AS (
  SELECT
    e.po_id, e.po_line_number,
    SUM(CASE WHEN e.status = 'DELIVERED' THEN e.quantity END) AS received_qty,
    SUM(CASE WHEN e.status = 'DELIVERED' AND to_date(e.event_ts) <= po.promised_delivery_date
             THEN e.quantity END) AS received_by_promised_qty,
    MAX(CASE WHEN e.status = 'DELIVERED' THEN to_date(e.event_ts) END) AS actual_delivery_date,
    max_by(to_date(e.eta_ts), e.event_ts) AS latest_eta_date
  FROM LIVE.silver_shipment_events e
  JOIN po ON e.po_id = po.po_id AND e.po_line_number = po.po_line_number
  GROUP BY e.po_id, e.po_line_number
),
base AS (
  SELECT po.*,
    CAST(coalesce(s.received_qty, 0) AS DECIMAL(18,2)) AS received_qty,
    coalesce(s.received_by_promised_qty, 0) AS received_by_promised_qty,
    s.actual_delivery_date, s.latest_eta_date,
    po.promised_delivery_date <= COALESCE(try_cast('${gold.as_of_date}' AS DATE), current_date()) AS past_due,
    coalesce(s.latest_eta_date > po.promised_delivery_date, false) AS late_eta
  FROM po LEFT JOIN shipped s ON po.po_id = s.po_id AND po.po_line_number = s.po_line_number
),
delivered AS (
  SELECT *, received_qty >= ordered_qty OR line_status = 'CLOSED' AS is_delivered FROM base
),
delays AS (
  SELECT *,
    CASE WHEN is_delivered THEN datediff(actual_delivery_date, promised_delivery_date)
         WHEN past_due THEN datediff(COALESCE(try_cast('${gold.as_of_date}' AS DATE), current_date()), promised_delivery_date)
         ELSE 0 END AS delay_days
  FROM delivered
)
SELECT *,
  is_delivered AND coalesce(actual_delivery_date <= promised_delivery_date, false) AS is_on_time,
  (is_delivered AND coalesce(delay_days > 0, false))
    OR (NOT is_delivered AND (past_due OR late_eta)) AS is_delayed,
  CAST(CASE WHEN past_due THEN greatest(ordered_qty - received_by_promised_qty, 0)
            WHEN late_eta THEN greatest(ordered_qty - received_qty, 0)
            ELSE 0 END AS DECIMAL(18,2)) AS delayed_qty
FROM delays;

-- gold_dim_date
CREATE OR REFRESH LIVE TABLE gold_dim_date
(
  `date_key` INT COMMENT 'Surrogate key yyyyMMdd [Internal]',
  `date` DATE COMMENT 'Calendar date [Internal]',
  `year` INT COMMENT 'Year [Internal]',
  `quarter` INT COMMENT 'Quarter 1-4 [Internal]',
  `month` INT COMMENT 'Month 1-12 [Internal]',
  `month_name` STRING COMMENT 'Month name [Internal]',
  CONSTRAINT key_present EXPECT (date_key IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'Date dimension; date_key = yyyyMMdd, -1 = unknown'
TBLPROPERTIES ('quality' = 'gold')
AS
WITH used_dates AS (
  SELECT order_date AS d FROM LIVE.silver_purchase_orders
  UNION ALL SELECT promised_delivery_date FROM LIVE.silver_purchase_orders
  UNION ALL SELECT to_date(event_ts) FROM LIVE.silver_shipment_events
  UNION ALL SELECT to_date(eta_ts) FROM LIVE.silver_shipment_events
  UNION ALL SELECT COALESCE(try_cast('${gold.as_of_date}' AS DATE), current_date())
),
calendar AS (
  SELECT explode(sequence(make_date(year(MIN(d)), 1, 1), make_date(year(MAX(d)), 12, 31))) AS `date`
  FROM used_dates
)
SELECT coalesce(CAST(date_format(`date`, 'yyyyMMdd') AS INT), -1) AS date_key, `date`, year(`date`) AS year, quarter(`date`) AS quarter,
       month(`date`) AS month, date_format(`date`, 'MMMM') AS month_name
FROM calendar
UNION ALL
SELECT -1, NULL, NULL, NULL, NULL, 'Unknown';

-- gold_dim_supplier
CREATE OR REFRESH LIVE TABLE gold_dim_supplier
(
  `supplier_key` BIGINT COMMENT 'Surrogate key -> gold_dim_supplier (-1 = unknown) [Internal]',
  `supplier_id` STRING COMMENT 'Unique supplier identifier [Internal]',
  `supplier_name` STRING COMMENT 'Legal or trading name of the supplier [Internal]',
  `country_code` STRING COMMENT 'ISO 3166-1 alpha-2 country of the supplier [Public]',
  `region` STRING COMMENT 'EMEA, APAC, AMER [Public]',
  `default_currency` STRING COMMENT 'ISO 4217 currency used by the supplier [Internal]',
  `contract_lead_time_days` INT COMMENT 'Contractually agreed lead time in days [Confidential]',
  `risk_rating` STRING COMMENT 'LOW, MEDIUM, HIGH [Confidential]',
  `is_active` BOOLEAN COMMENT 'Whether the supplier is currently approved for ordering [Internal]',
  `onboarded_date` DATE COMMENT 'Date the supplier was approved [Internal]',
  CONSTRAINT key_present EXPECT (supplier_key IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'Suppliers (current snapshot)'
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT CASE WHEN supplier_id IS NULL THEN CAST(-1 AS BIGINT) ELSE xxhash64(concat_ws('||', CAST(supplier_id AS STRING))) END AS supplier_key,
       supplier_id, supplier_name, country_code, region, default_currency, contract_lead_time_days,
       risk_rating, is_active, onboarded_date
FROM LIVE.silver_suppliers
UNION ALL
SELECT -1, 'UNKNOWN', 'Unknown supplier', NULL, NULL, NULL, NULL, NULL, NULL, NULL;

-- gold_dim_product
CREATE OR REFRESH LIVE TABLE gold_dim_product
(
  `product_key` BIGINT COMMENT 'Surrogate key -> gold_dim_product (-1 = unknown) [Internal]',
  `product_id` STRING COMMENT 'Unique product identifier [Internal]',
  `product_name` STRING COMMENT 'Product description [Public]',
  `category` STRING COMMENT 'Product category [Public]',
  `unit_of_measure` STRING COMMENT 'EA, KG, L [Public]',
  `criticality` STRING COMMENT 'CRITICAL, HIGH, MEDIUM, LOW [Internal]',
  `primary_supplier_id` STRING COMMENT 'Main supplier for the product [Internal]',
  `alternate_supplier_id` STRING COMMENT 'Approved backup supplier [Internal]',
  `stocking_location_id` STRING COMMENT 'Primary warehouse holding the product [Internal]',
  `has_active_alternate` BOOLEAN COMMENT 'Brief: alternate_supplier_id not null AND <> primary_supplier_id AND alternate supplier is_active [Internal]',
  CONSTRAINT key_present EXPECT (product_key IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'Products (current snapshot)'
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT CASE WHEN p.product_id IS NULL THEN CAST(-1 AS BIGINT) ELSE xxhash64(concat_ws('||', CAST(p.product_id AS STRING))) END AS product_key,
       p.product_id, p.product_name, p.category, p.unit_of_measure, p.criticality,
       p.primary_supplier_id, p.alternate_supplier_id, p.stocking_location_id,
       -- brief: alternate not null AND <> primary AND alternate supplier is_active
       p.alternate_supplier_id IS NOT NULL AND p.alternate_supplier_id <> p.primary_supplier_id
         AND coalesce(a.is_active, false) AS has_active_alternate
FROM LIVE.silver_products p
LEFT JOIN LIVE.silver_suppliers a ON p.alternate_supplier_id = a.supplier_id
UNION ALL
SELECT -1, 'UNKNOWN', 'Unknown product', NULL, NULL, NULL, NULL, NULL, NULL, NULL;

-- gold_dim_location
CREATE OR REFRESH LIVE TABLE gold_dim_location
(
  `location_key` BIGINT COMMENT 'Surrogate key -> gold_dim_location (-1 = unknown) [Internal]',
  `location_id` STRING COMMENT 'Location identifier [Internal]',
  `location_name` STRING COMMENT 'Location name [Public]',
  `location_country` STRING COMMENT 'ISO 3166-1 alpha-2 country of the location [Public]',
  `location_type` STRING COMMENT 'WAREHOUSE, PORT, HUB, SUPPLIER_SITE [Public]',
  CONSTRAINT key_present EXPECT (location_key IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'Locations of shipment events and warehouses referenced by PO lines / products'
TBLPROPERTIES ('quality' = 'gold')
AS
WITH seen AS (
  -- attributes of the most recent event per location
  SELECT location_id, location_name, location_country, location_type
  FROM (SELECT *, row_number() OVER (PARTITION BY location_id ORDER BY event_ts DESC) AS rn
        FROM LIVE.silver_shipment_events WHERE location_id IS NOT NULL)
  WHERE rn = 1
),
referenced AS (
  -- warehouses known only from PO lines or products (no attributes)
  SELECT DISTINCT location_id FROM (
    SELECT delivery_location_id AS location_id FROM LIVE.silver_purchase_orders
    UNION ALL SELECT stocking_location_id FROM LIVE.silver_products)
  WHERE location_id IS NOT NULL AND location_id NOT IN (SELECT location_id FROM seen)
),
locations AS (
  SELECT * FROM seen
  UNION ALL SELECT location_id, NULL, NULL, NULL FROM referenced
)
SELECT CASE WHEN location_id IS NULL THEN CAST(-1 AS BIGINT) ELSE xxhash64(concat_ws('||', CAST(location_id AS STRING))) END AS location_key, location_id, location_name, location_country, location_type
FROM locations
UNION ALL
SELECT -1, 'UNKNOWN', 'Unknown location', NULL, NULL;

-- gold_dim_shipment_status
CREATE OR REFRESH LIVE TABLE gold_dim_shipment_status
(
  `shipment_status_key` BIGINT COMMENT 'Surrogate key -> gold_dim_shipment_status (-1 = unknown) [Internal]',
  `status` STRING COMMENT 'Shipment status [Internal]',
  CONSTRAINT key_present EXPECT (shipment_status_key IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'Shipment statuses (data dictionary)'
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT CASE WHEN status IS NULL THEN CAST(-1 AS BIGINT) ELSE xxhash64(concat_ws('||', CAST(status AS STRING))) END AS shipment_status_key, status
FROM VALUES ('CREATED'), ('PICKED_UP'), ('IN_TRANSIT'), ('CUSTOMS'), ('OUT_FOR_DELIVERY'), ('DELIVERED'), ('DELAYED'), ('CANCELLED') AS v(status)
UNION ALL
SELECT -1, 'UNKNOWN';

-- gold_dim_risk_level
CREATE OR REFRESH LIVE TABLE gold_dim_risk_level
(
  `risk_level_key` BIGINT COMMENT 'Supplier risk_rating -> gold_dim_risk_level (-1 = unknown) [Internal]',
  `risk_level` STRING COMMENT 'LOW, MEDIUM, HIGH [Internal]',
  CONSTRAINT key_present EXPECT (risk_level_key IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'Supplier risk levels (data dictionary risk_rating)'
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT CASE WHEN risk_level IS NULL THEN CAST(-1 AS BIGINT) ELSE xxhash64(concat_ws('||', CAST(risk_level AS STRING))) END AS risk_level_key, risk_level
FROM VALUES ('LOW'), ('MEDIUM'), ('HIGH') AS v(risk_level)
UNION ALL
SELECT -1, 'UNKNOWN';

-- gold_fact_purchase_order_line (grain: one PO line; brief KPIs Order Value, On-Time Delivery Rate,
-- Average Delay, Supplier Fill Rate, Delayed Quantity)
CREATE OR REFRESH LIVE TABLE gold_fact_purchase_order_line
(
  `po_id` STRING COMMENT 'Purchase order identifier [Internal]',
  `po_line_number` INT COMMENT 'Line number within the purchase order [Internal]',
  `supplier_key` BIGINT COMMENT 'Surrogate key -> gold_dim_supplier (-1 = unknown) [Internal]',
  `product_key` BIGINT COMMENT 'Surrogate key -> gold_dim_product (-1 = unknown) [Internal]',
  `location_key` BIGINT COMMENT 'Destination warehouse (delivery_location_id) -> gold_dim_location [Internal]',
  `risk_level_key` BIGINT COMMENT 'Supplier risk_rating -> gold_dim_risk_level (-1 = unknown) [Internal]',
  `order_date_key` INT COMMENT 'order_date -> gold_dim_date [Internal]',
  `promised_delivery_date_key` INT COMMENT 'promised_delivery_date -> gold_dim_date [Internal]',
  `actual_delivery_date_key` INT COMMENT 'actual_delivery_date -> gold_dim_date (-1 = not delivered) [Internal]',
  `latest_eta_date_key` INT COMMENT 'latest_eta_date -> gold_dim_date (-1 = no shipment event) [Internal]',
  `line_status` STRING COMMENT 'OPEN, PARTIALLY_RECEIVED, CLOSED [Internal]',
  `ordered_qty` DECIMAL(18,2) COMMENT 'Quantity ordered [Internal]',
  `unit_price` DECIMAL(18,4) COMMENT 'Agreed price per unit (line currency) [Confidential]',
  `rate_to_eur` DECIMAL(18,8) COMMENT 'FX rate to EUR valid on order_date (fx_rates) [Internal]',
  `received_qty` DECIMAL(18,2) COMMENT 'Brief: SUM(items[].quantity) from DELIVERED events for the PO line [Internal]',
  `delay_days` INT COMMENT 'Brief: delivered -> actual - promised; open and past due -> snapshot - promised; else 0 [Internal]',
  `delayed_qty` DECIMAL(18,2) COMMENT 'Brief: past due -> ordered - qty received by promised date; not yet due with late ETA -> ordered - received; else 0 [Internal]',
  `is_delivered` BOOLEAN COMMENT 'Brief: received_qty >= ordered_qty OR line_status = \'CLOSED\' [Internal]',
  `is_on_time` BOOLEAN COMMENT 'Brief: is_delivered AND actual_delivery_date <= promised_delivery_date [Internal]',
  `is_delayed` BOOLEAN COMMENT 'Brief: (delivered AND delay_days > 0) OR (not delivered AND (past due OR latest ETA after promised)) [Internal]',
  CONSTRAINT keys_present EXPECT (supplier_key IS NOT NULL AND product_key IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT not_cancelled EXPECT (line_status <> 'CANCELLED') ON VIOLATION FAIL UPDATE,
  CONSTRAINT delayed_qty_not_negative EXPECT (delayed_qty >= 0) ON VIOLATION FAIL UPDATE,
  CONSTRAINT on_time_implies_delivered EXPECT (NOT is_on_time OR is_delivered) ON VIOLATION FAIL UPDATE
)
CLUSTER BY (order_date_key, supplier_key)
COMMENT 'Grain: one purchase-order line (current version, cancelled lines excluded). KPIs: Order Value, On-Time Delivery Rate, Average Delay, Supplier Fill Rate, Delayed Quantity'
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT
  k.po_id, k.po_line_number,
  CASE WHEN k.supplier_id IS NULL THEN CAST(-1 AS BIGINT) ELSE xxhash64(concat_ws('||', CAST(k.supplier_id AS STRING))) END AS supplier_key,
  CASE WHEN k.product_id IS NULL THEN CAST(-1 AS BIGINT) ELSE xxhash64(concat_ws('||', CAST(k.product_id AS STRING))) END AS product_key,
  CASE WHEN k.delivery_location_id IS NULL THEN CAST(-1 AS BIGINT) ELSE xxhash64(concat_ws('||', CAST(k.delivery_location_id AS STRING))) END AS location_key,
  CASE WHEN s.risk_rating IS NULL THEN CAST(-1 AS BIGINT) ELSE xxhash64(concat_ws('||', CAST(s.risk_rating AS STRING))) END AS risk_level_key,
  coalesce(CAST(date_format(k.order_date, 'yyyyMMdd') AS INT), -1) AS order_date_key,
  coalesce(CAST(date_format(k.promised_delivery_date, 'yyyyMMdd') AS INT), -1) AS promised_delivery_date_key,
  coalesce(CAST(date_format(k.actual_delivery_date, 'yyyyMMdd') AS INT), -1) AS actual_delivery_date_key,
  coalesce(CAST(date_format(k.latest_eta_date, 'yyyyMMdd') AS INT), -1) AS latest_eta_date_key,
  k.line_status, k.ordered_qty, k.unit_price, k.rate_to_eur,
  k.received_qty, k.delay_days, k.delayed_qty, k.is_delivered, k.is_on_time, k.is_delayed
FROM LIVE.po_line_kpis k
LEFT JOIN LIVE.silver_suppliers s ON k.supplier_id = s.supplier_id;

-- gold_fact_shipment_event (grain: one shipment status event)
CREATE OR REFRESH LIVE TABLE gold_fact_shipment_event
(
  `event_id` STRING COMMENT 'Unique event identifier [Internal]',
  `shipment_id` STRING COMMENT 'Shipment the event belongs to [Internal]',
  `po_id` STRING COMMENT 'Purchase order being shipped [Internal]',
  `shipment_status_key` BIGINT COMMENT 'Surrogate key -> gold_dim_shipment_status (-1 = unknown) [Internal]',
  `supplier_key` BIGINT COMMENT 'Supplier of the purchase order -> gold_dim_supplier [Internal]',
  `location_key` BIGINT COMMENT 'Event location -> gold_dim_location [Internal]',
  `event_date_key` INT COMMENT 'Event date (UTC) -> gold_dim_date [Internal]',
  `carrier` STRING COMMENT 'Logistics carrier name [Internal]',
  `delay_reason_code` STRING COMMENT 'Reason for delay (nullable) [Internal]',
  `event_ts` TIMESTAMP COMMENT 'Time the status occurred (UTC) [Internal]',
  `eta_ts` TIMESTAMP COMMENT 'Carrier\'s estimated arrival time (UTC) [Internal]',
  CONSTRAINT keys_present EXPECT (event_id IS NOT NULL AND shipment_status_key IS NOT NULL) ON VIOLATION FAIL UPDATE
)
CLUSTER BY (event_date_key, supplier_key)
COMMENT 'Grain: one shipment status event'
TBLPROPERTIES ('quality' = 'gold')
AS
WITH events AS (
  -- Silver grain is event x PO line; the event-level columns are identical on all lines of an event
  SELECT DISTINCT event_id, shipment_id, po_id, carrier, status, event_ts, eta_ts, location_id, delay_reason_code
  FROM LIVE.silver_shipment_events
),
po_supplier AS (
  SELECT po_id, MIN(supplier_id) AS supplier_id FROM LIVE.silver_purchase_orders GROUP BY po_id
)
SELECT
  e.event_id, e.shipment_id, e.po_id,
  CASE WHEN e.status IS NULL THEN CAST(-1 AS BIGINT) ELSE xxhash64(concat_ws('||', CAST(e.status AS STRING))) END AS shipment_status_key,
  CASE WHEN p.supplier_id IS NULL THEN CAST(-1 AS BIGINT) ELSE xxhash64(concat_ws('||', CAST(p.supplier_id AS STRING))) END AS supplier_key,
  CASE WHEN e.location_id IS NULL THEN CAST(-1 AS BIGINT) ELSE xxhash64(concat_ws('||', CAST(e.location_id AS STRING))) END AS location_key,
  coalesce(CAST(date_format(to_date(e.event_ts), 'yyyyMMdd') AS INT), -1) AS event_date_key,
  e.carrier, e.delay_reason_code, e.event_ts, e.eta_ts
FROM events e
LEFT JOIN po_supplier p ON e.po_id = p.po_id;

-- gold_fact_inventory_exposure (grain: one product at snapshot_date; Exposed Value, Critical-Item Count)
CREATE OR REFRESH LIVE TABLE gold_fact_inventory_exposure
(
  `snapshot_date_key` INT COMMENT 'snapshot_date (as-of date of the refresh) -> gold_dim_date [Internal]',
  `product_key` BIGINT COMMENT 'Surrogate key -> gold_dim_product (-1 = unknown) [Internal]',
  `supplier_key` BIGINT COMMENT 'Primary supplier -> gold_dim_supplier [Internal]',
  `location_key` BIGINT COMMENT 'Stocking warehouse -> gold_dim_location [Internal]',
  `risk_level_key` BIGINT COMMENT 'Supplier risk_rating -> gold_dim_risk_level (-1 = unknown) [Internal]',
  `on_hand_qty` DECIMAL(18,2) COMMENT 'Stock on hand at extract time [Internal]',
  `safety_stock_qty` DECIMAL(18,2) COMMENT 'Minimum stock to be held as buffer [Internal]',
  `avg_daily_demand` DECIMAL(18,2) COMMENT 'Average units consumed per day [Confidential]',
  `unit_cost` DECIMAL(18,4) COMMENT 'Standard cost per unit [Confidential]',
  `rate_to_eur` DECIMAL(18,8) COMMENT 'FX rate to EUR of cost_currency valid on snapshot_date (fx_rates) [Internal]',
  `days_of_cover` DOUBLE COMMENT 'Brief: MAX(0, on_hand_qty - safety_stock_qty) / avg_daily_demand [Internal]',
  `delayed_inbound_qty` DECIMAL(18,2) COMMENT 'Brief: SUM(delayed_qty) of open delayed lines for the product [Internal]',
  `days_until_arrival` INT COMMENT 'Brief: latest_eta_date of earliest delayed open line - snapshot_date; if no ETA, delay_days + contract_lead_time_days [Internal]',
  CONSTRAINT key_present EXPECT (product_key IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT cover_not_negative EXPECT (days_of_cover IS NULL OR days_of_cover >= 0) ON VIOLATION FAIL UPDATE
)
COMMENT 'Grain: one product at snapshot_date. KPIs: Exposed Value, Critical-Item Count'
TBLPROPERTIES ('quality' = 'gold')
AS
WITH open_delayed AS (
  SELECT * FROM LIVE.po_line_kpis WHERE NOT is_delivered AND is_delayed
),
inbound AS (
  -- brief: delayed_inbound_qty = SUM(delayed_qty) of open delayed lines for the product
  SELECT product_id, SUM(delayed_qty) AS delayed_inbound_qty FROM open_delayed GROUP BY product_id
),
earliest AS (
  -- earliest delayed open line = earliest promised_delivery_date (flagged assumption)
  SELECT o.product_id, o.latest_eta_date AS eta_date, o.delay_days, s.contract_lead_time_days
  FROM (SELECT *, row_number() OVER (PARTITION BY product_id
                                     ORDER BY promised_delivery_date, po_id, po_line_number) AS rn
        FROM open_delayed) o
  LEFT JOIN LIVE.silver_suppliers s ON o.supplier_id = s.supplier_id
  WHERE o.rn = 1
),
fx AS (
  SELECT currency, valid_from, lead(valid_from) OVER (PARTITION BY currency ORDER BY valid_from) AS valid_to,
         rate_to_eur
  FROM LIVE.silver_fx_rates
)
SELECT
  coalesce(CAST(date_format(COALESCE(try_cast('${gold.as_of_date}' AS DATE), current_date()), 'yyyyMMdd') AS INT), -1) AS snapshot_date_key,
  CASE WHEN p.product_id IS NULL THEN CAST(-1 AS BIGINT) ELSE xxhash64(concat_ws('||', CAST(p.product_id AS STRING))) END AS product_key,
  CASE WHEN p.primary_supplier_id IS NULL THEN CAST(-1 AS BIGINT) ELSE xxhash64(concat_ws('||', CAST(p.primary_supplier_id AS STRING))) END AS supplier_key,
  CASE WHEN p.stocking_location_id IS NULL THEN CAST(-1 AS BIGINT) ELSE xxhash64(concat_ws('||', CAST(p.stocking_location_id AS STRING))) END AS location_key,
  CASE WHEN r.risk_rating IS NULL THEN CAST(-1 AS BIGINT) ELSE xxhash64(concat_ws('||', CAST(r.risk_rating AS STRING))) END AS risk_level_key,
  p.on_hand_qty, p.safety_stock_qty, p.avg_daily_demand, p.unit_cost,
  fx.rate_to_eur,                                        -- rate of cost_currency on snapshot_date (assumption)
  -- brief: MAX(0, on_hand - safety_stock) / avg_daily_demand (unknown when a stock figure is NULL)
  CAST(CASE WHEN p.avg_daily_demand > 0 AND p.on_hand_qty IS NOT NULL AND p.safety_stock_qty IS NOT NULL
            THEN round(greatest(p.on_hand_qty - p.safety_stock_qty, 0) / p.avg_daily_demand, 2) END
       AS DOUBLE) AS days_of_cover,
  CAST(coalesce(i.delayed_inbound_qty, 0) AS DECIMAL(18,2)) AS delayed_inbound_qty,
  -- brief: ETA of earliest delayed open line - snapshot_date; no ETA -> delay_days + contract lead time
  CASE WHEN e.product_id IS NOT NULL
       THEN coalesce(datediff(e.eta_date, COALESCE(try_cast('${gold.as_of_date}' AS DATE), current_date())), e.delay_days + e.contract_lead_time_days) END
    AS days_until_arrival
FROM LIVE.silver_products p
LEFT JOIN inbound i ON p.product_id = i.product_id
LEFT JOIN earliest e ON p.product_id = e.product_id
LEFT JOIN LIVE.silver_suppliers r ON p.primary_supplier_id = r.supplier_id
LEFT JOIN fx ON p.cost_currency = fx.currency AND COALESCE(try_cast('${gold.as_of_date}' AS DATE), current_date()) >= fx.valid_from
            AND (fx.valid_to IS NULL OR COALESCE(try_cast('${gold.as_of_date}' AS DATE), current_date()) < fx.valid_to);
