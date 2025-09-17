CREATE TABLE roster_raw (
  id NUMBER PRIMARY KEY,
  payload CLOB,
  source VARCHAR2(200),
  created_at TIMESTAMP DEFAULT SYSTIMESTAMP,
  export_status VARCHAR2(20),
  export_at TIMESTAMP
);
