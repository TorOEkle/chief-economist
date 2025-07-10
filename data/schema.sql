CREATE TABLE  documents (
    id INTEGER PRIMARY KEY,
    title TEXT,
    source_url TEXT,
    publication_date DATE,
    file_path TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE build_index (
    input_factor TEXT,
    month DATE,
    cost_index DOUBLE,
    delta_month_pct DOUBLE,
    delta_year_pct DOUBLE
);