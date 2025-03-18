import duckdb
import os
import tqdm

conn = duckdb.connect('transport_data.duckdb')

# init table
table_creation = """
create table if not exists opal_raw (
    trip_origin_date date,
    mode_name text,
    ti_region text,
    tap_hour bigint,
    Tap_Ons text,
    Tap_Offs text
)
;
"""
conn.execute(table_creation)

# load data
for folder in tqdm.tqdm(os.listdir('data')):
    query = f"""
    insert into opal_raw
        select * from read_csv(
        '{os.path.join(os.getcwd(),'data',folder,'*.txt')}',
        delim='|')
    ;
    """
    conn.execute(query)

conn.close()
