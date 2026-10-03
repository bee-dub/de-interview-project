import psycopg2

conn = psycopg2.connect(
    host="postgres",
    database="warehouse",
    user="de_user",
    password="de_password"
)

cur = conn.cursor()

batch_size = 10

while True:

    # Read checkpoint
    cur.execute("""
        SELECT last_successful_batch
        FROM analytics.pipeline_checkpoints
        WHERE job_name = 'raw_orders_to_staging'
    """)

    last_successful_batch = cur.fetchone()[0]

    next_batch = last_successful_batch + 1
    start_id = (next_batch - 1) * batch_size + 1
    end_id = next_batch * batch_size

    # Read next batch
    cur.execute("""
        SELECT order_id, customer_id, order_date, amount, updated_at
        FROM raw.orders
        WHERE order_id BETWEEN %s AND %s
        ORDER BY order_id
    """, (start_id, end_id))

    orders = cur.fetchall()

    print("Last successful batch:", last_successful_batch)
    print("Processing batch:", next_batch)
    print("Order IDs:", start_id, "to", end_id)
    print("Rows found:", len(orders))

    # No more data
    if not orders:
        print("No more batches to process")
        break

    # Insert batch
    for order in orders:
        cur.execute("""
            INSERT INTO analytics.batch_stg_orders
                (order_id, customer_id, order_date, amount, updated_at)
            VALUES (%s, %s, %s, %s, %s)
        """, order)

    conn.commit()


    # Update checkpoint
    cur.execute("""
        UPDATE analytics.pipeline_checkpoints
        SET last_successful_batch = %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE job_name = 'raw_orders_to_staging'
    """, (next_batch,))

    conn.commit()


    print(f"Batch {next_batch} committed successfully")

cur.close()
conn.close()