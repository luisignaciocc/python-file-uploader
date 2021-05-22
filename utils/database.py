
def search_by_name(cursor, table, value):

    cursor.execute(f"SELECT id from {table} where name = {value}")
    result = cursor.fetchone()
    if result is not None:
        return result['id']
    else:
        return 0
