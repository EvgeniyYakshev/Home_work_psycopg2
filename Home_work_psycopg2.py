import psycopg2
from psycopg2.sql import SQL, Identifier

# 1 Функция, создающая структуру БД (таблицы).
# 		имя,
#  		фамилия,
#  		email,
#  		телефон.

def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
	        DROP TABLE IF EXISTS phone_book;
	        DROP TABLE IF EXISTS client_info;
	        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS client_info(
            client_id SERIAL PRIMARY KEY,
	    first_name VARCHAR(40) NOT NULL,
	    last_name VARCHAR(60) NOT NULL,
	    email VARCHAR(100) NOT NULL UNIQUE 
	        );
            """)
        #conn.commit()  #фиксируем в БД

        cur.execute("""
        CREATE TABLE IF NOT EXISTS phone_book(
            id SERIAL PRIMARY KEY, 
            client_id INTEGER NOT NULL REFERENCES client_info(client_id),
	    phone VARCHAR(12)
            );
            """)
        #conn.commit()  # фиксируем в БД

# 2 Функция, позволяющая добавить нового клиента.

def add_client(conn, first_name: str, last_name: str, email, phones=None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO client_info(first_name, last_name, email)
            VALUES (%s, %s, %s)
            RETURNING client_id, first_name, last_name, email;
            """, (first_name, last_name, email))
        return cur.fetchone()

#3 Функция, позволяющая добавить телефон для существующего клиента.

def add_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO phone_book(client_id, phone)
        VALUES(%s, %s)
        RETURNING client_id, phone;
        """, (client_id, phone))
        return cur.fetchone()

# 4 Функция, позволяющая изменить данные о клиенте.

def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    with conn.cursor() as cur:

        arg_list = {'first_name': first_name, 'last_name': last_name, 'email': email}
        for key, arg in arg_list.items():
            if arg:
                cur.execute(SQL('UPDATE client_info SET {}=%s WHERE client_id = %s').format(Identifier(key)),
                            (arg, client_id))
        cur.execute("""
            SELECT * FROM client_info
            WHERE client_id = %s;
            """, client_id)
        return cur.fetchall()

#5 Функция, позволяющая удалить телефон для существующего клиента.

def delete_phone(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM phone_book
            WHERE client_id=%s;
            """, (client_id,))
        return cur.fetchone()

# 6 Функция, позволяющая удалить существующего клиента.

def delete_client(conn, client_id):
    delete_phone(conn, client_id)
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM client_info
            WHERE client_id = %s;
            """, (client_id,))
        return cur.fetchone()

# 7 Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону.

def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT * FROM client_info c
        LEFT JOIN phone_book p ON c.client_id = p.client_id
        WHERE (first_name = %(first_name)s OR %(first_name)s IS NULL)
        AND (last_name = %(last_name)s OR %(last_name)s IS NULL)
        AND (email = %(email)s OR %(email)s IS NULL)
        OR (phone = %(phone)s OR %(phone)s IS NULL);
        """, {'first_name': first_name, 'last_name': last_name, 'email': email, 'phone': phone})
        return cur.fetchone()

if __name__ == '__main__':
    with psycopg2.connect(database='clients_db_sql', user="postgres", password="1837099") as conn:
        create_db(conn)
        print(add_client(conn, 'John', 'Python', 'super_user@yandex.ru'))
        print(add_phone(conn, '1', '8999926477'))
        #print(change_client(conn, '1', 'Tom', 'Raider'))
        #print(delete_phone(conn, '1'))
        #print(delete_client(conn, 1))
        print(find_client(conn, 'John'))
conn.close()
