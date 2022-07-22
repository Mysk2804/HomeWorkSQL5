import psycopg2
import configparser

config = configparser.ConfigParser()
config.read("parol.ini")
# print(config["parol"]["parol"])

def new_table(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE contact_phone;"
                    "DROP TABLE contact_man;"
                    "DROP TABLE number_phone;")

        cur.execute("CREATE TABLE IF NOT EXISTS contact_man(id SERIAL PRIMARY KEY, "
                    "last_name VARCHAR(80) NOT NULL, "
                    "first_name VARCHAR(80) NOT NULL, "
                    "email VARCHAR(300) NOT NULL);")

        cur.execute("CREATE TABLE IF NOT EXISTS number_phone(id SERIAL PRIMARY KEY,"
                    "number INTEGER NOT NULL);")

        cur.execute("CREATE TABLE IF NOT EXISTS contact_phone(man_id INTEGER REFERENCES contact_man(id),"
                    "phone_id INTEGER REFERENCES number_phone(id),"
                    "CONSTRAINT pk PRIMARY KEY (man_id, phone_id));")
        conn.commit()


def new_client(conn, last_name, first_name, email, number=None):
    with conn.cursor() as cur:
        cur.execute(f"INSERT INTO contact_man(last_name, first_name, email) VALUES('{last_name}', '{first_name}', '{email}') RETURNING id;")
        man_id = cur.fetchone()[0]
        if number != None:
            cur.execute(f"INSERT INTO number_phone(number) VALUES('{number}') RETURNING id;")
            number_id = cur.fetchone()[0]
            cur.execute(f"INSERT INTO contact_phone(man_id, phone_id) VALUES('{man_id}', '{number_id}' );")
    conn.commit()

def add_phone(conn, id_man, number):
    with conn.cursor() as cur:
        cur.execute(f"INSERT INTO number_phone(number) VALUES('{number}') RETURNING id;")
        cur.execute(f"INSERT INTO contact_phone(man_id, phone_id) VALUES('{id_man}', '{cur.fetchone()[0]}' );")
        # print(cur.fetchone())
        conn.commit()

def change_contact(conn, id_client, last_name=None, first_name=None, email=None, phone=None):
    with conn.cursor() as cur:
        if last_name != None:
            cur.execute("UPDATE contact_man SET last_name=%s WHERE id=%s;",
                       (last_name, id_client))
        if first_name != None:
            cur.execute("UPDATE contact_man SET first_name=%s WHERE id=%s;",
                        (first_name, id_client))
        if email != None:
            cur.execute("UPDATE contact_man SET email=%s WHERE id=%s;",
                        (email, id_client))
        if phone != None:
            cur.execute("UPDATE number_phone SET number=%s WHERE id=%s;",
                        (phone, id_client))
        conn.commit()

def delete_number(conn, id_client):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM contact_phone WHERE man_id=%s;",
                   (id_client))
        conn.commit()

def delete_client(conn, id_client):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM contact_phone WHERE man_id=%s;",
                    (id_client))
        cur.execute("DELETE FROM contact_man WHERE id=%s;",
                    (id_client))
        conn.commit()

def select_client(conn, last_name=None, first_name=None, email=None, phone=None):
    with conn.cursor() as cur:
        if last_name != None:
            cur.execute("SELECT last_name, first_name, email, np.number FROM contact_man cm "
                        "JOIN contact_phone cp on cm.id = cp.man_id "
                        "JOIN number_phone np on np.id = cp.phone_id "
                        "WHERE last_name=%s;",
                        (last_name, ))
            print(cur.fetchall())
        if first_name != None:
            cur.execute("SELECT last_name, first_name, email, np.number FROM contact_man cm "
                        "JOIN contact_phone cp on cm.id = cp.man_id "
                        "JOIN number_phone np on np.id = cp.phone_id "
                        "WHERE first_name=%s;",
                        (first_name, ))
            print(cur.fetchall())
        if email != None:
            cur.execute("SELECT last_name, first_name, email, np.number FROM contact_man cm "
                        "JOIN contact_phone cp on cm.id = cp.man_id "
                        "JOIN number_phone np on np.id = cp.phone_id "
                        "WHERE email=%s;",
                        (email, ))
            print(cur.fetchall())
        if phone != None:
            cur.execute("SELECT last_name, first_name, email, np.number FROM contact_man cm "
                        "JOIN contact_phone cp on cm.id = cp.man_id "
                        "JOIN number_phone np on np.id = cp.phone_id "
                        "WHERE np.number=%s;",
                        (phone, ))
            print(cur.fetchall())

if __name__ == '__main__':
    password = int(config["parol"]["parol"])
    # print(password)
    conn = psycopg2.connect(database="contact", user="postgres", password=f"{password}")
    new_table(conn)  #Make new table
    new_client(conn, "Vasia", "Pupkins", "milo@f.com", '155') #Make new client
    change_contact(conn, '1', first_name='Andreika', phone='456321') #Chanel contact
    add_phone(conn, '1', '456321') #add phones contact
    delete_number(conn, "1") # deleted number
    delete_client(conn, '2') # delete client
    select_client(conn, phone='456321') #select client
    conn.close()

