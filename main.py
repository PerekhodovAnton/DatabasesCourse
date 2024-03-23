import asyncio
import asyncpg
from fastapi import FastAPI

app = FastAPI()

# это пока тоже тестовые данные, т.к. сервис авторизации пока не реализован
username = 'Polina'
user_type = 'admin'

# переменная для иимтации работы модели
summarization_model = 'Сработала суммаризация' #test model

# подключаемся к БД
async def connect_to_database():
    try:
        conn = await asyncpg.connect(
            user='postgres',
            password='sql',
            database='postgres',
            host='localhost'
        )
        print("Connected to the database successfully!")
        return conn
    except asyncpg.PostgresError as e:
        print(f"Error connecting to the database: {e}")

# реализован класс для вставки данных в БД
class DBinsert:
    @staticmethod
    async def user(conn, name, user_type):
        try:
            existing_user = await conn.fetchrow(
                'SELECT userid FROM users WHERE name = $1;',
                name
            )
            if existing_user:
                print(f"User with name '{name}' already exists!")
                return existing_user['userid']  # возвращаем существующий userid
            else:
                result = await conn.fetchrow(
                    'INSERT INTO users (name, type) VALUES ($1, $2) RETURNING userid;',
                    name, user_type
                )
                print("User inserted successfully!")
                return result['userid']
        except asyncpg.PostgresError as e:
            print(f"Error inserting user: {e}")
            return None

    @staticmethod
    async def query(conn, user_id, message):
        try: 
            result = await conn.fetchrow(
                "INSERT INTO queries (message, validity, date_time, userid) VALUES ($1, 'valid', NOW(), $2) RETURNING queryid, userid;",
                message, user_id
            )
            print("Query inserted successfully!")
            return result['userid'], result['queryid']  # возвращаем userid и queryid нового запроса
        except asyncpg.PostgresError as e:
            print(f"Error inserting query: {e}")
            return None, None

    @staticmethod
    async def response(conn, text, query_id):
        try: 
            await conn.execute(
                "INSERT INTO responses (text, date_time, queryid) VALUES ($1, NOW(), $2);",
                text, query_id
            )
            print("Response inserted successfully!")
        except asyncpg.PostgresError as e:
            print(f"Error inserting response: {e}")

@app.post("/api/queries/")
async def receive_query(query: str):
    conn = await connect_to_database()
    user_id = await DBinsert.user(conn, username, user_type)  # вставляем пользователя и получаем его userid 
    user_id, query_id = await DBinsert.query(conn, user_id, query)  # используем userid при вставке запроса
    await DBinsert.response(conn, summarization_model + ' ' + query, query_id)  # используем query_id при вставке ответа
    await conn.close()
    return {"message": summarization_model + ' ' + query}


# launch: uvicorn main:app --host 0.0.0.0 --port 8000

'''
работает следующим образом:
при обращении к сервису данные о пользователе сразу добавляются в БД,
далее пользователь отправляет пост-запрос, получает ответ и оба результата записываются в БД
Скриншоты работы представлены в папке screenshots
'''