from flask import Flask, request, jsonify
from redis import Redis
import psycopg2

def conn_postgres(): 
    conn = psycopg2.connect(host='10.100.24.26', 
                        dbname='postgres', 
                        user='postgres', 
                        password='postgres') 
    return conn

def conn_redis(): 
    r = Redis(host="10.100.34.209", 
                port=10076, 
                password="P@ssw0rd", 
                decode_responses=True) 
    return r

app = Flask(__name__)

# @app.route('/postgres', methods=['GET']) 
# def postgres(): 
#     if request.method == 'GET': 
#         cur = conn_postgres().cursor()
#         query = 'select customerid, gender, age, annual_income_$, profession, work_experience, family_size from customer_data where customerid=1'
#         cur.execute(query)
#         tampung = [dict((cur.description[i][0], value) \
#                     for i, value in enumerate(row)) for row in cur.fetchall()] 
#         cur.connection.close()
#         return (tampung[0] if tampung else None) if 1 else tampung
#     else: 
#         return 0
    
@app.route('/postgres', methods=['GET']) 
def postgres(): 
    if request.method == 'GET': 
        args = request.args 
        param = args.get("id", type=str)
        cur = conn_postgres().cursor()
        query = f"select customerid, gender, age, annual_income_$, profession, work_experience, family_size from customer_data where customerid={param}"
        cur.execute(query)
        tampung = [dict((cur.description[i][0], value) \
                    for i, value in enumerate(row)) for row in cur.fetchall()] 
        cur.connection.close()
        return (tampung[0] if tampung else None) if 1 else tampung
    else: 
        return 0

@app.route('/redis', methods=['GET']) 
def redis(): 
    if request.method =='GET':
        args = request.args 
        param = args.get("id", type=str)
        tes = conn_redis().hgetall(f"customers:{param}")
        return tes
    else: 
        return 0

if __name__ == '__main__':
   app.run(port=5000)
