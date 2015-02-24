from flask import Flask, abort, jsonify, request
import os
from sqlalchemy import create_engine, MetaData, Table
import psycopg2
import datetime

app = Flask(__name__)

#trying to connect to the database
# noinspection PyBroadException
try:
    # postgresql+psycopg2://user:password@host:port/dbname[?key=value&key=value...]
    db_engine = create_engine(
        "postgresql+psycopg2://" +
        "weklupyvfstrbo:xrQ7q81SexyqHZU0gF7P_B2xhv@ec2-107-21-93-97.compute-1.amazonaws.com:5432/delbu700esm11g")
    db_connection = db_engine.connect()

except Exception as e:
    print e.message
    print ("failed database connection")
    abort(500)

db_metadata = MetaData(db_engine)

#setting up database tables
request_table = Table('requests', db_metadata, autoload=True)
worker_table = Table('worker', db_metadata, autoload=True)
result_table = Table('results', db_metadata, autoload=True)

@app.route('/')
def hello_world():
    return 'Hello World!'

#takes a result proxy from a sql and converts it into a list of dictionaries
def get_list_of_dicts(result_proxy):
    result_set = []
    for row in result_proxy:
        result_set.append(dict(row))
    return result_set


#Gets the current time for the db
def get_current_time_db():
    return datetime.time.isoformat(datetime.datetime.now().time())


#get all open requests for images - for the workers
@app.route('/getRequests')
def requests():
    return jsonify(requests=get_list_of_dicts(request_table.select().execute()))


#create a request for a new image search - for the controller
@app.route('/createRequest', methods=["POST"])
def create_request():
    status = request_table.insert().values(
        imei=request.form["imei"],
        image=request.form["image"], sub_image=request.form["sub_image"],
        rtime=get_current_time_db()).execute()

    return jsonify(status=status)
#delete a current request - for the controller
@app.route('/deleteRequest', methods=["POST"])
def delete_request():
    status = request_table.delete().values(
        imei=request.form["imei"]).execute()

    return jsonify(status=status)

#register as a worker for some job which is given by the parent imei - for workers
@app.route('/registerWorker', methods=["POST"])
def register_worker():
    status = worker_table.insert().values(
        rtime=get_current_time_db(),
        imei=request.form["imei"],
        parent=request.form["parent"]).execute()

    return jsonify(status=status)

#unregister as a worker for some job which is given by the parent imei - for workers
@app.route('/unregisterWorker', methods=["POST"])
def un_register_worker():
    worker_table.delete().values(imei=request.form["imei"]).execute()

#gets all the workers that have registered for the job, and informs them to start working on the job
#TODO
@app.route('/startJob', methods=["POST"])
def start_job():
    workers = get_list_of_dicts(worker_table.select().where(parent=request.form["imei"]).execute())
    #The calculation logic of which worker gets which coordinates and stuff goes here

#registers a result once it has completed - for the worker
@app.route('/registerWorkerResult', methods=["POST"])
def register_worker_result():
    result_table.insert().values(imei=request.form["imei"], result=request.form["result"]).execute()

#gets the results for a certain job once it has completed - for the controller
@app.route('/getResults', methods=["GET"])
def get_worker_result():
    return jsonify(results=get_list_of_dicts(result_table.select().where(
        result_table.c.parent == request.args.get("imei")).execute()))


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=True, host="0.0.0.0", port=port)
