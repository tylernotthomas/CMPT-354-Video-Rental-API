from flask import Flask, g, request, jsonify
import pyodbc
from connect_db import connect_db
import sys
import time, datetime


app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello, World!'

def get_db():
    """Opens a new database connection if there is none yet for the
    current application context.
    """
    if not hasattr(g, 'azure_db'):
        g.azure_db = connect_db()
        g.azure_db.autocommit = True
        g.azure_db.set_attr(pyodbc.SQL_ATTR_TXN_ISOLATION, pyodbc.SQL_TXN_SERIALIZABLE)
    return g.azure_db

@app.teardown_appcontext
def close_db(error):
    """Closes the database again at the end of the request."""
    if hasattr(g, 'azure_db'):
        g.azure_db.close()



@app.route('/login')
def login():
    username = request.args.get('username', "")
    password = request.args.get('password', "")
    cid = -1
    #print (username, password)
    conn = get_db()
    #print (conn)
    cursor = conn.execute("SELECT * FROM Customer WHERE username = ? AND password = ?", (username, password))
    records = cursor.fetchall()
    #print records
    if len(records) != 0:
        cid = records[0][0]
    response = {'cid': cid}
    return jsonify(response)




@app.route('/getRenterID')
def getRenterID():
    """
       This HTTP method takes mid as input, and
       returns cid which represents the customer who is renting the movie.
       If this movie is not being rented by anyone, return cid = -1
    """
    mid = int(request.args.get('mid', -1))
    cid = -1 #if cid is not overwritten -1 will be returned
    conn = get_db()
    cursor = conn.execute("SELECT * FROM Rental WHERE mid = ? AND status = 'open'", mid)
    records = cursor.fetchall()
    if len(records) != 0:
        cid = records[0][0]
    response = {'cid': cid}
    return jsonify(response)



@app.route('/getRemainingRentals')
def getRemainingRentals():
    """
        This HTTP method takes cid as input, and returns n which represents
        how many more movies that cid can rent.

        n = 0 means the customer has reached its maximum number of rentals.
    """
    cid = int(request.args.get('cid', -1))

    conn = get_db()
    # Tell ODBC that you are starting a multi-statement transaction
    conn.autocommit = False

    cursor = conn.execute("SELECT max_movies FROM Customer C, RentalPlan P WHERE C.pid = P.pid AND C.cid = ?", cid)
    recordsPlan = cursor.fetchall()
    cursor = conn.execute("SELECT COUNT(*) FROM Rental WHERE cid = ? AND status = 'open'", cid)
    recordsRental = cursor.fetchall()

    conn.autocommit = True
    
    if len(recordsPlan) != 0:
        maxMovies = recordsPlan[0][0]
    if len(recordsRental) != 0:
        numRented = recordsRental[0][0]
    n = maxMovies - numRented

    response = {"remain": n}
    return jsonify(response)





def currentTime():
    ts = time.time()
    return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')


@app.route('/rent')
def rent():
    """
        This HTTP method takes cid and mid as input, and returns either "success" or "fail".

        It returns "fail" if C1, C2, or both are violated:
            C1. at any time a movie can be rented to at most one customer.
            C2. at any time a customer can have at most as many movies rented as his/her plan allows.
        Otherwise, it returns "success" and also updates the database accordingly.
    """
    cid = int(request.args.get('cid', -1))
    mid = int(request.args.get('mid', -1))

    conn = get_db()

     # Tell ODBC that you are starting a multi-statement transaction
    conn.autocommit = False

    numFail = 0
    cursor = conn.execute("SELECT COUNT(*) FROM Rental WHERE mid = ? AND status = 'open'", mid)
    recordsCount = cursor.fetchall()
    if recordsCount[0][0] != 0:
        numFail = numFail + 1
    
    cursor = conn.execute("SELECT max_movies FROM Customer C, RentalPlan P WHERE C.pid = P.pid AND C.cid = ?", cid)
    recordsPlan = cursor.fetchall()
    cursor = conn.execute("SELECT COUNT(*) FROM Rental WHERE cid = ? AND status = 'open'", cid)
    recordsRental = cursor.fetchall()

    if len(recordsPlan) != 0:
        maxMovies = recordsPlan[0][0]
    if len(recordsRental) != 0:
        numRented = recordsRental[0][0]

    n = maxMovies - numRented
    if n <= 0:
        numFail = numFail + 1

    conn.execute("INSERT INTO Rental VALUES (?,?,?,'open')", (cid, mid, currentTime()))
    if numFail > 0:
        conn.rollback()

    conn.autocommit = True

    if numFail > 0:
        response = {"rent": "fail"}
    else:
        response = {"rent": "success"}

    #response = {"rent": "success"} OR response = {"rent": "fail"}
    return jsonify(response)

