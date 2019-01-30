import pyodbc
from connect_db import connect_db


def loadRentalPlan(filename, conn):
    """
        Input:
            $filename: "RentalPlan.txt"
            $conn: you can get it by calling connect_db()
        Functionality:
            1. Create a table named "RentalPlan" in the "VideoStore" database on Azure
            2. Read data from "RentalPlan.txt" and insert them into "RentalPlan"
               * Columns are separated by '|'
               * You can use executemany() to insert multiple rows in bulk
    """
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE RentalPlan(pid INTEGER, pname VARCHAR(50), monthly_fee FLOAT, max_movies INTEGER, PRIMARY KEY(pid))")
    rentalPlanFile = open("RentalPlan.txt", "r")
    for line in rentalPlanFile:
        temp = line.strip()
        res = temp.split("|")
        print("INSERT INTO dbo.RentalPlan(pid,pname,monthly_fee,max_movies) VALUES(%s,'%s',%s,%s)" % (res[0], res[1], res[2], res[3]))
        cursor.execute("INSERT INTO dbo.RentalPlan(pid,pname,monthly_fee,max_movies) VALUES(%s,'%s',%s,%s)" % (res[0], res[1], res[2], res[3]))


def loadCustomer(filename, conn):
    """
        Input:
            $filename: "Customer.txt"
            $conn: you can get it by calling connect_db()
        Functionality:
            1. Create a table named "Customer" in the "VideoStore" database on Azure
            2. Read data from "Customer.txt" and insert them into "Customer".
               * Columns are separated by '|'
               * You can use executemany() to insert multiple rows in bulk
    """
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE Customer(cid INTEGER, pid INTEGER, username VARCHAR(50), password VARCHAR(50), PRIMARY KEY(cid), FOREIGN KEY(pid) REFERENCES RentalPlan(pid) ON DELETE CASCADE)")
    customerFile = open("Customer.txt", "r")
    for line in customerFile:
        temp = line.strip()
        res = temp.split("|")
        cursor.execute("INSERT INTO dbo.Customer VALUES(%s,%s,'%s','%s')" % (res[0], res[1], res[2], res[3]))


def loadMovie(filename, conn):
    """
        Input:
            $filename: "Movie.txt"
            $conn: you can get it by calling connect_db()
        Functionality:
            1. Create a table named "Movie" in the "VideoStore" database on Azure
            2. Read data from "Movie.txt" and insert them into "Movie".
               * Columns are separated by '|'
               * You can use executemany() to insert multiple rows in bulk
    """
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE Movie(mid INTEGER, mname VARCHAR(50), year INTEGER, PRIMARY KEY(mid))")
    movieFile = open("Movie.txt", "r")
    for line in movieFile:
        temp = line.strip()
        res = temp.split("|")
        cursor.execute("INSERT INTO Movie VALUES(%s,'%s',%s)" % (res[0], res[1], res[2]))


def loadRental(filename, conn):
    """
        Input:
            $filename: "Rental.txt"
            $conn: you can get it by calling connect_db()
        Functionality:
            1. Create a table named "Rental" in the VideoStore database on Azure
            2. Read data from "Rental.txt" and insert them into "Rental".
               * Columns are separated by '|'
               * You can use executemany() to insert multiple rows in bulk
    """
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE Rental(cid INTEGER, mid INTEGER, date_and_time DATETIME, status VARCHAR(6), FOREIGN KEY(cid) REFERENCES Customer(cid) ON DELETE CASCADE, FOREIGN KEY(mid) REFERENCES Movie(mid) ON DELETE CASCADE)")
    rentalFile = open("Rental.txt", "r")
    for line in rentalFile:
        temp = line.strip()
        res = temp.split("|")
        cursor.execute("INSERT INTO Rental VALUES(%s,%s,'%s','%s')" % (res[0], res[1], res[2], res[3]))


def dropTables(conn):
    conn.execute("DROP TABLE IF EXISTS Rental")
    conn.execute("DROP TABLE IF EXISTS Customer")
    conn.execute("DROP TABLE IF EXISTS RentalPlan")
    conn.execute("DROP TABLE IF EXISTS Movie")



if __name__ == "__main__":
    conn = connect_db()

    dropTables(conn)

    loadRentalPlan("RentalPlan.txt", conn)
    loadCustomer("Customer.txt", conn)
    loadMovie("Movie.txt", conn)
    loadRental("Rental.txt", conn)


    conn.commit()
    conn.close()






