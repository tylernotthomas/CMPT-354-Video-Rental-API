import pyodbc

def connect_db():
    ODBC_STR = "Driver={ODBC Driver 13 for SQL Server};Server=tcp:videostoreserver7.database.windows.net,1433;Database=VideoStore;Uid=Tyler@videostoreserver7;Pwd={Canadabc1};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    return pyodbc.connect(ODBC_STR)


if __name__ == '__main__':
    print (connect_db())
