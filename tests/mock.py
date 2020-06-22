import socket, ssl, selectors, json, sqlite3

sel = selectors.DefaultSelector()
conn = sqlite3.connect('memory')

ss = socket.socket()
ss.bind(('', 5002))
# ss.listen()

def read():
    
    pass


def accept():

    conn, addr = ss.accept()
    conn.setblocking(False)
    sel.register(conn, selectors.EVENT_READ, read)





# {"username":"sqream", "password":"sqream", "connectDatabase":"master", "service":"sqream"}
'{"username":"sqream", "password":"sqream", "connectDatabase":"master", "service":"sqream"}'
# "getStatementId" : "getStatementId"}
'{"statementId":12458}'
# {"prepareStatement": "select * from waste", "chunkSize": 0}
'{"ip":"192.168.1.4","listener_id":0,"port":5000,"port_ssl":5001,"reconnect":true,"statementPrepared":true}'
# {"service": "sqream", "reconnectDatabase":"master", "connectionId":10512, "listenerId":0,"username":"sqream", "password":"sqream"}
'{"connectionId":10512,"databaseConnected":"databaseConnected","varcharEncoding":"cp874"}'
# {"reconstructStatement": 12458}
'{"statementReconstructed":"statementReconstructed"}'
# {"execute" : "execute"}
'{"executed":"executed"}'
# {"queryTypeIn": "queryTypeIn"}
'{"queryType":[]}'
# {"queryTypeOut" : "queryTypeOut"}
'{"queryTypeNamed":[  \
    {"isTrueVarChar":false,"name":"bools","nullable":true,"type":["ftBool",1,0]}, \
    {"isTrueVarChar":false,"name":"ubytes","nullable":true,"type":["ftUByte",1,0]}, \
    {"isTrueVarChar":false,"name":"shorts","nullable":true,"type":["ftShort",2,0]},  \
    {"isTrueVarChar":false,"name":"ints","nullable":true,"type":["ftInt",4,0]},   \
    {"isTrueVarChar":false,"name":"bigints","nullable":true,"type":["ftLong",8,0]}, \
    {"isTrueVarChar":false,"name":"floats","nullable":true,"type":["ftFloat",4,0]},\
    {"isTrueVarChar":false,"name":"doubles","nullable":true,"type":["ftDouble",8,0]},\
    {"isTrueVarChar":false,"name":"dates","nullable":true,"type":["ftDate",4,0]},\
    {"isTrueVarChar":false,"name":"datetimes","nullable":true,"type":["ftDateTime",8,0]},\
    {"isTrueVarChar":false,"name":"varchars","nullable":true,"type":["ftVarchar",10,0]},\
    {"isTrueVarChar":true,"name":"nvarchars","nullable":true,"type":["ftBlob",0,0]} \
    ]}'
# {"fetch" : "fetch"}
'{"colSzs":[2,2,2,2,2,4,2,8,2,16,2,8,2,16,2,8,2,16,2,20,2,8,8],"rows":2}'
'binary size 138 checksum(s) 1104745215 1489118142 1104745215 3586999183 1104745215 2398564995 1104745215 2279106030 1104745215 1293789500 1104745215 2582315024 1104745215 2113974802 1104745215 456794608 1104745215 2532444189 1104745215 1953235247 1104745215 1846167236 3344985997'
# {"fetch" : "fetch"}
'{"colSzs":[],"rows":0}'
# {"closeStatement": "closeStatement"}
'{"statementClosed":"statementClosed"}'
# {"closeConnection":  "closeConnection"}
'{"connectionClosed":"connectionClosed"}'

