# Developer Guide

## Background 

### How does a SQream connector communicate

- SQreamd listens on 1-2 ports, one is plain, the other secure.
- The server accepts two types of messages: 
  
     - json messages - valid json strings encoded to bytes.
     - raw data      - columns sent to be inserted into a table via what we call "network insert".
     
     The json messages will mostly be plain English, except for possibly one - the json that passes the SQL query that was entered by the user.
     
     For our nvarchar / text (textual data type) columns, the encoding SQreamD uses is UTF8 - also the standard encoding in Python.  
 - Each message, json or data, is prepended by a 10 byte header:
     
     - byte 1     - Protocol version   - The Python connector supports versions 6-8
     - byte 2     - text or bytes flag - 1 - the message a json, 2 - message is raw data (the main course of a network insert correspondence) 
     - bytes 3-10 - size of the json / raw data as a bigint / long
 - The protocol is symmetrical in this regard - messages sent from SQreamd are also either json messages or raw data, packed in the same way, similar to messages sent to SQreamd.
 - Messages from SQreamd are prepended with the same header as described above.
 - SQreamd and a connector may respond to the header info in different ways. For example:
     - SQreamd supports only one protocol version, a connector may support several.
     - SQreamd may raise an error if the text/binary flag is mismatched, a connector may be more forgiving.
     - etc.
 - Each side (SQreamd / connector) sends one message and waits for a response. 
 - There is only one occasion where 2 messages are sent one after the other - the message before a binary data is sent to or from SQream (see Protocol state machine)    
 
 ### Protocol State Machine
 
 
 ### Python connector mechanics
 
 To facilitate the communication described above, we use the following libraries, all in Python's standard library:
 - socket
 - json
 - struct
 
 #### json  (`import json`)
This library is used to parse responses from SQream, and to pack one statement sent to sqream - "prepareStatement"

`json.loads` - Convert a valid json string to a Python dictionary. Example:
``` python
res = json.loads('{"connectionId":3130,"databaseConnected":"databaseConnected","varcharEncoding":"ascii"}')

res['connectionId']
# Output : 3130
```

`json.dumps` - Convert a Python dictionary to a json string. Example:
``` python
stmt = "select * from some_table where [parentheses, newlines, escaped characters, other weird stuff]"

# Python's json parser then makes it all into a valid json string, that can be properly opened via json.loads and others
stmt_json = json.dumps({"prepareStatement": stmt, "chunkSize": DEFAULT_CHUNKSIZE})  

# Fun fact - "chunkSize" key doesn't do anything, set to 0 in the Python connector
 ```
 
 Note that:
 - `json.loads` takes a string, whereas `json.dumps` takes a python dictionay - The separation could be as little as the external single parentheses
 - 
 
 
 
 
 
 
 
 
 
 
 
 
 
