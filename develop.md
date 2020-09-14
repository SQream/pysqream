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
 
 ### Data Column Structure
 
 For a regular in-line insert, we may use an sql such as:
 
 `--create or replace table test (x int, y int not null, z text)` 
 
 `insert into table test values (5, 6, 'alabama'), (7, 8, 'mississippi')`
  
 (Note: The default nullability (when not mentioned otherwise) of columns in SQream is nullable)
 
 When the amount of data increases, we like to be able to pack it in advance, and use batch insert or network insert.
 
 Every sql column we want to send, will constitute 1-3 binary columns that we'll put adjacently and send to SQream:
 - Data column - The actual data represented in its binary format.
 - Null column - A byte column which marks which items are actually nulls (Optional - for nullable columns). Always comes first when applicable
 - Text sizes column - Our text (alias - nvarchar) columns are prepended with an int lengths column - for each string, what is the *encoded* length 

 
 For the above example, we have `[5, 7]` for column x, `[6, 8]` for column y, `['alabama', 'mississippi']` for column z. 
 
 Lets see how the first column looks packed:
 ```python
 from struct import pack
 
 pack('2i', 6, 8)
 # Output:  b'\x06\x00\x00\x00\x08\x00\x00\x00'
 ```
 
The 1st parameter to `pack` is a code for what is being packed - `i` means one integer, `2i` means two integers etc.

The list of supported types for packing and their codes can be found [here](https://docs.python.org/3/library/struct.html#format-characters).
Relevant types for SQream and their codes are saved thus:
```python
# SQream type names on the left, struct codes on the right
type_to_letter = {
    'ftBool'     : '?',
    'ftUByte'    : 'B',
    'ftShort'    : 'h',
    'ftInt'      : 'i',
    'ftLong'     : 'q',
    'ftFloat'    : 'f',
    'ftDouble'   : 'd',
    'ftDate'     : 'i',
    'ftDateTime' : 'q',
    'ftVarchar'  : 's',
    'ftBlob'     : 's'
}
```

The funny names on the left are the internal representation names for SQream's types, and these names show up in appropriate message jsons received from SQream, namely table metadata jsons known as `queryType`.
 
What about the null column?


 
 
 
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
 - We use formatted strings to fill up most jsons sent to SQream. Example:
 ```python
 num_rows = 100
 msg_json = f'{{"put":{num_rows}}}'  # ready to be encoded, prepended with a header and sent to SQreamd
 ```
 
 #### struct (`from struct import pack, unpack`)
 We use `struct` for the data (non json-message) parts of our messaging - packing Python objects into bytes and other way around.
 
 
 
 
 #### socket  (`import socket`)
 socket is the lowest software abstraction for connecting remote machines. Since SQreamd uses a custom protocol, we implement it over socket. 
 To send a connection message to SQreamd, we might do as follows:
 
 
 
 
 
 
 
 
 
 
 
