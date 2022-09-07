import os

container = os.environ["AZ_BLOB_CONTAINER"]
conn_string = os.environ["AZ_SA_CONN_STRING"]

print(container,'\n'+conn_string)