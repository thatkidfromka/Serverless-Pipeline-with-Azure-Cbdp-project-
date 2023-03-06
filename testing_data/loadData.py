import uuid
import os
import logging
import json
import pandas as pd
from io import StringIO

import azure.functions as func
from azure.storage.queue import (
    QueueClient,
    BinaryBase64EncodePolicy,
    BinaryBase64DecodePolicy
)
import base64


coded_string = 'VGVzdDI='
connect_str = "DefaultEndpointsProtocol=https;AccountName=342798357878325789238553;AccountKey=8O/UZhm2m2m/yM5+3LomoMl12+jaHLDPQBNDAQMzsQLTIOmuQWlJxamjuqNHQ9GXIkTRmVKh3MAZ+AStZiDvSg==;EndpointSuffix=core.windows.net"
q_name = "results"

queue = QueueClient.from_connection_string(
    conn_str=connect_str, queue_name=q_name)

# Receive the messages
response = queue.receive_messages(messages_per_page=2)

# Print the content of the messages
for message in response:
    print(base64.b64decode(message.content).decode("utf-8"))
    m = base64.b64decode(message.content).decode("utf-8").split(',')

