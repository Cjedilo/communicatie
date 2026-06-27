import json

from datetime import datetime
import uuid

import asyncpg

class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, uuid.UUID) or isinstance(obj, asyncpg.pgproto.pgproto.UUID):  
            return str(obj)
        if isinstance(obj, datetime):
            return obj.strftime("%m/%d/%Y, %H:%M:%S.%f")
        return json.JSONEncoder.default(self, obj)
    