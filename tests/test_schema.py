from easycopy.schema.comparison import compare_schema
class Dummy:
    def __init__(self, fields):
        self.fields = fields

s = Dummy([{"name":"id","type":"INTEGER"},{"name":"name","type":"STRING","length":50}])
t = Dummy([{"name":"id","type":"SMALLINTEGER"},{"name":"name","type":"STRING","length":100}])
print(compare_schema(s, t, mode='SOFT'))