import simdjson
parser = simdjson.Parser()
for i in parser.get_implementations:
    print(i)
print(parser.implementation)
doc = parser.parse(b'{"res": [{"name": "first"}, {"name": "second"}]}', True)
print(doc)
