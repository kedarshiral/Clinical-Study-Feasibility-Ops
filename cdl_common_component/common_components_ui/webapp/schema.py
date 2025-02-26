import sys
from messytables import CSVTableSet, type_guess,jts,types_processor, headers_guess, headers_processor,offset_processor, any_tableset


def infer_schema(filename):
	fh = open(filename, 'rb')

# Load a file object:
	table_set =any_tableset(fh)

# If you aren't sure what kind of file it is, you can use
# any_table

# A table set is a collection of tables:
	row_set = table_set.tables[0]

# guess header names of the header:
	offset, headers = headers_guess(row_set.sample)

#print(type(headers))

#print(map(str,headers))

	row_set.register_processor(headers_processor(headers))

# add one to begin with content, not the header:
	row_set.register_processor(offset_processor(offset + 1))

# guess column types:
	types = type_guess(row_set.sample, strict=True)
#print(types)
#print(type(types))
#print(str(types))
#print(len(map(str,headers)))
#print(len(types))

	dictionary = dict(zip(map(str,headers),types))

#rint(dictionary)

#print(dictionary.keys())
#print(jts.headers_and_typed_as_jts(str(headers), str(types)))
#callfunction(headers,types)
# and tell the row set to apply these types to
# each row when traversing the iterator:
	row_set.register_processor(types_processor(types))

	return map(str,headers),types