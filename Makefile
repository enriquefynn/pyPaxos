PROTOC=protoc
RM=/bin/rm
all: message.proto
	${PROTOC} message.proto --python_out=.
clean:
	${RM} -rf message_pb2.py *.pyc
