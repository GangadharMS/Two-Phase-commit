LIB_PATH=/home/yaoliu/src_code/local/libthrift-1.0.0.jar:/home/yaoliu/src_code/local/slf4j-log4j12-1.5.8.jar:/home/yaoliu/src_code/local/slf4j-api-1.5.8.jar
all: clean
	mkdir bin
	mkdir bin/client_classes
	mkdir bin/coordinator_classes
	mkdir bin/participant_classes

	javac -classpath $(LIB_PATH) -d bin/client_classes/ src/FileHandlingClient.java src/FileStore.java src/RFile.java  src/Status.java src/StatusReport.java src/SystemException.java src/Transaction.java src/Participant.java
	javac -classpath $(LIB_PATH) -d bin/coordinator_classes/ src/Coordinator.java src/CoordinatorHandler.java src/FileStore.java src/RFile.java src/Participant.java src/Status.java src/StatusReport.java src/SystemException.java src/Transaction.java
	javac -classpath $(LIB_PATH) -d bin/participant_classes/ src/Participant.java src/ParticipantHandler.java src/ParticipantServer.java src/FileStore.java src/RFile.java src/Status.java src/StatusReport.java src/SystemException.java src/Transaction.java 


clean:
	rm -rf bin/

