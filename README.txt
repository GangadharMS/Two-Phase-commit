CS557 Introduction to Distributed Systems
Fall 2016
PROJECT-3 README FILE

Author: Gangadhara Maddur Srikantaiah
e-mail: gmaddur1@binghamton.edu

PURPOSE:
Purpose of this project is to implement Two-Phase commit algorithm to a file server and client using Apache thrift framework.

IMPLEMENTATION:
This assignment is implemented using java programming language. As per the requirement Apache thrift framework is used 
to implement the project.

PERCENT COMPLETE:
We believe We have completed 100% of this project.

PARTS THAT ARE NOT COMPLETE:
N/A

BUGS:
None.

FILES:
Coordinator.java
CoordinatorHandler.java
FileHandlingClient.java
FileStore.java
Participant.java
ParticipantHandler.java
ParticipantServer.java
RFile.java
Status.java
StatusReport.java
SystemException.java
Transaction.java
Makefile
coordinatorServer.sh
participantServer.sh
client.sh
branches.txt
listOfFiles.txt
listOfOperations.txt
README.txt

All source files are inside src directory.

SAMPLE OUTPUT:

Refer report.


TO COMPILE:
make

TO RUN:
Participant:
./participant.sh <participant name> <port number>

Coordinator:
./coordinator.sh branches.txt <port number>

Client:
./client.sh <ip address of coordinator> <port number of coordinator> <listOfFiles.txt> <listOfOperations.txt> <number of threads to run>




