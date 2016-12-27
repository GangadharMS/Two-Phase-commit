import java.util.List;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

//import org.apache.log4j.BasicConfigurator;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

public class ParticipantServer {

	public static ParticipantHandler handler;
	public static FileStore.Processor<ParticipantHandler> processor;
	public static void main(String[] args){
		int portNumber;
		String participantName;
		if(args.length != 2){
			System.out.println("Please provide participant name and port number");
			System.exit(0);
		}
		try{
			portNumber = Integer.parseInt(args[1]);
			participantName = args[0];	
			handler = new ParticipantHandler(participantName);
			processor = new FileStore.Processor<ParticipantHandler>(handler);
			//BasicConfigurator.configure();
			StartServer(processor,portNumber);
		}
		catch(NumberFormatException e){
			System.out.println("Please provide participant name and port number in order");
			System.exit(0);
		} catch (TTransportException e) {
			e.printStackTrace();
		} 
	}

	public static void StartServer(FileStore.Processor<ParticipantHandler> processor, int portNumber){
		//System.out.println("port number is "+portNumber);
		try{
			TServerTransport serverTransport = new TServerSocket(portNumber);
			TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
			System.out.println("Participant started in port "+portNumber);
			server.serve();
		} 
		catch(TTransportException e){
			System.out.println("Provided port number is in use. Please provide different port number");
			System.exit(0);
		}
		catch (Exception e) {
			//TODO
		System.out.println("One of the communicating branch went down. I am shutting down.");
		System.exit(0);
		//	e.printStackTrace();
		}
	}	

}
