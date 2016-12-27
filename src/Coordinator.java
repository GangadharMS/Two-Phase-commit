import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.Vector;

//import org.apache.log4j.BasicConfigurator;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

public class Coordinator {
	public static CoordinatorHandler handler;
	public static FileStore.Processor<CoordinatorHandler> processor;
	public static void main(String[] args){
		int portNumber;
		String participantsFileName;
		Vector<Participant> participantsList = new Vector<Participant>();
		Scanner inputSource = null;
		if(args.length != 2){
			System.out.println("Please provide participants info file name and port number");
			System.exit(0);
		}
		try{
			portNumber = Integer.parseInt(args[1]);
			participantsFileName = args[0];
			inputSource = new Scanner(new File(participantsFileName));
			while(true){
				if(!inputSource.hasNext()){
					break;
				}
				String eachLine = inputSource.nextLine();
				String eachLineContent[]=eachLine.split("\\s");
				Participant participant = new Participant();
				participant.setName(eachLineContent[0]);
				participant.setIp(eachLineContent[1]);
				participant.setPort(Integer.parseInt(eachLineContent[2]));
				participantsList.add(participant);
			}	
			try {
				handler = new CoordinatorHandler(participantsList);
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			processor = new FileStore.Processor<CoordinatorHandler>(handler);
			//BasicConfigurator.configure();
			StartServer(processor,portNumber);
		}
		catch(NumberFormatException e){
			System.out.println("Please provide participants info file name and port number in order");
			System.exit(0);
		} catch (FileNotFoundException e) {
			System.out.println("Participants info file not found, please check.");
			System.exit(0);
		}
	}

	public static void StartServer(FileStore.Processor<CoordinatorHandler> processor, int portNumber){
		//System.out.println("port number is "+portNumber);
		try{
			TServerTransport serverTransport = new TServerSocket(portNumber);
			TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
			System.out.println("Cooordinator started in port "+portNumber);
			server.serve();
		} 
		catch(TTransportException e){
			System.out.println("Provided port number is in use. Please provide different port number");
			System.exit(0);
		}
		catch (Exception e) {
		System.out.println("One of the communicating branch went down. I am shutting down.");
		System.exit(0);
		//	e.printStackTrace();
		}
	}	
}
