import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Hashtable;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class ParticipantHandler implements FileStore.Iface{
	Hashtable<String,Integer> fileInfo = new Hashtable<String,Integer>();
	//to abort write operation to same file
	Hashtable<String,Boolean> canWrite = new Hashtable<String,Boolean>();
	//to log participant decisions and recover from crash
	Hashtable<String,Transaction> participantLog = new Hashtable<String,Transaction>();
	Hashtable<String,Transaction> tempParticipantLog = new Hashtable<String,Transaction>();
	String participantName;
	boolean votedTransaction = false;
	public ParticipantHandler(String participantName) throws TTransportException{
		this.participantName = participantName;
		FileInputStream fin;
		try {
			//to load all the files from persistent storage
			if(new File("logs/persistentBackUp"+this.participantName+".txt").exists()){
				fin = new FileInputStream("logs/persistentBackUp"+this.participantName+".txt");
				ObjectInputStream oin = new ObjectInputStream(fin);
				fileInfo = (Hashtable<String, Integer>) oin.readObject();
				oin.close();
				fin.close();
			}

			//to check logs after recovery
			
			if(new File("logs/participantLog"+this.participantName+".txt").exists()){
				
				fin = new FileInputStream("logs/participantLog"+this.participantName+".txt");
				ObjectInputStream oin = new ObjectInputStream(fin);
				tempParticipantLog = (Hashtable<String, Transaction>) oin.readObject();
				oin.close();
				fin.close();
				int j = 0;
				for(int i=0;i<tempParticipantLog.size();i++){
					j = i+1;
					try{
						if(tempParticipantLog.containsKey(j+"VOTE_COMMIT")){
							if(tempParticipantLog.containsKey(j+"DO_COMMIT") || tempParticipantLog.containsKey(j+"DO_ABORT")){
								//Do nothing
								//System.out.println("committed or aborted");
							}
							else{
								//after voting yes to commit participant failed so communicate 
								//to coordinator for it's final decision
								//System.out.println(tempParticipantLog.get(i+"VOTE_COMMIT").getTransactionId());
								//System.out.println("Not committed or aborted");
								this.coordinatorFinalDecision(tempParticipantLog.get(j+"VOTE_COMMIT"));
							}
						}
					}
					catch (Exception e ) {
						System.out.println("Coordinator is down. Will try to communicate after sometime");
						Thread.sleep(5000);
						i = i -1;
						continue;
					} 
				}


			}

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch(Exception e){
			System.out.println("Something went wrong. Restart all operations");
		}

	}

	public void coordinatorFinalDecision(Transaction transaction) throws SystemException, TException{
		//contact coordinator to check it's final decision on a particular transaction
		//assumption is list's first entry is the coordinator info


		StatusReport statusReport = new StatusReport();
		TTransport transport = null;
		//System.out.println(transaction.getParticipants().get(0).getPort());
		transport = new TSocket(transaction.getParticipants().get(0).getIp(), 
				transaction.getParticipants().get(0).getPort());
		transport.open();
		TProtocol protocol = new TBinaryProtocol(transport);
		FileStore.Client client = new FileStore.Client(protocol);
		statusReport = client.checkDecision(transaction);
		//System.out.println("Received status is "+statusReport.getStatus());
		//statusReport.setStatus(Status.COMMIT);
		if(statusReport.getStatus().equals(Status.COMMIT)){//statusReport.getStatus().equals("GLOBAL_COMMIT") ||
			this.doCommit(transaction);
		}
		else if( statusReport.getStatus().equals(Status.ABORT)){//statusReport.getStatus().equals("GLOBAL_ABORT") ||
			// call doAbort
			this.doAbort(transaction);
		}
		else{
			//do nothing
			System.out.println("do nothing");
		}
	}

	@Override
	public StatusReport writeFile(RFile rFile) throws SystemException, TException {
		//to abort write request on same file
		canWrite.put(rFile.getFilename(), true);
		PrintWriter writer = null;
		StatusReport statusReport = new StatusReport();
		BasicFileAttributes attribute = null;
		String fileName = (rFile.getFilename()).toUpperCase();
		File file = new File(fileName);
		try {
			if(!file.exists() || file.isDirectory()) { 
				writer = new PrintWriter(fileName, "UTF-8");
			}
			else{
				writer = new PrintWriter(fileName);
			}
			Path path = file.toPath(); 
			attribute = Files.readAttributes(path, BasicFileAttributes.class);
		} catch (FileNotFoundException e) {
			statusReport.setStatus(Status.FAILED);
			e.printStackTrace();
			return statusReport;

		} catch (UnsupportedEncodingException e) {
			statusReport.setStatus(Status.FAILED);
			e.printStackTrace();
			return statusReport;
		} catch (IOException e) {
			statusReport.setStatus(Status.FAILED);
			e.printStackTrace();
			return statusReport;
		}
		writer.println(rFile.getContent());
		writer.close();
		//add to dataStructure
		fileInfo.put(fileName,rFile.getContent().length());
		//save to persistent storage
		try {
			FileOutputStream fos = new FileOutputStream("logs/persistentBackUp"+this.participantName+".txt");
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(fileInfo);
			oos.close();
			fos.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		statusReport.setStatus(Status.SUCCESSFUL);
		canWrite.put(rFile.getFilename(), false);
		return statusReport;
	}

	@Override
	public RFile readFile(String filename, String owner) throws SystemException, TException {
		RFile rFile = new RFile();
		File file;
		filename = (filename).toUpperCase();
		FileInputStream inputStream;
		if(fileInfo.containsKey(filename)){
			file = new File(filename);
			try {
				inputStream = new FileInputStream(file);
				byte[] content = new byte[(int) file.length()];
				inputStream.read(content);
				rFile.setContent(new String(content,"UTF-8"));
				inputStream.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		else{

			SystemException e = new SystemException();
			e.setMessage("File doesn't exist");
			throw e;
		}
		return rFile;
	}


	@Override
	public StatusReport canCommit(Transaction transaction) throws SystemException, TException {
		//System.out.println("canCommit called in participant");
		StatusReport statusReport = new StatusReport();
		if(canWrite.containsKey(transaction.getRFile().getFilename())){
			if(canWrite.get(transaction.getRFile().getFilename())){
				statusReport.setStatus(Status.VOTE_ABORT);
				return statusReport;
			}
		}
		statusReport.setStatus(Status.VOTE_COMMIT);
		//to handle participant itself crash
		participantLog.put(transaction.getTransactionId()+"VOTE_COMMIT", transaction);
		try {
			FileOutputStream fos = new FileOutputStream("logs/participantLog"+this.participantName+".txt");
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(participantLog);
			oos.close();
			fos.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//to handle coordinator crash
		votedTransaction = true;
		//this.CheckCoordinatorVote();
		System.out.println("Voted VOTE_COMMIT for transaction "+transaction.getTransactionId());
		return statusReport;
	}
	//to check coordinator crash
	public void CheckCoordinatorVote(){
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(votedTransaction){

		}

	}

	@Override
	public StatusReport doCommit(Transaction transaction) throws SystemException, TException {
		System.out.println("doCommit called in participant for transaction "+transaction.getTransactionId());
		votedTransaction = false;
		//to handle participant itself crash
		participantLog.put(transaction.getTransactionId()+"DO_COMMIT", transaction);
		try {
			FileOutputStream fos = new FileOutputStream("logs/participantLog"+this.participantName+".txt");
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(participantLog);
			oos.close();
			fos.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		StatusReport statusReport = new StatusReport();
		statusReport = this.writeFile(transaction.getRFile());
		return statusReport;
	}

	@Override
	public void doAbort(Transaction transaction) throws SystemException, TException {
		System.out.println("doAbort called in participant");
		//to handle participant itself crash
		participantLog.put(transaction.getTransactionId()+"DO_ABORT", transaction);
		try {
			FileOutputStream fos = new FileOutputStream("logs/participantLog"+this.participantName+".txt");
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(participantLog);
			oos.close();
			fos.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}

	@Override
	public StatusReport checkDecision(Transaction transaction) throws SystemException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StatusReport isAborted(Transaction transaction) throws SystemException, TException {
		StatusReport statusReport = new StatusReport();
		if(participantLog.containsKey(transaction.getTransactionId()+"VOTE_COMMIT") || tempParticipantLog.containsKey(transaction.getTransactionId()+"VOTE_COMMIT")){
			statusReport.setStatus(Status.COMMIT);
		}
		else{
			statusReport.setStatus(Status.ABORT);
		}
		System.out.println("returned status as ABORT for transaction "+transaction.getTransactionId());
		return statusReport;
	}

}
