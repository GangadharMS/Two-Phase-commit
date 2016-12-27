import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class CoordinatorHandler implements FileStore.Iface {

	private Vector<Participant> participantList = new Vector<Participant>();
	private int transactionId = 0;
	CopyOnWriteArrayList<Participant> votedYesParticipantList = new CopyOnWriteArrayList<Participant>();
	private Hashtable<Integer,Transaction> coordinatorFinalDecLogs = new Hashtable<Integer,Transaction>();
	private Hashtable<Integer,Transaction> commitedTransactions = new Hashtable<Integer,Transaction>();
	private Hashtable<Integer,Transaction> arrivedTransactions = new Hashtable<Integer,Transaction>();
	private Hashtable<String,Transaction> coordinatorLogs = new Hashtable<String,Transaction>();

	//	public CoordinatorHandler() {
	//		
	//	}


	public int getTransactionId() {
		return transactionId;
	}

	public synchronized void setTransactionId(int transactionId) {
		this.transactionId = transactionId;
	}

	public CoordinatorHandler(Vector<Participant> participantsListIN) throws SystemException, TException {
		this.participantList = participantsListIN;


		FileInputStream fin;
		try {
			//System.out.println("constructor called");
			//to load all the files from persistent storage
			Hashtable<Integer,Transaction> tempCoordinatorFinalDecLogs = new Hashtable<Integer,Transaction>();
			Hashtable<Integer,Transaction> tempCommitedTransactionsLogs = new Hashtable<Integer,Transaction>();
			Hashtable<Integer,Transaction> tempArrivedTransactionsLogs = new Hashtable<Integer,Transaction>();
			if(new File("logs/coordinatorFinalDecLogs.txt").exists()){//BackUp
				fin = new FileInputStream("logs/coordinatorFinalDecLogs.txt");//BackUp
				ObjectInputStream oin = new ObjectInputStream(fin);
				tempCoordinatorFinalDecLogs = (Hashtable<Integer, Transaction>) oin.readObject();
				oin.close();
				fin.close();
			}

			if(new File("logs/arrivedTransactionsLogs.txt").exists()){//BackUp
				fin = new FileInputStream("logs/arrivedTransactionsLogs.txt");//BackUp
				ObjectInputStream oin = new ObjectInputStream(fin);
				tempArrivedTransactionsLogs = (Hashtable<Integer, Transaction>) oin.readObject();
				oin.close();
				fin.close();
			}

			if(new File("logs/commitedTransactionsLogs.txt").exists()){//BackUp
				fin = new FileInputStream("logs/commitedTransactionsLogs.txt");//BackUp
				ObjectInputStream oin = new ObjectInputStream(fin);
				tempCommitedTransactionsLogs = (Hashtable<Integer, Transaction>) oin.readObject();
				oin.close();
				fin.close();
			}

			Hashtable<String,Transaction> tempCoordinatorLogs = new Hashtable<String,Transaction>();
			if(new File("logs/coordinatorLogs.txt").exists()){//BackUp
				fin = new FileInputStream("logs/coordinatorLogs.txt");
				ObjectInputStream oin = new ObjectInputStream(fin);
				tempCoordinatorLogs = (Hashtable<String, Transaction>) oin.readObject();
				oin.close();
				fin.close();
				for(String transParticipant:tempCoordinatorLogs.keySet()){
					Transaction tempTransaction = tempCoordinatorLogs.get(transParticipant);
					if(tempCoordinatorFinalDecLogs.containsKey(tempTransaction.getTransactionId())){
						//final decision is made.
						//System.out.println("final decision is made");

						//check whether transaction is committed after final decision
						if(tempCommitedTransactionsLogs.containsKey(tempTransaction.getTransactionId())){
							//System.out.println("transaction "+tempTransaction.getTransactionId()+" is already committed");
							//do nothing
						}
						else{
							//ask all participants to commit the transaction.
							System.out.println("Ask participants to commit for"
									+ " transaction "+tempTransaction.getTransactionId());
							this.doCommit(tempTransaction);
							tempCommitedTransactionsLogs.put(tempTransaction.getTransactionId(), tempTransaction);
						}

					}
					else{
						//conduct voting again for the transaction
						System.out.println("conduct voting again");
						//
						this.writeFile(tempTransaction.getRFile());
						tempCoordinatorFinalDecLogs.put(tempTransaction.getTransactionId(),
								tempTransaction);
					}
				}

			}
			
			
			//for test case 3
			for(int transactionId:tempArrivedTransactionsLogs.keySet()){
				Transaction tempTransaction = tempArrivedTransactionsLogs.get(transactionId);
				if(tempCoordinatorFinalDecLogs.containsKey(transactionId)){
					//do nothing
				}
				else{
					//start the voting
					//this.writeFile(tempTransaction.getRFile());
					StatusReport statusReport = new StatusReport();
					for(Participant participant:participantList){
						if(participant.getName().equals("coordinator")){
							continue;
						}
						TTransport transport = null;
						transport = new TSocket(participant.getIp(), participant.getPort());
						transport.open();
						TProtocol protocol = new TBinaryProtocol(transport);
						FileStore.Client client = new FileStore.Client(protocol);
						statusReport = client.isAborted(tempTransaction);	
					}
					if(statusReport.status.equals(Status.ABORT)){
						System.out.println("After timeout, operation is aborted for transaction "+transactionId);
						this.setTransactionId(transactionId);
					}
					else{
						System.out.println("Operation is not aborted");
						//do nothing
					}
					//tempCoordinatorFinalDecLogs.put(tempTransaction.getTransactionId(), tempTransaction);
					//tempCommitedTransactionsLogs.put(tempTransaction.getTransactionId(), tempTransaction);
				}
			}
			
		}
		catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}

	@Override
	public StatusReport writeFile(RFile rFile)  throws SystemException, TException{ 
		synchronized(this){
		//System.out.println("writeFile called in coordinator");
		TTransport transport = null;
		Transaction transaction = new Transaction();
		transaction.setRFile(rFile);
		transaction.setOperation("write");
		if(rFile.getTransactionid()==0){
			this.setTransactionId(this.getTransactionId()+1);
			transaction.setTransactionId(this.getTransactionId());
			rFile.setTransactionid(transaction.getTransactionId());
		}
		else{
			transaction.setTransactionId(rFile.transactionid);
			this.setTransactionId(rFile.transactionid);
		}
		transaction.setParticipants(this.participantList);
		System.out.println("transaction "+transaction.getTransactionId()+" arrived.");
		arrivedTransactions.put(this.getTransactionId(), transaction);
		//save arrived transactions to persistent storage
		try {
			FileOutputStream fos = new FileOutputStream("logs/arrivedTransactionsLogs.txt");
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(arrivedTransactions);
			oos.close();
			fos.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			Thread.sleep(6000);
		} catch (InterruptedException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}

		StatusReport statusReport = new StatusReport();
		
		votedYesParticipantList.clear();
		
		System.out.println("voting started");
		//Hashtable<Integer,Transaction> voteReport = new Hashtable<Integer,Transaction>();
		//for(Participant participant:participantList)
		for(int i = 0;i<participantList.size();i++){
			Participant participant = participantList.get(i);
			if(participant.getName().equals("coordinator")){
				continue;
			}
			try{
				transport = new TSocket(participant.getIp(), participant.getPort());
				transport.open();
				TProtocol protocol = new TBinaryProtocol(transport);
				FileStore.Client client = new FileStore.Client(protocol);
				statusReport = client.canCommit(transaction);
			}
			catch (Exception e) {
				
				statusReport.setStatus(Status.VOTE_ABORT);
				
//				System.out.println("One of the communicating branch went down. I am moving to wait state.");
//				try {
//					Thread.sleep(2000);
//					i = i-1;
//					continue;
//				} catch (InterruptedException e1) {
//					// TODO Auto-generated catch block
//					e1.printStackTrace();
//				}
				//	e.printStackTrace();
			}
			//				catch(TTransportException e){
			//					System.out.println("Provided port number is in use. Please provide different port number");
			//					System.exit(0);
			//				}

			if(statusReport.getStatus().equals(Status.VOTE_COMMIT)){
				votedYesParticipantList.add(participant);
				System.out.println(participant.getName() +" voted VOTE_COMMIT for transaction "+transaction.getTransactionId());
			}
			//to recover from coordinator crash, test case 4
			coordinatorLogs.put(transaction.getTransactionId()+participant.getName(),
					transaction);
			try {
				FileOutputStream fos = new FileOutputStream("logs/coordinatorLogs.txt");
				ObjectOutputStream oos = new ObjectOutputStream(fos);
				oos.writeObject(coordinatorLogs);
				oos.close();
				fos.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			if(statusReport.getStatus().equals(Status.VOTE_ABORT)){
				this.doAbort(transaction);
				statusReport.setStatus(Status.FAILED);
				System.out.println(participant.getName() +" voted VOTE_ABORT for transaction "+transaction.getTransactionId());
				return statusReport;
			}
			transport.close();
			try {
				Thread.sleep(8000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		//ask vote for itself
		this.canCommit(transaction);
		System.out.println("Final decision is made as COMMIT for transaction "+transaction.getTransactionId());

		transaction.setStatus(Status.COMMIT);

		coordinatorFinalDecLogs.put(transaction.getTransactionId(), transaction);

		//System.out.println("CHECK !!!!! "+coordinatorFinalDecLogs.get(transaction.getTransactionId()).getStatus());

		try {
			FileOutputStream fos = new FileOutputStream("logs/coordinatorFinalDecLogs.txt");
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(coordinatorFinalDecLogs);
			oos.close();
			fos.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		statusReport = this.doCommit(transaction);
		return statusReport;

			}
	}

	@Override
	public RFile readFile(String filename, String owner) throws SystemException, TException {
		System.out.println("readFIle for file name "+filename);
		TTransport transport = null;
		RFile rFile = null;
		int prevRandomNumber = 0;
		//this.setTransactionId(this.getTransactionId()+1);
		int count = 1;
		while(count < this.participantList.size()){
			int randomNumber = count;
			//Random random = new Random();
//			int randomNumber = random.nextInt(participantList.size()) + 0;
//			if (randomNumber == 0){
//				randomNumber = 1;
//			}
//			if(randomNumber == prevRandomNumber){
//				if(randomNumber == this.participantList.size()-1){
//					randomNumber =-1;
//				}else{
//					randomNumber =+1;
//				}
//			}
//			prevRandomNumber = randomNumber;
			try{
				transport = new TSocket(participantList.get(randomNumber).getIp(), participantList.get(randomNumber).getPort());
				transport.open();
				TProtocol protocol = new TBinaryProtocol(transport);
				FileStore.Client client = new FileStore.Client(protocol);
				//TODO use statusReport
				rFile = client.readFile(filename, owner);
//				if(rFile.getContent() == null){
//					//System.out.println("F D E");
//					SystemException systemException = new SystemException();
//					systemException.setMessage("File doesn't exist");
//					throw new SystemException(systemException);
//				}
			}
				catch(TTransportException e){
					count++;
					continue;
				}
			catch(SystemException e){
				throw e;
			}
				return rFile;
		}
		
		SystemException systemException = new SystemException();
		systemException.setMessage("Participants not available");
		throw systemException;
		
	}

	@Override
	public StatusReport canCommit(Transaction transaction) throws SystemException, TException {
		//System.out.println("canCommit called in coordinator");
		StatusReport statusReport = new StatusReport();
		statusReport.setStatus(Status.VOTE_COMMIT);
		return statusReport;
	}

	@Override
	public StatusReport doCommit(Transaction transaction) throws SystemException, TException {
		//System.out.println("doCommit is called in coordinator "+transaction.getTransactionId());
		TTransport transport = null;
		StatusReport statusReport = new StatusReport();
		Hashtable<Participant,Transaction> failedCommits = new Hashtable<Participant,Transaction>();
		if(transaction.getRFile().getTransactionid()!=0){
			this.setTransactionId(transaction.getRFile().getTransactionid());
		}
		commitedTransactions.put(transaction.getTransactionId(), transaction);
		try {
			FileOutputStream fos = new FileOutputStream("logs/commitedTransactionsLogs.txt");
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(commitedTransactions);
			oos.close();
			fos.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//for(Participant participant:participantList)
		Participant participant;
		for(int i=0;i<participantList.size();i++){
			participant = participantList.get(i);
			if(participant.getName().equals("coordinator")){
				continue;
			}
			try{
				transport = new TSocket(participant.getIp(), participant.getPort());
				transport.open();
				TProtocol protocol = new TBinaryProtocol(transport);
				FileStore.Client client = new FileStore.Client(protocol);
				//TODO use statusReport
				statusReport = client.doCommit(transaction);
			}
			catch(Exception e){
				failedCommits.put(participant, transaction);
				continue;
//				System.out.println("One of the participant is down, however decision is sent to other and waiting.");
//				try {
//					Thread.sleep(2000);
//					i = i-1;
//					continue;
//				} catch (InterruptedException e1) {
//					// TODO Auto-generated catch block
//					e1.printStackTrace();
//				}
			}

		}
		
		
		//logs to help coordinator crash
		//statusReport.setStatus(Status.FAILED);
		//System.out.println("COMMIT is sent to all participants");





		//		coordinatorFinalDecLogs.put(transaction.getTransactionId(), transaction);
		//		try {
		//			FileOutputStream fos = new FileOutputStream("logs/coordinatorFinalDecLogs.txt");
		//			ObjectOutputStream oos = new ObjectOutputStream(fos);
		//			oos.writeObject(coordinatorFinalDecLogs);
		//			oos.close();
		//			fos.close();
		//		} catch (IOException e) {
		//			// TODO Auto-generated catch block
		//			e.printStackTrace();
		//		}
		return statusReport;
	}

	@Override
	public void doAbort(Transaction transaction) throws SystemException, TException {
		//System.out.println("doAbort called in coordinator");
		System.out.println("Final decision is made as ABORT for transaction "+transaction.getTransactionId());
		TTransport transport = null;
		for(Participant participant:votedYesParticipantList){
			//System.out.println(participant.getIp()+","+participant.getPort());
			transport = new TSocket(participant.getIp(), participant.getPort());
			transport.open();
			TProtocol protocol = new TBinaryProtocol(transport);
			FileStore.Client client = new FileStore.Client(protocol);
			client.doAbort(transaction);
		}
		transaction.setStatus(Status.ABORT);
		coordinatorFinalDecLogs.put(transaction.getTransactionId(), transaction);
		try {
			FileOutputStream fos = new FileOutputStream("logs/coordinatorFinalDecLogs.txt");
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(coordinatorFinalDecLogs);
			oos.close();
			fos.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}
	}

	//participants contacts this method to check the final decision made on a particular transaction
	@Override
	public StatusReport checkDecision(Transaction transaction) throws SystemException, TException {
		//		System.out.println("checkDecision called in coordinator");
		StatusReport statusReport = new StatusReport();
		if(coordinatorFinalDecLogs.containsKey(transaction.getTransactionId())){
			//System.out.println("transaction id "+transaction.getTransactionId()+" present in logs");
			statusReport.setStatus(coordinatorFinalDecLogs.get(transaction.getTransactionId()).getStatus());
			//System.out.println("Sent status is "+coordinatorFinalDecLogs.get(transaction.getTransactionId()).getStatus());
		}
		else{
			//System.out.println("Status not found at coordinator");
		}
		return statusReport;
		//statusReport.setStatus(Status.COMMIT);
		//return statusReport;

	}

	@Override
	public StatusReport isAborted(Transaction transaction) throws SystemException, TException {
		// TODO Auto-generated method stub
		return null;
	}

}
