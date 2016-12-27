import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;


import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

class TClass implements Runnable{
	List<String> fileNameList = new ArrayList<String>();
	String coOrdinatorIp = null;
	String coOrdinatorPort = null;
	String operation = null;
	List<String> listOfOperations = new ArrayList<String>();
	int randomNumber = 0;
	String fileName = null;
	String owner = null;
	int threadId = 0;
	TClass(List<String> fileNameList, List<String> listOfOperations,String coOrdinatorIp,String coOrdinatorPort,int threadId){
		this.fileNameList = fileNameList;
		this.coOrdinatorIp = coOrdinatorIp;
		this.coOrdinatorPort = coOrdinatorPort;
		this.threadId = threadId;
		this.listOfOperations = listOfOperations;
	}
	@Override
	public void run() {
		TJSONProtocol tjsonProtocol  = new TJSONProtocol(new TIOStreamTransport(System.out));	

			System.out.println("Client called threadId "+threadId);			 
			
			while(true){
				try{
				TTransport transport;
				transport = new TSocket(coOrdinatorIp, Integer.parseInt(coOrdinatorPort));
				transport.open();
				TProtocol protocol = new TBinaryProtocol(transport);//TJSONProtocol(transport);
				FileStore.Client client = new FileStore.Client(protocol);
				//if(fileNameList.size() > 1){
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				//}

				Random random = new Random();
				randomNumber = random.nextInt(listOfOperations.size()) + 0;
				operation = listOfOperations.get(randomNumber);
				//operation = "write";
				System.out.println("operation is "+operation+" threadId "+threadId);
				
					if(operation.equalsIgnoreCase("write")){
						RFile rFile = new RFile();
						BufferedReader reader = null;
						String line;
						StringBuilder fileContentBuilder = new StringBuilder("");
						try {
							randomNumber = random.nextInt(fileNameList.size()) + 0;
							fileName = fileNameList.get(randomNumber);
							//fileName = "test.txt";
							System.out.println("fileName is "+fileName+" threadId is "+threadId);
							if(!(new File(fileName).exists())){
								System.out.println("Specified file doesn't exist at client end");
								System.exit(0);
							}
							reader = new BufferedReader(new FileReader(new File(fileName)));
							while((line = reader.readLine()) != null){
								fileContentBuilder.append(line);
							}
						} catch(IOException e){
							//e.printStackTrace();
							throw e;
						} finally{
							try {
								reader.close();
							} catch (IOException e) {
								// TODO Auto-generated catch block
								//e.printStackTrace();
								throw e;
							}
						}
						rFile.setContent(fileContentBuilder.toString());
						rFile.setFilename(fileName);
						StatusReport statusReport = client.writeFile(rFile);
						//statusReport.write(tjsonProtocol);
						System.out.println(statusReport);
					}
					else{
						randomNumber = random.nextInt(fileNameList.size()) + 0;
						fileName = fileNameList.get(randomNumber);
					//	System.out.println();
						RFile rFile = client.readFile(fileName, owner);
						rFile.write(tjsonProtocol);
						//System.out.println(rFile);
					}
					//transport.close();

				}
				catch(SystemException e){
					System.out.println(e);
					continue;
				}
				catch(Exception e){
					//e.printStackTrace();
					System.out.println("Coordinator went down, going to wait state. Will start communicating "
							+ "once coordinator is up");
					try {
						Thread.sleep(5000);
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					continue;

				}


			}

	}

}


public class FileHandlingClient {

	public static void main(String[] args){
		List<String> fileNameList = new ArrayList<String>();
		List<String> listOfOperations = new ArrayList<String>();
		Scanner inputSource = null;
		if(args.length!=5){
			System.out.println("Please provide coorinator ip, port, filenameList file name and operationsList fileName and number of threads.");
			System.exit(0);
		}
		String contentNamesFile = args[2];
		String operationNamesFile = args[3];
		int numberOfThreads = Integer.parseInt(args[4]);
		try{
			inputSource = new Scanner(new File(contentNamesFile));
			while(true){
				if(!inputSource.hasNext()){
					break;
				}
				String eachLine = inputSource.nextLine();
				fileNameList.add(eachLine);
			}
			inputSource.close();
			inputSource = new Scanner(new File(operationNamesFile));
			while(true){
				if(!inputSource.hasNext()){
					break;
				}
				String eachLine = inputSource.nextLine();
				listOfOperations.add(eachLine);
			}	
			inputSource.close();
		}
		catch(NumberFormatException e){
			System.out.println("Please provide coorinator ip, port, filenameList file name and operationsList fileName in order");
			System.exit(0);
		} catch (FileNotFoundException e) {
			System.out.println("one of the file not found, please check.");
			System.exit(0);
		}

		//		fileNameList.add("test.txt");
		//		fileNameList.add("test1.txt");
		//		fileNameList.add("test2.txt");
		int threadId = 0;
		for(int i =0;i<numberOfThreads;i++){
			TClass threadClass = new TClass(fileNameList,listOfOperations, args[0], args[1],++threadId);
			Thread fileHandlingThread = new Thread(threadClass);
			fileHandlingThread.start();
		}

	}
}
