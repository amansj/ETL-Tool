
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.ObjectInputStream;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.mutable.MutableInt;
//import org.apache.commons.lang.mutable.MutableInt;
import org.apache.log4j.Logger;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

public class Main {
	private static  String SQL="";
	private static final String FILE_NAME="doc/testfile.csv";
	private static final String TABLE_NAME="test";
	private static final String DESC_TABLE_NAME="desctest";
	private static final int NO_OF_CORES=2;
	final static Logger logger = Logger.getLogger("Global Logger");
	private static HashMap<String,Integer> tableMetaData()
	{
		HashMap<String,Integer> tableMeta=new HashMap<String,Integer>();
		Connection con=C3P0DataSource.getInstance().getConnection();
		try {
			PreparedStatement pStatement=con.prepareStatement("select * from "+TABLE_NAME);
			ResultSetMetaData rsmd=pStatement.getMetaData();
			SQL="insert into "+TABLE_NAME+" values(";
			for(int i=1;i<=rsmd.getColumnCount();i++)
			{	
				if(i>1)
				{
					SQL+=",";
				}
				SQL+="?";
				tableMeta.put(rsmd.getColumnName(i),i);
			}
			
			SQL+=")";
			
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			logger.error(e1.toString());
			
		}
		return tableMeta;
	}
	private static HashMap<String,HashMap<String,String>> tableDescription()
	{
		HashMap<String,HashMap<String,String>> tableMappingDesc=new HashMap<String,HashMap<String,String>>();
		Connection con=C3P0DataSource.getInstance().getConnection();
		try {
			String query="select * from "+DESC_TABLE_NAME;
			PreparedStatement pStatement=con.prepareStatement(query);
			ResultSet rs=pStatement.executeQuery();
			HashMap<String,String> dbDesc=null;
			while(rs.next())
			{	
				if(tableMappingDesc.containsKey(rs.getString(2)))
				{
					dbDesc=tableMappingDesc.get(rs.getString(2));
					
				}
				else
				{
					dbDesc=new HashMap<String,String>();
				}
				dbDesc.put(rs.getString(1),rs.getString(3));
				tableMappingDesc.put(rs.getString(2),dbDesc);
			}
			
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			logger.error(e1.toString());
			
		}
		finally{
			try {
				if(con!=null)
				{
					con.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.error(e.toString());
				
			}	
		}
		return tableMappingDesc;
	}
	private static HashMap<String,Integer> csvHeader()
	{
		HashMap<String,Integer> header=new HashMap<String,Integer>();
		CSVReader csvReader=null;
		Reader reader;
		try {
			reader = Files.newBufferedReader(Paths.get(FILE_NAME));
			csvReader =new CSVReaderBuilder(reader).build();
			String[] data;
			data=csvReader.readNext();
			for(int j=0;j<data.length;j++)
			{
				header.put(data[j], j);
			}
			csvReader.close();
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error(e.toString());
		}		 		    
			
		return header;
	}
	private static void pageCalculations(MutableInt numOfRecord,MutableInt numPages,MutableInt remainingRecord,int numOfThread,File file) {
		numOfRecord.setValue(countLineNumber(file));
		numPages.setValue((numOfRecord.intValue())/numOfThread);
		remainingRecord.setValue((numOfRecord.intValue())-numOfThread*(numPages.intValue()));	
	}
	private static int countLineNumber(File file)
	{
		int lines = 0;
		try {
			LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(file));
			lineNumberReader.skip(Long.MAX_VALUE);
	        lines = lineNumberReader.getLineNumber();
	      	lineNumberReader.close();
		}catch (IOException e) {
	        logger.error(e.toString());
	    }
		return lines-1;
	}
	
	@SuppressWarnings("unchecked")
	private static HashMap<Long, int[]> deSerialize() {
		HashMap<Long, int[]> prevThreadStatus=null;
		File file=new File("ser_files/write_record.ser");
		if(file.exists())
		{
			try {
				FileInputStream fileIn = new FileInputStream("ser_files/write_record.ser");
				ObjectInputStream in = new ObjectInputStream(fileIn);
				prevThreadStatus = (HashMap<Long, int[]>) in.readObject();
				in.close();
				fileIn.close();
				
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				logger.error(e.toString());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				logger.error(e.toString());
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				logger.error(e.toString());
			}
			
		}
		return prevThreadStatus;
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		HashMap<Long, int[]> prevThreadStatus=deSerialize();
		HashMap<String,Integer> tableMetaData=tableMetaData();
		HashMap<String,Integer> header=csvHeader();
		HashMap<String,HashMap<String,String>> tableMappingDesc=tableDescription();
		HashMap<Long,int[]> threadStatus=new HashMap<Long,int[]>();
		ExecutorService execService=Executors.newFixedThreadPool(NO_OF_CORES);
		int start,end;
		Instant starttime = Instant.now();
		if(prevThreadStatus!=null)
		{
			for(Map.Entry<Long,int[]> entry:prevThreadStatus.entrySet()){    
		        int[] b=entry.getValue();  
		        if(b[2]<b[1])
		        {
		        	start=b[2]+1;
		        	end=b[1];
		        	WriterThread thread=new WriterThread(FILE_NAME,SQL,header,tableMappingDesc,tableMetaData);
					threadStatus.put((long)(thread.hashCode()),new int[] {start,end,start-1});
					thread.setIndex(start, end,threadStatus);
					execService.execute(thread);
		        }
		    }
		}
		else
		{
			System.out.print("Enter Number of Thread:");
			Scanner sc=new Scanner(System.in);
			int numOfThread=sc.nextInt();
			sc.close();
			File file=new File(FILE_NAME);
			MutableInt numRecords = new MutableInt();
			MutableInt numPage=new MutableInt();
			MutableInt remRecords=new MutableInt();
			pageCalculations(numRecords,numPage,remRecords,numOfThread,file);
			int numOfRecord=numRecords.intValue();
			int numPages=numPage.intValue();
			int remainingRecord=remRecords.intValue();
			int i;
			for(i=0;i<numOfThread;i++)
			{
				start=i*numPages+1;
				end=start+numPages-1;
				WriterThread thread=new WriterThread(FILE_NAME,SQL,header,tableMappingDesc,tableMetaData);
				threadStatus.put((long)(thread.hashCode()),new int[] {start,end,start-1});
				thread.setIndex(start, end,threadStatus);
				execService.execute(thread);
			}
			while(remainingRecord!=0)
			{
				start=numOfRecord-remainingRecord+1;
				end=numOfRecord-remainingRecord+1;
				WriterThread thread=new WriterThread(FILE_NAME,SQL,header,tableMappingDesc,tableMetaData);
				threadStatus.put((long)(thread.hashCode()),new int[] {start,end,start});
				thread.setIndex(start, end,threadStatus);
				execService.execute(thread);
				remainingRecord--;
			}
		}	
		execService.shutdown();  
		while (!execService.isTerminated()) {   } 
		boolean status=true;
		for(Map.Entry<Long, int[]> entry:threadStatus.entrySet()){    
	        int[] b=entry.getValue();  
	        if(b[2]<b[1])
	        {
	        	status=false;
	        	break;
	        }
	    }
		if(status)
		{
			try {
				if(Files.deleteIfExists(Paths.get("ser_files/write_record.ser")))
				{
					logger.info("Ser File Deleted");
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				logger.error(e.toString());
			}
		}
		Instant endtime = Instant.now();
		logger.info(Duration.between(starttime, endtime));
		System.out.println(Duration.between(starttime, endtime));
	}
}
