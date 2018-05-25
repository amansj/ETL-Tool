import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.sql.BatchUpdateException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import javax.imageio.ImageIO;

import org.apache.log4j.Logger;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
public class WriterThread implements Runnable {
	String fileName;
	private final String DECIMAL_REGEX="[-+]?[0-9]*\\.?[0-9]+";
	long threadHashCode;
	String sql;
	HashMap<String,Integer> header;
	HashMap<String,Integer> tableMetaData;
	HashMap<String,HashMap<String,String>> tableMappingDesc;
	HashMap<Long, int[]> threadStatus;
	int start,end;
	final Logger logger = Logger.getLogger("Global Logger");
	private final int BATCH_SIZE=50;
	private synchronized String formulaSubstitution(String formula,int index)
	{
		String result="";
		int currentIndexOpen=formula.indexOf("(",index+1);
		int currentIndexClose=formula.indexOf(")", index+1);
		if(currentIndexOpen>currentIndexClose||currentIndexOpen==-1)
		{
			result=formula.substring(index+1, currentIndexClose);
		}
		else
		{
			result=formulaSubstitution(formula,currentIndexOpen);
		}
		return result;
	}
	private synchronized String formulaeEvaluation(String[] data,String formula,String dataType,String scale)
	{
		String result="";
		int count_=0;
		int count__=0;
		for(int i=0;i<formula.length();i++)
		{
		    if(formula.indexOf("(", i)!=-1)
		    {
		    	count_++;
		    }
		    else
		    {
		    	break;
		    }
		}
		for(int i=0;i<formula.length();i++)
		{
		    if(formula.indexOf("(", i)!=-1)
		    {
		    	count__++;
		    }
		    else
		    {
		    	break;
		    }
		}
		if(count_!=count__)
			return "Invalid Formula";
		if(dataType.equals("Decimal"))
		{
			while(formula.contains("("))
			{
				int start=formula.indexOf("(");
				String formulatoEvaluate=formulaSubstitution(formula,start);
				result=formulaEvaluation(data,formulatoEvaluate,scale);
				
				formula=formula.replace("("+formulatoEvaluate+")",":"+result);
			}
			formula=formulaEvaluation(data,formula,scale);
			result=formula;
			
		}
		if(dataType.equals("String"))
		{
			String operand1="";
			String operand2="";
			String data1="";
			String data2="";
			String operator="+";
			formula=formula.replace("\" \"", " ");
			formula=formula.replace("\"", "");
			while(formula.contains(operator))
			{
				int op=formula.indexOf(operator);
				operand1=prevcolname(formula,op);
				operand2=nextcolname(formula,op);
				if(!header.containsKey(operand1))
				{
					data1=operand1;
				}
				else
				{
					data1=data[header.get(operand1)];
				}
				if(!header.containsKey(operand2))
				{
					data2=operand2;
				}
				else
				{
					data2=data[header.get(operand2)];
				}
				result=data1+data2;
				formula=formula.replace(":"+operand1+operator+":"+operand2, ":"+result);
			}
			formula=formula.substring(1, formula.length());
			if(header.containsKey(formula))
			{
				formula=data[header.get(formula)];
			}
			result=formula;
		}
		
		return result;
	}
	private synchronized String Operation(String operand1,String operand2,String operator,String scale)
	{
		BigDecimal op1=new BigDecimal(operand1);
		BigDecimal op2=new BigDecimal(operand2);
		String result=null;
		if(operator.equals("+"))
		{
			result=op1.add(op2).setScale(Integer.parseInt(scale), RoundingMode.HALF_EVEN).toString();
		}
		else if(operator.equals("-"))
		{
			result=op1.subtract(op2).setScale(Integer.parseInt(scale), RoundingMode.HALF_EVEN).toString();
		}
		else if(operator.equals("*"))
		{
			result=op1.multiply(op2).setScale(Integer.parseInt(scale), RoundingMode.HALF_EVEN).toString();
		}
		else if(operator.equals("/"))
		{
			result=op1.divide(op2,Integer.parseInt(scale), RoundingMode.HALF_EVEN).toString();
		}
		/*switch(operator)
		{
		case "+":
			result=op1.add(op2).setScale(Integer.parseInt(scale), RoundingMode.HALF_EVEN).toString();
			break;
		case "-":
			result=op1.subtract(op2).setScale(Integer.parseInt(scale), RoundingMode.HALF_EVEN).toString();
			break;
		case "*":
			result=op1.multiply(op2).setScale(Integer.parseInt(scale), RoundingMode.HALF_EVEN).toString();
			break;
		case "/":
			result=op1.divide(op2,Integer.parseInt(scale), RoundingMode.HALF_EVEN).toString();
			break;
		}*/
		return result;
	}
	private synchronized String OperationResult(String[] data,String formula,String operator,String scale)
	{
		String operand1="";
		String operand2="";
		String data1="";
		String data2="";
		String result="";
		while(formula.contains(operator))
		{
			int op=formula.indexOf(operator);
			operand1=prevcolname(formula,op);
			operand2=nextcolname(formula,op);
			if(Pattern.matches(DECIMAL_REGEX,operand1))
			{
				data1=operand1;
			}
			else
			{
				data1=data[header.get(operand1)];
			}
			if(Pattern.matches(DECIMAL_REGEX,operand2))
			{
				data2=operand2;
			}
			else
			{
				data2=data[header.get(operand2)];
			}
			if(!Pattern.matches(DECIMAL_REGEX,data1))
			{
				return "NAN";
			}
			if(!Pattern.matches(DECIMAL_REGEX,data2))
			{
				return "NAN";
			}
			result=Operation(data1, data2, operator,scale);
			formula=formula.replace(":"+operand1+operator+":"+operand2, ":"+result);
		}
		return formula;
	}
	private synchronized String formulaEvaluation(String[] data,String formula,String scale)
	{
		String operator="/";
		formula=OperationResult(data, formula, operator,scale);
		operator="*";
		formula=OperationResult(data, formula, operator,scale);
		operator="+";
		formula=OperationResult(data, formula, operator,scale);
		operator="-";
		formula=OperationResult(data, formula, operator,scale);
		if(formula.equals("NAN"))
		{
			return formula;
		}
		formula=formula.substring(1, formula.length());
		if(!Pattern.matches(DECIMAL_REGEX, formula))
		{
			formula=data[header.get(formula)];
		}
		return formula;
	}
	private synchronized String prevcolname(String exp,int op)
	  {
		  String[] data=exp.substring(0, op).split(":");
		  return data[data.length-1];
	  }
	private synchronized String nextcolname(String exp,int op)
	  {
		  int r=exp.indexOf(":", op+1);
		  String sub="";
		  if(r==-1)
		  {
			  sub=exp.substring(op+1, r-2);
		  }
		  else
		  {
			  sub=exp.substring(op+2);
		  }
		  return sub;
	  }
	private synchronized void errorinfo(Connection con,String error,int rownum)
	{
		PreparedStatement ps=null;
		try {
			ps=con.prepareStatement("insert into errortable values(?,?)");
			ps.setInt(1, rownum);
			ps.setString(2, error);
			if(ps.executeUpdate()==1)
			{
				logger.info("Error Msg Inserted");
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			logger.error(e1.toString());
		}
		
	}
	private synchronized void serialize()
	{
		FileOutputStream fileOut;
		try {
			fileOut = new FileOutputStream("ser_files/write_record.ser");
			 ObjectOutputStream out = new ObjectOutputStream(fileOut);
			 out.writeObject(threadStatus);
			 out.close();
			 fileOut.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); 	
		}
	}
	public void run()
	{  
		Connection con=null;
		CSVReader csvReader=null;
		PreparedStatement ps=null;
		FileInputStream input;
		long i=0;
		boolean prevRecordStatus=true;
		long current=start-1;
		Savepoint savingstate=null;
		try {
			input = new FileInputStream(fileName);
			CharsetDecoder decoder=Charset.forName("UTF-8").newDecoder();
			decoder.onMalformedInput(CodingErrorAction.IGNORE);
			Reader reader=new InputStreamReader(input,decoder);		 		    
			csvReader =new CSVReaderBuilder(reader).withSkipLines(start).build();  		 		     
			con=C3P0DataSource.getInstance().getConnection();
			con.setAutoCommit(false);
			con.rollback();
			savingstate=con.setSavepoint("currentsafestate");
			boolean status;
			for(i=start;i<=end;i++)
			{
				status=true;
				String[] data=csvReader.readNext();
				
				if(data.length!=header.size())
				{
					errorinfo(con,"Invalid Data Format",(int) i);
					prevRecordStatus=false;
					continue;
				}
				if((i-start+1)%BATCH_SIZE==1)
				{
					ps=con.prepareStatement(sql);
				}
				else if((i-start)%BATCH_SIZE==1&&!prevRecordStatus)
				{
					ps=con.prepareStatement(sql);
				}
				for(Map.Entry<String, HashMap<String,String>> mapEntry:tableMappingDesc.entrySet())
		    	{
		    		HashMap<String,String> csvHeader=mapEntry.getValue();
		    		if(csvHeader.size()==1)
		    		{
		    			for(Map.Entry<String,String> entry:csvHeader.entrySet())
			    		{
		    				String dataType=entry.getValue();
		    				String[] precision=null;
		    				if(entry.getValue().contains("Decimal"))
		    				{		
		    					precision=dataType.substring(dataType.indexOf("(")+1, dataType.length()-1).split(",");
    							dataType="Decimal";
		    				}
		    				String result="";
			    			DbDataTypeEnum var=DbDataTypeEnum.valueOf(dataType);
			    			switch(var)
			    			{
			    				case Decimal:
			    					try {
			    						result=formulaeEvaluation(data, entry.getKey(),dataType,precision[1]);
			    						if(Pattern.matches(DECIMAL_REGEX, result))
				    					{
			    							ps.setBigDecimal(tableMetaData.get(mapEntry.getKey()), new BigDecimal(result));
				    					}
			    						else
				    					{
				    						status=false;
				    						errorinfo(con,"Cannot parse String to Decimal",(int) i);
				    					}
			    						
									} catch (Exception e) {
										// TODO: handle exception
										status=false;
										errorinfo(con,"Cannot parse String to Decimal",(int) i);
									}
			    					break;
			    				case String:
			    					result=formulaeEvaluation(data, entry.getKey(),dataType,"0");
			    					ps.setString(tableMetaData.get(mapEntry.getKey()), result);
			    					break;
			    				case File:
			    					String formula=entry.getKey();
			    					Blob fileData=null;
			    					if(formula.startsWith("FromPath"))
			    					{
			    						formula=formula.replace("FromPath(", "");
			    						formula=formula.replace(")", "");
			    						result=formulaEvaluation(data, formula,"0");
			    						File dataFile=new File(result);
			    						if(dataFile.exists())
			    						{
			    							byte[] imageInByte;
			    							String extension=result.substring(result.lastIndexOf("."));
			    							BufferedImage originalImage = ImageIO.read(dataFile);
			    							ByteArrayOutputStream baos = new ByteArrayOutputStream();
			    							ImageIO.write(originalImage, extension, baos);
			    							baos.flush();
			    							imageInByte = baos.toByteArray();
			    							baos.close();
			    							fileData=con.createBlob();
			    							fileData.setBytes(1, imageInByte);
			    						}
			    					}
			    					if(formula.startsWith("FromString"))
			    					{
			    						formula=formula.replace("FromString(", "");
			    						formula=formula.replace(")", "");
			    						result=formulaEvaluation(data, formula,"0");
			    						fileData=con.createBlob();
			    						fileData.setBytes(1, result.getBytes());
			    					}
			    					else
			    					{
			    						status=false;
			    						errorinfo(con,"Invalid Formula",(int) i);
			    					}
			    					if(fileData!=null)
			    					{
			    						ps.setBlob(tableMetaData.get(mapEntry.getKey()), fileData);
			    					}
			    					else
			    					{
			    						status=false;
			    						errorinfo(con,"Invalid FilePath",(int) i);
			    					}
			    					break;
			    				case Boolean:
			    					formula=entry.getKey();
			    					if(formula.startsWith("Char"))
			    					{
			    						formula=formula.replace("Char(", "");
			    						formula=formula.replace(")", "");
			    						result=formulaEvaluation(data, formula,"0");
			    						ps.setString(tableMetaData.get(mapEntry.getKey()),(Boolean.parseBoolean(result)?"Y":"N" ));
			    					}
			    					if(formula.startsWith("Int"))
			    					{
			    						formula=formula.replace("Int(", "");
			    						formula=formula.replace(")", "");
			    						result=formulaEvaluation(data, formula,"0");
			    						ps.setInt(tableMetaData.get(mapEntry.getKey()),(Boolean.parseBoolean(result)?1:0 ));
			    					}
			    					break;
			    				default:
			    					errorinfo(con,"Invalid DataType",(int) i);
		    				}
			    			
			    		}
		    		}
		    	}
				if(status)
				{
					ps.addBatch();
				}
				if((i-start+1)%BATCH_SIZE==0)
				{
					int[] update=ps.executeBatch();
					
					for(int k=0;k<update.length;k++)
					{
						logger.info("/****************************************Processed  " +(i-BATCH_SIZE+k+1)+ "th Record********************************************************************************/");	
					}
					con.commit();
					con.releaseSavepoint(savingstate);
					savingstate=con.setSavepoint("savepoint");
					current+=update.length;
					int[] recordStatus=threadStatus.get(threadHashCode);
					recordStatus[2]=(int) current;
					threadStatus.put(threadHashCode, recordStatus);
					serialize();
					ps.close();
				}
    		}
			int[] update=ps.executeBatch();
			for(int k=0;k<update.length;k++)
			{
				logger.info("/****************************************Processed  " +(i-update.length+k+1)+ "th Record********************************************************************************/");
			}
			con.commit();
			con.releaseSavepoint(savingstate);
			savingstate=con.setSavepoint("savepoint");
			current+=update.length;
			int[] recordStatus=threadStatus.get(threadHashCode);
			recordStatus[2]=(int) current;
			threadStatus.put(threadHashCode, recordStatus);
			serialize();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			errorinfo(con,e.getMessage(),(int) i);
    		logger.error(e.toString());
		}
		catch (BatchUpdateException buex) {
			errorinfo(con,buex.getMessage(),(int) i-BATCH_SIZE+1);
    		logger.error(buex.toString());
			try {
				con.rollback(savingstate);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		catch ( SQLException e) {
			// TODO Auto-generated catch block
			errorinfo(con,e.getMessage(),(int) i);
    		logger.error(e.toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			errorinfo(con,e.getMessage(),(int) i);
    		logger.error(e.toString());
		}
		finally {
			try {
				if(ps!=null)
				{
					ps.close();
				}
				if(con!=null)
				{
					con.close();
				}
				if(csvReader!=null)
				{
					csvReader.close();
				}	
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				errorinfo(con,e.getMessage(),(int) i);
	    		logger.error(e.toString());
			}
			catch(IOException e)
			{
				errorinfo(con,e.getMessage(),(int) i);
	    		logger.error(e.toString());
			}
		}
			
	}  
	public void setIndex(int s,int e,HashMap<Long, int[]> threadStatus)
	{
		this.threadStatus=threadStatus;
		start=s;
		end=e;
	}
	WriterThread(String file,String sql,HashMap<String,Integer> header,HashMap<String,HashMap<String,String>> tableMappingDesc,HashMap<String,Integer> tableMetaData)
	{
		this.tableMetaData=tableMetaData;
		this.tableMappingDesc=tableMappingDesc;
		this.header=header;
		threadHashCode=this.hashCode();
		fileName=file;
		this.sql=sql;
	}
}
