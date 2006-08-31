/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2005-05  Javlin Consulting <info@javlinconsulting.cz>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/
package org.jetel.component;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.formatter.DelimitedDataFormatter;
import org.jetel.data.formatter.FixLenDataFormatter;
import org.jetel.data.formatter.Formatter;
import org.jetel.data.parser.DelimitedDataParser;
import org.jetel.data.parser.FixLenDataParser;
import org.jetel.data.parser.Parser;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.SynchronizeUtils;
import org.w3c.dom.Element;

/**
 *  <h3>System Execute Component</h3> 
 *  <!-- This component executes system command.-->
 * 
 *  <table border="1">
 *
 *    <th>
 *      Component:
 *    </th>
 *    <tr><td>
 *        <h4><i>Name:</i> </h4></td><td>SystemExecute</td>
 *    </tr>
 *    <tr><td><h4><i>Category:</i> </h4></td><td></td>
 *    </tr>
 *    <tr><td><h4><i>Description:</i> </h4></td>
 *      <td>
 *  This component executes system command.<br>
 *      </td>
 *    </tr>
 *    <tr><td><h4><i>Inputs:</i> </h4></td>
 *    <td>
 *        [0]- input records for system command<br>
 *    </td></tr>
 *    <tr><td> <h4><i>Outputs:</i> </h4>
 *      </td>
 *      <td>
 *        [0] - output stream from system command
 *      </td></tr>
 *    <tr><td><h4><i>Comment:</i> </h4>
 *      </td>
 *      <td></td>
 *    </tr>
 *  </table>
 *  <br>
 *  <table border="1">
 *    <th>XML attributes:</th>
 *    <tr><td><b>type</b></td><td>SYS_EXECUTE</td></tr>
 *    <tr><td><b>command</b></td><td> command to be execute by system</td></tr>
 *    <tr><td><b>errorLinesNumber</b></td><td> number of lines that are print
 *    	 out if command finishes with errors</td></tr>
 *    </table>
 *    <h4>Example:</h4> <pre>&lt;Node id="SYS_EXECUTE0" type="SYS_EXECUTE"&gt;
 * &lt;attr name="errorLinesNumber"&gt;3&lt;/attr&gt;
 * &lt;attr name="command"&gt;rm test.txt &lt;/attr&gt;
 * &lt;/Node&gt;</pre>
 *
 * @author avackova
 *
 */
public class SystemExecute extends Node{
	
	private static final String XML_COMMAND_ATTRIBUTE = "command";
	private static final String XML_ERROR_LINES_ATTRIBUTE = "errorLinesNumber";
	private static final String XML_OUTPUT_FILE_ATTRIBUTE = "outputFile";
	private static final String XML_APPEND_ATTRIBUTE = "append";
	private static final String XML_INTERPRETER_ATTRIBUTE = "interpreter";
	
	public final static String COMPONENT_TYPE = "SYS_EXECUTE";

	private final static int INPUT_PORT = 0;
	private final static int OUTPUT_PORT = 0;
	
	private final static int ERROR_LINES=2;

	public  final static long KILL_PROCESS_WAIT_TIME = 1000;
	
	private String command;
	private String executeCommand;
	private int errorLinesNumber;
	private FileWriter outputFile = null;
	private boolean append;
	private int exitValue;
	private Parser parser;
	private Formatter formatter;
	private File batch;
	private String interpreter;
	private String outputFileName;
	
	static Log logger = LogFactory.getLog(SystemExecute.class);
	
	public SystemExecute(String id,String interpreter, String command,int errorLinesNumber) {
		super(id);
		this.interpreter = interpreter;
		this.command=command;
		this.errorLinesNumber=errorLinesNumber;
	}
	
	static boolean kill(Thread thread, long millisec){
		if (!thread.isAlive()){
			return true;
		}
		thread.interrupt();
		try {
			Thread.sleep(millisec);
		}catch(InterruptedException ex){
		}
		return !thread.isAlive();
	}
	
	
	static void waitKill(Thread thread, long millisec){
		if (!thread.isAlive()){
			return;
		}
		try{
			Thread.sleep(millisec);
			thread.interrupt();
		}catch(InterruptedException ex){
			//do nothing, just leave
		}
			
	}
	
	
	private String createBatch(String command)throws IOException{
		batch =  File.createTempFile("tmp",".bat");
		FileWriter batchWriter = new FileWriter(batch);
		batchWriter.write(command);
		batchWriter.close();
		return batch.getCanonicalPath();
	}

	@Override public void run() {
		//Creating and initializing record from input port
		DataRecord in_record=null;
		InputPort inPort = getInputPort(INPUT_PORT);
		//If there is input port read metadadata and initialize in_record
		if (inPort!=null) {
			DataRecordMetadata meta=inPort.getMetadata();
			in_record = new DataRecord(meta);
			in_record.init();
			if (meta.getRecType()==DataRecordMetadata.DELIMITED_RECORD) {
				formatter=new DelimitedDataFormatter();
			}else {
				formatter=new FixLenDataFormatter();
			}
		}else{
			formatter=null;
		}
		DataRecord out_record=null;
		//Creating and initializing record to otput port
		OutputPort outPort=getOutputPort(OUTPUT_PORT);
		//If there is output port read metadadata and initialize in_record
		if (outPort!=null) {
			DataRecordMetadata meta=outPort.getMetadata();
			out_record= new DataRecord(meta);
			out_record.init();
			if (meta.getRecType()==DataRecordMetadata.DELIMITED_RECORD) {
				parser=new DelimitedDataParser();
			}else {
				parser=new FixLenDataParser();
			}
		}else{
			parser=null;
		}

		boolean ok = true;
		resultCode = Node.RESULT_ERROR;
		try{
			Process	process= Runtime.getRuntime().exec(executeCommand);
			
			BufferedOutputStream process_in=new BufferedOutputStream(process.getOutputStream());
			BufferedInputStream process_out=new BufferedInputStream(process.getInputStream());
			BufferedInputStream process_err=new BufferedInputStream(process.getErrorStream());
			// If there is input port read records and write them to input stream of the process
			GetData getData=null; 
			if (inPort!=null) {
                formatter.open(process_in,getInputPort(INPUT_PORT).getMetadata());
                getData=new GetData(Thread.currentThread(),inPort, in_record, formatter);
				getData.start();
			}
			//If there is output port read output from process and send it to output ports
			SendData sendData=null;
			SendDataToFile sendDataToFile = null;
			SendDataToFile sendErrToFile = null;
			if (outPort!=null){
                parser.open(process_out, getOutputPort(OUTPUT_PORT).getMetadata());
                sendData=new SendData(Thread.currentThread(),outPort,out_record,parser);
				//send all out_records to output ports
				sendData.start();
			}else if (outputFile!=null){
				sendDataToFile = new SendDataToFile(Thread.currentThread(),outputFile,process_out);
				sendErrToFile = new SendDataToFile(Thread.currentThread(),outputFile, process_err);
				sendDataToFile.start();
				sendErrToFile.start();
			}

			if (sendDataToFile==null ){
				BufferedReader err=new BufferedReader(new InputStreamReader(process_err));
				String line;
				StringBuffer errmes=new StringBuffer();
				int i=0;
				while (((line=err.readLine())!=null)&&i++<Math.max(errorLinesNumber,ERROR_LINES)){
					if (i<=errorLinesNumber)
						logger.warn(line);
					if (i<=ERROR_LINES)
						errmes.append(line+"\n");
				}
				if (ERROR_LINES<i) errmes.append(".......\n");
				err.close();
				process_err.close();
				resultMsg=errmes.toString();
			}            
			
			// wait for executed process to finish
			// wait for SendData and/or GetData threads to finish work
			try{
				exitValue=process.waitFor();
				if (sendData!=null) sendData.join(KILL_PROCESS_WAIT_TIME);
				if (getData!=null) getData.join(KILL_PROCESS_WAIT_TIME);
				if (sendDataToFile!=null){
					sendDataToFile.join(KILL_PROCESS_WAIT_TIME);
					sendErrToFile.join(KILL_PROCESS_WAIT_TIME);
				}
				
			}catch(InterruptedException ex){
				logger.error("InterruptedException in "+this.getId(),ex);
				process.destroy();
				if (getData!=null) {
					if (!kill(getData,KILL_PROCESS_WAIT_TIME)){
						throw new RuntimeException("Can't kill "+getData.getName());
					}
				}
				if (sendData!=null) {
					if (!kill(sendData,KILL_PROCESS_WAIT_TIME)){
						throw new RuntimeException("Can't kill "+sendData.getName());
					}
				}
				if (sendDataToFile!=null) {
					if (!kill(sendDataToFile,KILL_PROCESS_WAIT_TIME)){
						throw new RuntimeException("Can't kill "+sendDataToFile.getName());
					}
					if (!kill(sendErrToFile,KILL_PROCESS_WAIT_TIME)){
						throw new RuntimeException("Can't kill "+sendErrToFile.getName());
					}
				}
			}
			if (outputFile!=null) {
				outputFile.close();
			}
			if (interpreter!=null) {
				batch.delete();
			}
			if (getData!=null){
				if (!kill(getData,KILL_PROCESS_WAIT_TIME)){
					throw new RuntimeException("Can't kill "+getData.getName());
				}
				if (getData.getResultCode()==Node.RESULT_ERROR) {
					ok = false;
					resultMsg = (resultMsg == null ? "" : resultMsg) + getData.getResultMsg() + "\n" + getData.getResultException();
				}
			}
			
			if (sendData!=null){
				if (!kill(sendData,KILL_PROCESS_WAIT_TIME)){
					throw new RuntimeException("Can't kill "+sendData.getName());
				}
				if (sendData.getResultCode()==Node.RESULT_ERROR){
					ok = false;
					resultMsg = (resultMsg == null ? "" : resultMsg) + sendData.getResultMsg() + "\n" + sendData.getResultException();
				}
			}

			if (sendDataToFile!=null){
				if (!kill(sendDataToFile,KILL_PROCESS_WAIT_TIME)){
					throw new RuntimeException("Can't kill "+sendDataToFile.getName());
				}
				if (sendDataToFile.getResultCode()==Node.RESULT_ERROR){
					ok = false;
					resultMsg = (resultMsg == null ? "" : resultMsg) + sendDataToFile.getResultMsg() + "\n" + sendDataToFile.getResultException();
				}
				if (!kill(sendErrToFile,KILL_PROCESS_WAIT_TIME)){
					throw new RuntimeException("Can't kill "+sendErrToFile.getName());
				}
				if (sendErrToFile.getResultCode()==Node.RESULT_ERROR){
					ok = false;
					resultMsg = (resultMsg == null ? "" : resultMsg) + sendErrToFile.getResultMsg() + "\n" + sendErrToFile.getResultException();
				}
			}

		}catch(IOException ex){
			ex.printStackTrace();
			resultMsg = ex.getMessage();
			ok = false;;
		}catch(Exception ex){
		    ex.printStackTrace();
			resultMsg = ex.getClass().getName()+" : "+ ex.getMessage();
			resultCode = Node.RESULT_FATAL_ERROR;
			return;
		}
		broadcastEOF();
		if (!runIt) {
			resultMsg = resultMsg + "\n" + "STOPPED";
			ok = false;;
		}
		if (exitValue!=0){
			if (outputFile!=null) {
				logger.error("Process exit value not 0");
			}
			resultMsg = "Process exit value not 0";
			ok = false;;
		}
		if (ok){
			resultCode = Node.RESULT_OK;
		}
	}

	@Override public void init() throws ComponentNotReadyException {
		if (getInPorts().size()>1) 
			throw new ComponentNotReadyException(getId() + ": too many input ports");
		if (getOutPorts().size()>1) 
			throw new ComponentNotReadyException(getId() + ": too many otput ports");
		if (interpreter!=null){
			try {
				if (interpreter.contains("${}")){
					executeCommand = interpreter.replace("${}",createBatch(command));
				}else {
					throw new ComponentNotReadyException("Incorect form of "+
							XML_INTERPRETER_ATTRIBUTE + " attribute:" + interpreter +
							"\nUse form:\"interpreter [parameters] ${} [parameters]\"");
				}
			}catch(IOException ex){
				throw new ComponentNotReadyException(ex);
			}
		}else{
			executeCommand = command;
		}
		if (getOutPorts().size()==0 && outputFileName!=null){
			File outFile= new File(outputFileName);
			try{
				outFile.createNewFile();
				this.outputFile = new FileWriter(outFile,append);
			}catch(IOException ex){
				throw new ComponentNotReadyException(ex);
			}
		}
	}

	@Override public String getType(){
		return COMPONENT_TYPE;
	}

	@Override public boolean checkConfig() {
		return true;
	}

	 @Override public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		SystemExecute sysExec;
		try {
			sysExec = new SystemExecute(xattribs.getString(XML_ID_ATTRIBUTE),
					xattribs.getString(XML_INTERPRETER_ATTRIBUTE,null),
					xattribs.getString(XML_COMMAND_ATTRIBUTE),
					xattribs.getInteger(XML_ERROR_LINES_ATTRIBUTE,2));
			sysExec.setAppend(xattribs.getBoolean(XML_APPEND_ATTRIBUTE,false));
			if (xattribs.exists(XML_OUTPUT_FILE_ATTRIBUTE)){
				sysExec.setOutputFile(xattribs.getString(XML_OUTPUT_FILE_ATTRIBUTE));
			}
			return sysExec;
		} catch (Exception ex) {
	           throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}
	}
	
	   @Override public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
		xmlElement.setAttribute(XML_COMMAND_ATTRIBUTE,command);
		xmlElement.setAttribute(XML_ERROR_LINES_ATTRIBUTE,String.valueOf(errorLinesNumber));
		if (interpreter!=null){
			xmlElement.setAttribute(XML_INTERPRETER_ATTRIBUTE,interpreter);
		}
		xmlElement.setAttribute(XML_APPEND_ATTRIBUTE,String.valueOf(append));
		if (outputFile!=null){
			xmlElement.setAttribute(XML_OUTPUT_FILE_ATTRIBUTE,outputFileName);
		}
	}
	
	private void setOutputFile(String outputFile){
		this.outputFileName = outputFile;
	}

	private void setAppend(boolean append) {
		this.append = append;
	}

	private static class GetData extends Thread {

		InputPort inPort;
		DataRecord in_record;
		Formatter formatter;
		String resultMsg=null;
		int resultCode;
        volatile boolean runIt;
        Thread parentThread;
		Throwable resultException;
	
		
		GetData(Thread parentThread,InputPort inPort,DataRecord in_record,Formatter formatter){
			super(parentThread.getName()+".GetData");
			this.in_record=in_record;
			this.inPort=inPort;
			this.formatter=formatter;
			runIt=true;
			this.parentThread = parentThread;
		}
		
		public void stop_it(){
			runIt=false;	
		}
		
		public void run() {
			resultCode = Node.RESULT_RUNNING;
			try{
				while (runIt && (( in_record=inPort.readRecord(in_record))!= null )) {
					formatter.write(in_record);
                    SynchronizeUtils.cloverYield();
				}
				formatter.close();
			}catch(IOException ex){
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_ERROR;
				resultException = ex;
				resultMsg = ex.getMessage();
				waitKill(parentThread,KILL_PROCESS_WAIT_TIME);
			}catch (InterruptedException ex){
				resultCode = Node.RESULT_ABORTED;
				formatter.close();
			}catch(Exception ex){
				logger.error("Error in sysexec GetData",ex);
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_ERROR;
				resultException = ex;
				waitKill(parentThread,KILL_PROCESS_WAIT_TIME);
				formatter.close();
			}
			if (resultCode==Node.RESULT_RUNNING){
	           if (runIt){
	        	   resultCode=Node.RESULT_OK;
	           }else{
	        	   resultCode = Node.RESULT_ABORTED;
	           }
			}
		}

		public int getResultCode() {
			return resultCode;
		}

		public String getResultMsg() {
			return resultMsg;
		}

		public Throwable getResultException() {
			return resultException;
		}
	}
	
	private static class SendData extends Thread {

		DataRecord out_record;
		OutputPort outPort;
		Parser parser;
		String resultMsg=null;
		int resultCode;
		volatile boolean runIt;
		Thread parentThread;
		Throwable resultException;
		
		SendData(Thread parentThread,OutputPort outPort,DataRecord out_record,Parser parser){
			super(parentThread.getName()+".SendData");
			this.out_record=out_record;
			this.parser=parser;
			this.outPort=outPort;
			this.runIt=true;
			this.parentThread = parentThread;
		}
		
		public void stop_it(){
			runIt=false;	
		}
		
		public void run() {
            resultCode=Node.RESULT_RUNNING;
			try{
				while (runIt && ((out_record = parser.getNext(out_record))!= null) ) {
					//broadcast the record to all connected Edges
					outPort.writeRecord(out_record);
					SynchronizeUtils.cloverYield();
				}
			}catch(IOException ex){	
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_ERROR;
				resultException = ex;
				parentThread.interrupt();
				waitKill(parentThread,KILL_PROCESS_WAIT_TIME);
			}catch (InterruptedException ex){
				resultCode = Node.RESULT_ABORTED;
			}catch(Exception ex){
				logger.error("Error in sysexec SendData",ex);
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_ERROR;
				resultException = ex;
				waitKill(parentThread,KILL_PROCESS_WAIT_TIME);
			}finally{
//				parser.close();
				outPort.close();
			}
			if (resultCode == Node.RESULT_RUNNING)
				if (runIt) {
					resultCode = Node.RESULT_OK;
				} else {
					resultCode = Node.RESULT_ABORTED;
				}
		}

        /**
		 * @return Returns the resultCode.
		 */
        public int getResultCode() {
            return resultCode;
        }

		public String getResultMsg() {
			return resultMsg;
		}

		public Throwable getResultException() {
			return resultException;
		}
	}
	
	private static class SendDataToFile extends Thread {
		
	    BufferedInputStream process_out;
		FileWriter outFile;
		String resultMsg=null;
		int resultCode;
		volatile boolean runIt;
		Thread parentThread;
		Throwable resultException;
		
		SendDataToFile(Thread parentThread,FileWriter outFile,
				BufferedInputStream process_out){
			super(parentThread.getName()+".SendDataToFile");
			this.outFile = outFile;
			this.process_out = process_out;
			this.runIt=true;
			this.parentThread = parentThread;
		}
		
		public void stop_it(){
			runIt=false;	
		}
		
		public void run() {
            resultCode=Node.RESULT_RUNNING;
            BufferedReader out = new BufferedReader(new InputStreamReader(process_out));
            String line = null;
  			try{
 				while (runIt && ((line=out.readLine())!=null)){
 					synchronized (outFile) {
 						outFile.write(line+"\n");
					}
 				}
			}catch(IOException ex){
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_ERROR;
				resultException = ex;
				waitKill(parentThread,KILL_PROCESS_WAIT_TIME);
			}catch(Exception ex){
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_ERROR;
				resultException = ex;
				waitKill(parentThread,KILL_PROCESS_WAIT_TIME);
			}
			finally{
				try{
					out.close();
				}catch(IOException e){
					//do nothing, out closed
				}
			}
			if (resultCode==Node.RESULT_RUNNING){
		           if (runIt){
		        	   resultCode=Node.RESULT_OK;
		           }else{
		        	   resultCode = Node.RESULT_ABORTED;
		           }
				}
		}

        /**
         * @return Returns the resultCode.
         */
        public int getResultCode() {
            return resultCode;
        }

		public String getResultMsg() {
			return resultMsg;
		}

		public Throwable getResultException() {
			return resultException;
		}
	}

	
}


