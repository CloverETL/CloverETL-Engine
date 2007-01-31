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
import java.nio.channels.Channels;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.formatter.DataFormatter;
import org.jetel.data.formatter.Formatter;
import org.jetel.data.parser.DelimitedDataParser;
import org.jetel.data.parser.FixLenDataParser;
import org.jetel.data.parser.Parser;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.StringUtils;
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
 *  This component executes the specified command and arguments in a separate process
 *   and send output from process to output port. If there is specified command 
 *   interpreter, command string is written to tmp file and there is called this
 *   interpreter to execute system script. If there is not connected output
 *   port, output from process (together with error) can be send to a file<br>
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
 *    <tr><td><b>command</b></td><td> command to be execute by system<br>
 *    	If there is specified interpreter, command is saved in tmp file and there
 *     is called interpreter,which executes this script  </td></tr>
 *    <tr><td><b>interpreter</b></td><td> command interpreter<br> It has to have
 *     form:"interpreter name [parameters] ${} [parameters]", where in place of
 *     ${} System Execute is to put the name of script file </td></tr>
 *    <tr><td><b>capturedErrorLines</b></td><td> number of lines that are print
 *    	 out if command finishes with errors</td></tr>
 *    <tr><td><b>outputFile</b></td><td>path to the output file</td></tr>
 *    <tr><td><b>append</b></td><td> whether to append data at the end if output 
 *    file exists or replace it (values: true/false)</td></tr>
 *    </table>
 *    <h4>Example:</h4> <pre>&lt;Node id="SYS_EXECUTE0" type="SYS_EXECUTE"&gt;
 * &lt;attr name="capturedErrorLines"&gt;3&lt;/attr&gt;
 * &lt;attr name="command"&gt;rm test.txt &lt;/attr&gt;
 * &lt;/Node&gt;
 *&lt;Node id="SYS_EXECUTE0" type="SYS_EXECUTE"&gt;
 *&lt;attr name="interpreter"&gt;sh ${}&lt;/attr&gt;
 *&lt;attr name="outputFile"&gt;data/data.txt&lt;/attr&gt;
 *&lt;attr name="command"&gt;touch data/data.txt
 *cat
 *dir /home/username&lt;/attr&gt;
 *&lt;/Node&gt;
 *&lt;Node id="SYS_EXECUTE0" type="SYS_EXECUTE"&gt;
 *&lt;attr name="interpreter"&gt;sh ${} /tmp&lt;/attr&gt;
 *&lt;attr name="command"&gt;cat
 *ls -l $1&lt;/attr&gt;
 *&lt;/Node&gt;
 *</pre>
/**
* @author avackova <agata.vackova@javlinconsulting.cz> ; 
* (c) JavlinConsulting s.r.o.
*	www.javlinconsulting.cz
*	@created October 10, 2006
 */
public class SystemExecute extends Node{
	
	private static final String XML_COMMAND_ATTRIBUTE = "command";
	private static final String XML_ERROR_LINES_ATTRIBUTE = "capturedErrorLines";
	private static final String XML_OUTPUT_FILE_ATTRIBUTE = "outputFile";
	private static final String XML_APPEND_ATTRIBUTE = "append";
	private static final String XML_INTERPRETER_ATTRIBUTE = "interpreter";
	
	public static String COMPONENT_TYPE = "SYS_EXECUTE";

	private final static int INPUT_PORT = 0;
	private final static int OUTPUT_PORT = 0;
	
	private final static int ERROR_LINES=2;

	public  final static long KILL_PROCESS_WAIT_TIME = 1000;
	
	private String command;
	private String[] cmdArray;
	private String executeCommand;
	private int capturedErrorLines;
	private FileWriter outputFile = null;
	private boolean append;
	private int exitValue;
	private Parser parser;
	private Formatter formatter;
	private File batch;
	private String interpreter;
	private String outputFileName;
	
	static Log logger = LogFactory.getLog(SystemExecute.class);
	
	public SystemExecute(String id) {
		super(id);
	}
	
	protected void set(String[] cmdArray, int errorLinesNumber)
	{
		this.interpreter = null;
		this.command = null;
		this.cmdArray = cmdArray;
		this.capturedErrorLines = errorLinesNumber;		
	}

	/**
	 * @param id of component
	 * @param interpreter command interpreter in proper form
	 * @param command system command to be executed
	 * @param errorLinesNumber number of error lines which will be logged
	 */
	public SystemExecute(String id,String interpreter, String command,int errorLinesNumber) {
		super(id);
		this.interpreter = interpreter;
		this.command = command;
		this.cmdArray = null;
		this.capturedErrorLines=errorLinesNumber;		
	}
	
	/**
	 * this method interupts thread
	 * 
	 * @param thread to be killed
	 * @param millisec time to wait 
	 * @return true if process has been interrupted in given time, false in another
	 *	case 
	 */
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
	
	
	/**
	 * This method interupts process if it is alive after given time
	 * 
	 * @param thread to be killed
	 * @param millisec time to wait before interrupting
	 */
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
	
	
	/**
	 * 
	 * This method writes to tmp file given String
	 * 
	 * @param command string to be written to tmp file
	 * @return canonical path of tmp file
	 * @throws IOException
	 */
	private String createBatch(String command)throws IOException{
		batch =  File.createTempFile("tmp",".bat");
		FileWriter batchWriter = new FileWriter(batch);
		batchWriter.write(command);
		batchWriter.close();
		return batch.getCanonicalPath();
	}

	@Override
	public Result execute() throws Exception {
		//Creating and initializing record from input port
		DataRecord in_record=null;
		InputPort inPort = getInputPort(INPUT_PORT);
		//If there is input port read metadadata, initialize in_record and create data formater
		if (inPort!=null) {
			DataRecordMetadata meta=inPort.getMetadata();
			in_record = new DataRecord(meta);
			in_record.init();
			formatter = new DataFormatter();
		}else{
			formatter=null;
		}
//		Creating and initializing record to otput port
		DataRecord out_record=null;
		OutputPort outPort=getOutputPort(OUTPUT_PORT);
		//If there is output port read metadadata, initialize out_record and create proper data parser
		if (outPort!=null) {
			DataRecordMetadata meta=outPort.getMetadata();
			out_record= new DataRecord(meta);
			out_record.init();
			if (meta.getRecType()==DataRecordMetadata.DELIMITED_RECORD) {
				parser=new DelimitedDataParser();
			}else {
				parser= FixLenDataParser.createParser(meta.getRecordProperties().getBooleanProperty(DataRecordMetadata.BYTE_MODE_ATTR, false));
			}
		}else{
			parser=null;
		}

		boolean ok = true;
		StringBuffer msg = new StringBuffer("Executing command: \"");
		Process	process;
		if (cmdArray != null) {
			msg.append(cmdArray[0]).append("\" with parameters:\n");
			for (int idx = 1; idx < cmdArray.length; idx++) {
				msg.append(idx).append(": ").append(cmdArray[idx]).append("\n");
			}
			logger.info(msg.toString());
			process = Runtime.getRuntime().exec(cmdArray);
		} else {
			msg.append(executeCommand);
			logger.info(msg.toString());
			process = Runtime.getRuntime().exec(executeCommand);
		}
		//get process input and output streams
		BufferedOutputStream process_in=new BufferedOutputStream(process.getOutputStream());
		BufferedInputStream process_out=new BufferedInputStream(process.getInputStream());
		BufferedInputStream process_err=new BufferedInputStream(process.getErrorStream());
		// If there is input port read records and write them to input stream of the process
		GetData getData=null; 
		if (inPort!=null) {
            formatter.init(getInputPort(INPUT_PORT).getMetadata());
            formatter.setDataTarget(Channels.newChannel(process_in));
            getData=new GetData(Thread.currentThread(),inPort, in_record, formatter);
			getData.start();
		}
		//If there is output port read output from process and send it to output port
		SendData sendData=null;
		SendDataToFile sendDataToFile = null;
		SendDataToFile sendErrToFile = null;
		if (outPort!=null){
            parser.init(getOutputPort(OUTPUT_PORT).getMetadata());
            parser.setDataSource(process_out);
            sendData=new SendData(Thread.currentThread(),outPort,out_record,parser);
			//send all out_records to output ports
			sendData.start();
		//If there is no output port, but there is defined output file read output
		// and error from process and send it to the file	
		}else if (outputFile!=null){
			sendDataToFile = new SendDataToFile(Thread.currentThread(),outputFile,process_out);
			sendErrToFile = new SendDataToFile(Thread.currentThread(),outputFile, process_err);
			sendDataToFile.start();
			sendErrToFile.start();
		}
		//if output is not sent to file log process error stream
		if (sendDataToFile==null ){
			BufferedReader err=new BufferedReader(new InputStreamReader(process_err));
			String line;
			StringBuffer errmes=new StringBuffer();
			int i=0;
			while (((line=err.readLine())!=null)&&i++<Math.max(capturedErrorLines,ERROR_LINES)){
				if (i<=capturedErrorLines)
					logger.warn(line);
				if (i<=ERROR_LINES)
					errmes.append(line+"\n");
			}
			if (ERROR_LINES<i) errmes.append(".......\n");
			err.close();
			process_err.close();
			logger.error(errmes.toString());
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
			//interrupt threads if they run still
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
		//chek results of getting and sending data
		String resultMsg = null;
		if (getData!=null){
			if (!kill(getData,KILL_PROCESS_WAIT_TIME)){
				throw new RuntimeException("Can't kill "+getData.getName());
			}
			if (getData.getResultCode()==Result.ERROR) {
				ok = false;
				resultMsg = getData.getResultMsg() + "\n" + getData.getResultException();
			}
		}
		
		if (sendData!=null){
			if (!kill(sendData,KILL_PROCESS_WAIT_TIME)){
				throw new RuntimeException("Can't kill "+sendData.getName());
			}
			if (sendData.getResultCode()==Result.ERROR){
				ok = false;
				resultMsg = (resultMsg == null ? "" : resultMsg) + sendData.getResultMsg() + "\n" + sendData.getResultException();
			}
		}

		if (sendDataToFile!=null){
			if (!kill(sendDataToFile,KILL_PROCESS_WAIT_TIME)){
				throw new RuntimeException("Can't kill "+sendDataToFile.getName());
			}
			if (sendDataToFile.getResultCode()==Result.ERROR){
				ok = false;
				resultMsg = (resultMsg == null ? "" : resultMsg) + sendDataToFile.getResultMsg() + "\n" + sendDataToFile.getResultException();
			}
			if (!kill(sendErrToFile,KILL_PROCESS_WAIT_TIME)){
				throw new RuntimeException("Can't kill "+sendErrToFile.getName());
			}
			if (sendErrToFile.getResultCode()==Result.ERROR){
				ok = false;
				resultMsg = (resultMsg == null ? "" : resultMsg) + sendErrToFile.getResultMsg() + "\n" + sendErrToFile.getResultException();
			}
		}
		broadcastEOF();
		if (!runIt) {
			resultMsg = resultMsg + "\n" + "STOPPED";
			ok = false;;
			return Result.ABORTED;
		}
		if (exitValue!=0){
			if (outputFile!=null) {
				logger.error("Process exit value not 0");
			}
			resultMsg = "Process exit value not 0";
			ok = false;;
			throw new JetelException(resultMsg);
		}
		if (ok) {
			return Result.FINISHED_OK;
		}else{
			throw new JetelException(resultMsg);
		}
	}
	
	@Override
	public void free() {
		super.free();
		deleteBatch();
	}
	
	private void deleteBatch(){
		if (interpreter!=null) {
			if (!batch.delete()) {
				logger.warn("Batch file (" + batch.getName() + ")was not deleted");
			}
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#init()
	 */
	@Override public void init() throws ComponentNotReadyException {
		super.init();
		//create tmp file with commands and string to execute
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
		}else if (cmdArray != null) {
			executeCommand = null;
		} else {
			executeCommand = command;
		}
		//prepare output file
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

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#getType()
	 */
	@Override public String getType(){
		return COMPONENT_TYPE;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#checkConfig()
	 */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		 
		checkInputPorts(status, 0, 1);
        checkOutputPorts(status, 0, 1);

        try {
            init();
            free();
        } catch (ComponentNotReadyException e) {
            ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
            if(!StringUtils.isEmpty(e.getAttributeName())) {
                problem.setAttributeName(e.getAttributeName());
            }
            status.add(problem);
        }
        
        return status;
    }

	 /* (non-Javadoc)
	 * @see org.jetel.graph.Node#fromXML(org.jetel.graph.TransformationGraph, org.w3c.dom.Element)
	 */
	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
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
	
	   /* (non-Javadoc)
	 * @see org.jetel.graph.Node#toXML(org.w3c.dom.Element)
	 */
	@Override public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
		xmlElement.setAttribute(XML_COMMAND_ATTRIBUTE,command);
		xmlElement.setAttribute(XML_ERROR_LINES_ATTRIBUTE,String.valueOf(capturedErrorLines));
		if (interpreter!=null){
			xmlElement.setAttribute(XML_INTERPRETER_ATTRIBUTE,interpreter);
		}
		xmlElement.setAttribute(XML_APPEND_ATTRIBUTE,String.valueOf(append));
		if (outputFile!=null){
			xmlElement.setAttribute(XML_OUTPUT_FILE_ATTRIBUTE,outputFileName);
		}
	}
	
	/**
	 * Sets output file 
	 * 
	 * @param outputFile
	 */
	protected void setOutputFile(String outputFile){
		this.outputFileName = outputFile;
	}

	/**
	 * Sets whether to append data to output file or create new file
	 * 
	 * @param append
	 */
	protected void setAppend(boolean append) {
		this.append = append;
	}

	/**
	 * This is class for reading records from input port and writing them to Formatter  
	 * 
	 * @author avackova
	 *
	 */
	private static class GetData extends Thread {

		InputPort inPort;
		DataRecord in_record;
		Formatter formatter;
		String resultMsg=null;
		Result resultCode;
        volatile boolean runIt;
        Thread parentThread;
		Throwable resultException;
	
		
		/**
		 * Constructor for GetData object
		 * 
		 * @param parentThread thread which creates this object
		 * @param inPort input port
		 * @param in_record input record
		 * @param formatter where are data written
		 */
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
			resultCode = Result.RUNNING;
			try{
				while (runIt && (( in_record=inPort.readRecord(in_record))!= null )) {
					formatter.write(in_record);
                    SynchronizeUtils.cloverYield();
				}
				formatter.close();
			}catch(IOException ex){
				resultMsg = ex.getMessage();
				resultCode = Result.ERROR;
				resultException = ex;
				resultMsg = ex.getMessage();
				waitKill(parentThread,KILL_PROCESS_WAIT_TIME);
			}catch (InterruptedException ex){
				resultCode =  Result.ERROR;
				formatter.close();
			}catch(Exception ex){
				logger.error("Error in sysexec GetData",ex);
				resultMsg = ex.getMessage();
				resultCode = Result.ERROR;
				resultException = ex;
				waitKill(parentThread,KILL_PROCESS_WAIT_TIME);
				formatter.close();
			}
			if (resultCode==Result.RUNNING){
	           if (runIt){
	        	   resultCode=Result.FINISHED_OK;
	           }else{
	        	   resultCode = Result.ABORTED;
	           }
			}
		}

		public Result getResultCode() {
			return resultCode;
		}

		public String getResultMsg() {
			return resultMsg;
		}

		public Throwable getResultException() {
			return resultException;
		}
	}
	
	/**
	 * This is class for reading records from Parser port and writing them to output port  
	 * 
	 * @author avackova
	 *
	 */
	private static class SendData extends Thread {

		DataRecord out_record;
		OutputPort outPort;
		Parser parser;
		String resultMsg=null;
		Result resultCode;
		volatile boolean runIt;
		Thread parentThread;
		Throwable resultException;
		
		/**
		 * Constructor for SendData object
		 * 
		 * @param parentThread thread which creates this object
		 * @param outPort output port
		 * @param out_record output record
		 * @param Parser where are data read from
		 */
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
            resultCode=Result.RUNNING;
			try{
				while (runIt && ((out_record = parser.getNext(out_record))!= null) ) {
					outPort.writeRecord(out_record);
					SynchronizeUtils.cloverYield();
				}
			}catch(IOException ex){	
				resultMsg = ex.getMessage();
				resultCode = Result.ERROR;
				resultException = ex;
				parentThread.interrupt();
				waitKill(parentThread,KILL_PROCESS_WAIT_TIME);
			}catch (InterruptedException ex){
				resultCode = Result.ABORTED;
			}catch(Exception ex){
				logger.error("Error in sysexec SendData",ex);
				resultMsg = ex.getMessage();
				resultCode = Result.ERROR;
				resultException = ex;
				waitKill(parentThread,KILL_PROCESS_WAIT_TIME);
			}finally{
//				parser.close();
				try {
                    outPort.close();
                } catch (InterruptedException e) {
                    resultCode = Result.ABORTED;
                }
			}
			if (resultCode == Result.RUNNING)
				if (runIt) {
					resultCode = Result.FINISHED_OK;
				} else {
					resultCode = Result.ABORTED;
				}
		}

        /**
		 * @return Returns the resultCode.
		 */
        public Result getResultCode() {
            return resultCode;
        }

		public String getResultMsg() {
			return resultMsg;
		}

		public Throwable getResultException() {
			return resultException;
		}
	}
	
	/**
	 * This is class for reading data from input stream and writing them to output file  
	 * 
	 * @author avackova
	 *
	 */
	private static class SendDataToFile extends Thread {
		
	    BufferedInputStream process_out;
		FileWriter outFile;
		String resultMsg=null;
		Result resultCode;
		volatile boolean runIt;
		Thread parentThread;
		Throwable resultException;
		
		/**
		 * Constructor for SendDataToFile object
		 * 
		 * @param parentThread thread which creates this object
		 * @param outFile output file
		 * @param process_out input stream, where are data read from
		 */
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
            resultCode=Result.RUNNING;
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
				resultCode = Result.ERROR;
				resultException = ex;
				waitKill(parentThread,KILL_PROCESS_WAIT_TIME);
			}catch(Exception ex){
				resultMsg = ex.getMessage();
				resultCode = Result.ERROR;
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
			if (resultCode==Result.RUNNING){
		           if (runIt){
		        	   resultCode=Result.FINISHED_OK;
		           }else{
		        	   resultCode = Result.ABORTED;
		           }
				}
		}

        /**
         * @return Returns the resultCode.
         */
        public Result getResultCode() {
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


