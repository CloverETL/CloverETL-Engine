/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
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
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.formatter.DataFormatter;
import org.jetel.data.formatter.Formatter;
import org.jetel.data.parser.Parser;
import org.jetel.data.parser.TextParserFactory;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.TempFileCreationException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.file.FileUtils;
import org.jetel.util.joinKey.JoinKeyUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;
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
 *    <tr><td><b>charset</b></td><td>encoding used for formating/parsing data for input/from output of system process</td></tr>
 *    <tr><td><b>append</b></td><td> whether to append data at the end if output 
 *    file exists or replace it (values: true/false)</td></tr>
 *    <tr><td><b>workingDirecory</b></td><td>this component's working directory.
 *    If not set, used current directory.</td></tr>
 *    <tr><td><b>environment</b></td><td>system-dependent mapping from variables to values. 
 * 		Mappings are separated by {@link Defaults.Component.KEY_FIELDS_DELIMITER_REGEX}. By default the new value is appended
 * 		to the environment of the current process. It can be changed by adding <i>!false</i> after the new value, eg.:
 * 		<i>PATH=/home/user/mydir</i> appends <i>/home/user/mydir</i> to the existing PATH, but <i>PATH=/home/user/mydir!false</i>
 * 		replaces the old value by the new one (<i>/home/user/mydir</i>).</td></tr>
 *    <tr><td><b>deleteBatch</b></td><td> whether to delete or not temporary batch file (values: true/false)</td></tr>
 *     </table>
 *    <h4>Example:</h4> <pre>&lt;Node id="SYS_EXECUTE0" type="SYS_EXECUTE"&gt;
 * &lt;attr name="capturedErrorLines"&gt;3&lt;/attr&gt;
 * &lt;attr name="command"&gt;echo $PATH
 * rm test.txt &lt;/attr&gt;
 * &lt;attr name="environment"&gt;PATH=/mybin&lt;/attr&gt;
 * &lt;/Node&gt;
 *&lt;Node id="SYS_EXECUTE0" type="SYS_EXECUTE"&gt;
 *&lt;attr name="interpreter"&gt;sh ${}&lt;/attr&gt;
 *&lt;attr name="outputFile"&gt;data/data.txt&lt;/attr&gt;
 *&lt;attr name="command"&gt;touch data/data.txt
 *cat
 *dir .&lt;/attr&gt;
 *&lt;attr name="workingDirectory"&gt;/home/user&lt;/attr&gt;
 *&lt;/Node&gt;
 *&lt;Node id="SYS_EXECUTE0" type="SYS_EXECUTE"&gt;
 *&lt;attr name="interpreter"&gt;sh ${} /tmp&lt;/attr&gt;
 *&lt;attr name="command"&gt;cat
 *ls -l $1&lt;/attr&gt;
 * &lt;attr name="deleteBatch"&gt;false&lt;/attr&gt;
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
	private static final String XML_WORKING_DIRECTORY_ATTRIBUTE = "workingDirectory";
	private static final String XML_ENVIRONMENT_ATTRIBUTE = "environment";
	private static final String XML_WORKERS_TIMEOUT_ATTRIBUTE= "workersTimeout";
	private static final String XML_CHARSET_ATTRIBUTE= "charset";
	private static final String XML_IGNORE_EXIT_VALUE_ATTRIBUTE= "ignoreExitValue";
	
	public static final String COMPONENT_TYPE = "SYS_EXECUTE";

	private final static int INPUT_PORT = 0;
	private final static int OUTPUT_PORT = 0;
	
	private final static int ERROR_LINES=2;

	public  final static long KILL_PROCESS_WAIT_TIME = 1000;
		
	private String command;
	private String[] cmdArray;
	private int capturedErrorLines;
	private FileWriter outputFile = null;
	private boolean append;
	private int exitValue;
	private Parser parser;
	private Formatter formatter;
	private File batch;
	private String interpreter;
	private String outputFileName;
	private File workingDirectory = null;
	private Properties environment = new Properties();
	private ProcessBuilder processBuilder;
	private long workersTimeout = 0;
	private String charset = null;
	/** Non-zero exit value of the executed process can be ignored.
	 * By default SystemExecute component fails in case non-zero exit value. */
	private boolean ignoreExitValue = false;
	
	static Log logger = LogFactory.getLog(SystemExecute.class);
	
	public SystemExecute(String id) {
		super(id);
	}
	
	protected void set(String[] cmdArray, int errorLinesNumber)
	{
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
		try {
			batch = getGraph().getAuthorityProxy().newTempFile("tmp", ".bat", -1);
			if (logger.isDebugEnabled()) {
				logger.debug("Created batch file " + batch.getAbsolutePath());
			}
		} catch (TempFileCreationException e) {
			throw new IOException(e);
		}
		FileWriter batchWriter = new FileWriter(batch);
		batchWriter.write(command);
		batchWriter.close();
		if (logger.isDebugEnabled()) {
			logger.debug("Batch file content:\n" + command);
		}
		if (!batch.setExecutable(true)){
			logger.warn("Can't set executable to " + batch.getAbsolutePath());
		}
		return batch.getCanonicalPath();
	}

    @Override
    public void preExecute() throws ComponentNotReadyException {
    	super.preExecute();
		if (getOutPorts().size()==0 && outputFileName!=null){
			try{
				File outFile = new File(FileUtils.getFile(getGraph().getRuntimeContext().getContextURL(), outputFileName));
				outFile.createNewFile();
				this.outputFile = new FileWriter(outFile,append);
			}catch(IOException ex){
				throw new ComponentNotReadyException(ex);
			}
		}
    }
	
	@Override
	public Result execute() throws Exception {
		//Creating and initializing record from input port
		DataRecord in_record=null;
		InputPort inPort = getInputPort(INPUT_PORT);
		//If there is input port read metadadata, initialize in_record and create data formater
		if (inPort!=null) {
			DataRecordMetadata meta=inPort.getMetadata();
			in_record = DataRecordFactory.newRecord(meta);
			in_record.init();
			formatter = charset != null ? new DataFormatter(charset) : new DataFormatter();
		}else{
			formatter=null;
		}
//		Creating and initializing record to otput port
		DataRecord out_record=null;
		OutputPort outPort=getOutputPort(OUTPUT_PORT);
		//If there is output port read metadadata, initialize out_record and create proper data parser
		if (outPort!=null) {
			DataRecordMetadata meta=outPort.getMetadata();
			out_record= DataRecordFactory.newRecord(meta);
			out_record.init();
			parser = TextParserFactory.getParser(getOutputPort(OUTPUT_PORT).getMetadata(), charset);
		}else{
			parser=null;
		}

		boolean ok = true;
		Process process = processBuilder.start();
		try {
		//get process input and output streams
		BufferedOutputStream process_in=new BufferedOutputStream(process.getOutputStream());
		BufferedInputStream process_out=new BufferedInputStream(process.getInputStream());
		// If there is input port read records and write them to input stream of the process
		GetData getData=null; 
		if (inPort!=null) {
            formatter.init(getInputPort(INPUT_PORT).getMetadata());
            formatter.setDataTarget(Channels.newChannel(process_in));
            getData=new GetData(Thread.currentThread(),inPort, in_record, formatter);
			getData.start();
			registerChildThread(getData); //register worker as a child thread of this component
		} else {
			process_in.close(); // Fix of issue #5725
		}
		//If there is output port read output from process and send it to output port
		SendData sendData=null;
		SendDataToFile sendDataToFile = null;
		SendDataToConsole sendDataToConsole = null;
		if (outPort!=null){
            parser.init();
            parser.setDataSource(process_out);
            sendData=new SendData(Thread.currentThread(),outPort,out_record,parser);
			//send all out_records to output ports
			sendData.start();
			registerChildThread(sendData); //register worker as a child thread of this component
		//If there is no output port, but there is defined output file read output
		// and error from process and send it to the file	
		}else if (outputFile!=null){
			sendDataToFile = new SendDataToFile(Thread.currentThread(),outputFile,process_out);
			sendDataToFile.start();
			registerChildThread(sendDataToFile); //register worker as a child thread of this component
		// neither output port nor output file is defined then read output
		// and error from process and send it to the console
		} else {
			sendDataToConsole = new SendDataToConsole(Thread.currentThread(),logger,process_out);
			sendDataToConsole.start();
			registerChildThread(sendDataToConsole); //register worker as a child thread of this component
		}

		//if output is sent to output port log process error stream
		if (sendData!=null){ 
			BufferedInputStream process_err=new BufferedInputStream(process.getErrorStream());
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
			if (errmes.length() > 0) {
				logger.error(errmes.toString());
			}			
		}            
		
		// wait for executed process to finish
		// wait for SendData and/or GetData threads to finish work
		try{
			exitValue=process.waitFor();
			if (sendData!=null) sendData.join(workersTimeout);
			if (getData!=null) getData.join(workersTimeout);
			if (sendDataToFile!=null){
				sendDataToFile.join(workersTimeout);
			}
			if (sendDataToConsole!=null){
				sendDataToConsole.join(workersTimeout);
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
			}
			if (sendDataToConsole!=null) {
				if (!kill(sendDataToConsole,KILL_PROCESS_WAIT_TIME)){
					throw new RuntimeException("Can't kill "+sendDataToConsole.getName());
				}
			}
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
		}
		
		if (sendDataToConsole!=null){
			if (!kill(sendDataToConsole,KILL_PROCESS_WAIT_TIME)){
				throw new RuntimeException("Can't kill "+sendDataToConsole.getName());
			}
			if (sendDataToConsole.getResultCode()==Result.ERROR){
				ok = false;
				resultMsg = (resultMsg == null ? "" : resultMsg) + sendDataToConsole.getResultMsg() + "\n" + sendDataToConsole.getResultException();
			}
		}
//		broadcastEOF();
		if (!runIt) {
			return Result.ABORTED;
		}
		if (exitValue!=0){
			if (ignoreExitValue) {
				logger.info("Process exit value is " + exitValue);
			} else {
				resultMsg = "Process exit value is " + exitValue;
				if (outputFile!=null) {
					logger.error(resultMsg);
				}
				ok = false;
				throw new JetelException(resultMsg);
			}
		}
		if (ok) {
			return Result.FINISHED_OK;
		}else{
			if (getData.getResultException() != null) {
				logger.error("Exception in thread writing to std-in of executed system process:", getData.getResultException());
			}
			if (sendData.getResultException() != null) {
				logger.error("Exception in thread reading std-out of executed system process", getData.getResultException());
			}
			throw new JetelException(resultMsg);
		}
		} finally {
			//this should be part of #postExecute() method, but the method is not invoked for interrupted graphs
			if (process != null) {
				process.destroy();
	}
		}
	}
	
    @Override
    public void postExecute() throws ComponentNotReadyException {
    	super.postExecute();
    	
    	//print out batch file content if the component failed
    	if (getResultCode() == Result.ERROR) {
    		logger.info("SystemExecute component failed with batch file content:\n" + command);
    	}
    	
		deleteBatch();

		try {
    		if (outputFile!=null) {
    			outputFile.close();
    		}
    	}
    	catch (Exception e) {
    		throw new ComponentNotReadyException(COMPONENT_TYPE + ": " + e.getMessage(),e);
    	}
    }
    

	
	@Override
	public void free() {
        if(!isInitialized()) return;
		super.free();
	}
	
	private void deleteBatch(){
		if (interpreter != null) {
			if ((batch != null) && !getGraph().getRuntimeContext().isDebugMode()) {
				if (batch.delete() || !batch.exists()) {
					batch = null;
				} else {
					logger.warn("Batch file (" + batch.getName() + ") was not deleted");
				}
			}
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#init()
	 */
	@Override public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
		//create tmp file with commands and string to execute
		if (cmdArray == null){
			if (interpreter != null) {
				try {
					if (interpreter.contains("${}")) {
						cmdArray = interpreter.replace("${}", createBatch(command)).split("\\s+");
					} else {
						throw new ComponentNotReadyException("Incorect form of " + XML_INTERPRETER_ATTRIBUTE + " attribute:" + interpreter + "\nUse form:\"interpreter [parameters] ${} [parameters]\"");
					}
				} catch (IOException ex) {
					throw new ComponentNotReadyException(ex);
				}
			}else{
				cmdArray = command.split("\\s+");
			}
		}
		StringBuffer msg = new StringBuffer("Command to execute: \"");
		msg.append(cmdArray[0]).append("\" with parameters:\n");
		for (int idx = 1; idx < cmdArray.length; idx++) {
			msg.append(idx).append(": ").append(cmdArray[idx]).append("\n");
		}
		logger.info(msg.toString());
		processBuilder = new ProcessBuilder(cmdArray);
		processBuilder.directory(workingDirectory != null ? 
				workingDirectory : (getGraph().getRuntimeContext().getContextURL() != null ? new File(getGraph().getRuntimeContext().getContextURL().getFile()) : new File(".")));
		logger.info("Working directory set to: " + processBuilder.directory().getAbsolutePath());
		if (!environment.isEmpty()){
			Map<String, String> origEnvironment = processBuilder.environment();
			String name, value, oldValue;
			int exclIndex;
			boolean appendV;
			for (Entry variable : environment.entrySet()) {
				appendV = true;
				name = (String) variable.getKey();
				value = (String) variable.getValue();
				exclIndex = value.indexOf('!');
				if (exclIndex > -1) {
					appendV = Boolean.valueOf(value.substring(exclIndex + 1));
					value = value.substring(0, exclIndex);
				}
				if (origEnvironment.containsKey(name) && appendV) {
					oldValue = origEnvironment.get(name);
					value = oldValue.concat(File.pathSeparator).concat(value);
				}
				origEnvironment.put(name, value);
				logger.info("Variable " + name + " = " +value);
			}
		}
		//wee need separate error stream only if data are sent to output port
		processBuilder.redirectErrorStream(getOutputPort(OUTPUT_PORT) == null);
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
		 
		if(!checkInputPorts(status, 0, 1)
				|| !checkOutputPorts(status, 0, 1)) {
			return status;
		}
		
        if (charset != null && !Charset.isSupported(charset)) {
        	status.add(new ConfigurationProblem(
            		"Charset "+charset+" not supported!", 
            		ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
        }
        	try {
				if (interpreter != null) {
					if (interpreter.contains("${}")) {
						createBatch(command);
						deleteBatch();
					}else{
			            ConfigurationProblem problem = new ConfigurationProblem(
			            		"Incorect form of " + XML_INTERPRETER_ATTRIBUTE + " attribute:" + interpreter + "\nUse form:\"interpreter [parameters] ${} [parameters]\"", ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
			            problem.setAttributeName(XML_INTERPRETER_ATTRIBUTE);
			            status.add(problem);
					}
				}
			} catch (IOException e) {
	            ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
	            problem.setAttributeName(XML_COMMAND_ATTRIBUTE);
	            status.add(problem);
	        }
        
        return status;
    }

	 /* (non-Javadoc)
	 * @see org.jetel.graph.Node#fromXML(org.jetel.graph.TransformationGraph, org.w3c.dom.Element)
	 */
	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException, AttributeNotFoundException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		SystemExecute sysExec;
		sysExec = new SystemExecute(xattribs.getString(XML_ID_ATTRIBUTE),
				xattribs.getString(XML_INTERPRETER_ATTRIBUTE,"${}"),
				xattribs.getStringEx(XML_COMMAND_ATTRIBUTE, RefResFlag.SPEC_CHARACTERS_OFF),
				xattribs.getInteger(XML_ERROR_LINES_ATTRIBUTE,2));
		sysExec.setAppend(xattribs.getBoolean(XML_APPEND_ATTRIBUTE,false));
		if (xattribs.exists(XML_OUTPUT_FILE_ATTRIBUTE)){
			sysExec.setOutputFile(xattribs.getStringEx(XML_OUTPUT_FILE_ATTRIBUTE, RefResFlag.SPEC_CHARACTERS_OFF));
		}
		if (xattribs.exists(XML_WORKING_DIRECTORY_ATTRIBUTE)){
			sysExec.setWorkingDirectory(xattribs.getString(XML_WORKING_DIRECTORY_ATTRIBUTE));
		}
		if (xattribs.exists(XML_ENVIRONMENT_ATTRIBUTE)) {
			sysExec.setEnvironment(xattribs.getString(XML_ENVIRONMENT_ATTRIBUTE));
		}
		if (xattribs.exists(XML_WORKERS_TIMEOUT_ATTRIBUTE)) {
			sysExec.setWorkersTimeout(xattribs.getTimeInterval(XML_WORKERS_TIMEOUT_ATTRIBUTE));
		}
		if (xattribs.exists(XML_CHARSET_ATTRIBUTE)) {
			sysExec.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE));
		}
		if (xattribs.exists(XML_IGNORE_EXIT_VALUE_ATTRIBUTE)) {
			sysExec.setIgnoreExitValue(xattribs.getBoolean(XML_IGNORE_EXIT_VALUE_ATTRIBUTE));
		}
		return sysExec;
	}

	/**
	  * Sets environment. 
	  * 
	 * @param string system-dependent mapping from variables to values. 
	 * 		Mappings are separated by {@link Defaults.Component.KEY_FIELDS_DELIMITER_REGEX}. By default the new value is appended
	 * 		to the environment of the current process. It can be changed by adding <i>!false</i> after the new value, eg.:
	 * 		<i>PATH=/home/user/mydir</i> appends <i>/home/user/mydir</i> to the existing PATH, but <i>PATH=/home/user/mydir!false</i>
	 * 		replaces the old value by the new one (<i>/home/user/mydir</i>).
	 * @throws XMLConfigurationException 
	 */
	public void setEnvironment(String string) throws XMLConfigurationException {
		String[] env = StringUtils.split(string);
		String[] def;
		for (int i = 0; i < env.length; i++) {
			def = JoinKeyUtils.getMappingItemsFromMappingString(env[i]);
			if (def[0] == null) {
		           throw new XMLConfigurationException("Invalid attribute " + StringUtils.quote(XML_ENVIRONMENT_ATTRIBUTE) + 
		        		   " - Missing property key for value: " + def[1]);
			}
			if (def[1] == null) {
		           throw new XMLConfigurationException("Invalid attribute " + StringUtils.quote(XML_ENVIRONMENT_ATTRIBUTE) + 
		        		   " - Missing property value for key: " + def[0]);
			}
			environment.setProperty(def[0], StringUtils.unquote(def[1]));
		}
	}

	/**
	 * Sets working directory
	 * 
	 * @param string working directory for command
	 */
	public void setWorkingDirectory(String string) {
		this.workingDirectory = new File(string);
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
		if (!environment.isEmpty()){
			StringBuilder env = new StringBuilder();
			for (Entry variable : environment.entrySet()) {
				env.append(variable.getKey()).append('=').append(variable.getValue()).append(Defaults.Component.KEY_FIELDS_DELIMITER);
			}		
			env.setLength(env.length() - 1);
			xmlElement.setAttribute(XML_ENVIRONMENT_ATTRIBUTE,env.toString());
		}
		if (workingDirectory != null) {
			xmlElement.setAttribute(XML_WORKING_DIRECTORY_ATTRIBUTE,workingDirectory.getPath());
		}
		if (charset != null) {
			xmlElement.setAttribute(XML_CHARSET_ATTRIBUTE,charset);
		}
	}
	
	/**
	 * Sets output file 
	 * 
	 * @param outputFile
	 */
	public void setOutputFile(String outputFile){
		this.outputFileName = outputFile;
	}

	/**
	 * Sets whether to append data to output file or create new file
	 * 
	 * @param append
	 */
	public void setAppend(boolean append) {
		this.append = append;
	}

	/**
	 * When the background system process finish the producer/consumer workers can still be running.
	 * How long should we wait for these workers? Default is 0 (unlimited waiting).
	 * @return waiting time limit for workers
	 */
	public long getWorkersTimeout() {
		return workersTimeout;
	}

	/**
	 * When the background system process finish the producer/consumer workers can still be running.
	 * How long should we wait for these workers? Default is 0 (unlimited waiting).
	 * @param waitTimeForWorkers waiting time limit for workers
	 */
	public void setWorkersTimeout(long workersTimeout) {
		this.workersTimeout = workersTimeout;
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
		
		@Override
		public void run() {
			resultCode = Result.RUNNING;
			try{
				while (runIt && (( in_record=inPort.readRecord(in_record))!= null )) {
					formatter.write(in_record);
                    SynchronizeUtils.cloverYield();
				}
			}catch(IOException ex){
				resultMsg = ex.getMessage();
				resultCode = Result.ERROR;
				resultException = ex;
				resultMsg = ex.getMessage();
				waitKill(parentThread,KILL_PROCESS_WAIT_TIME);
			}catch (InterruptedException ex){
				resultCode =  Result.ERROR;
			}catch(Exception ex){
				logger.error("Error in sysexec GetData",ex);
				resultMsg = ex.getMessage();
				resultCode = Result.ERROR;
				resultException = ex;
				waitKill(parentThread,KILL_PROCESS_WAIT_TIME);
			} finally {
				try {
					formatter.close();
				} catch (IOException e) {
					// Original code considered this to be an error
					//resultMsg = e.getMessage();
					//resultCode = Result.ERROR;
					//resultException = e;
					//waitKill(parentThread,KILL_PROCESS_WAIT_TIME);

					// Just log a warning rather. The process may have terminated successfully (it closes its stdin), but something may still be left in the formatter buffer.
					// In such a case formatter.close() (which calls flush()) fails with something like IOException: The pipe has been ended.
					// This happens when a binary file is read using UDR into fixed-length byte field, this component then stream-reads this field.
					// In this situation, the last record will (most likely) contain "incomplete" byte field -- its start will contain the end of read binary file
					// and the rest of the field will contain zeros. This is the case when system process (observer with lzop decompressor) terminates after it read all bytes
					// of the file, but there may still be those zeros waiting in the buffer of the formatter to be written into the stdin of the process.
					logger.warn("Failed to close formatter writing to the std-in of executed system process:", e);
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
		
		@Override
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
                } catch (Exception e) {
                    resultCode = Result.ERROR;
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
		
		@Override
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

	/**
	 * This is class for reading data from input stream and writing them to console  
	 * 
	 * @author Mirek Haupt
	 *
	 */
	private static class SendDataToConsole extends Thread {
		
	    BufferedInputStream process_out;
		Log logger;
		String resultMsg=null;
		Result resultCode;
		volatile boolean runIt;
		Thread parentThread;
		Throwable resultException;
		
		/**
		 * Constructor for SendDataToConsole object
		 * 
		 * @param parentThread thread which creates this object
		 * @param logger instance of Log for writing to console
		 * @param process_out input stream, where are data read from
		 */
		SendDataToConsole(Thread parentThread,Log logger,
				BufferedInputStream process_out){
			super(parentThread.getName()+".SendDataToConsole");
			this.logger = logger;
			this.process_out = process_out;
			this.runIt=true;
			this.parentThread = parentThread;
		}
		
		public void stop_it(){
			runIt=false;	
		}
		
		@Override
		public void run() {
            resultCode=Result.RUNNING;
            BufferedReader out = new BufferedReader(new InputStreamReader(process_out));
            String line = null;
  			try{
 				while (runIt && ((line=out.readLine())!=null)){
 					synchronized (logger) {
 						logger.info(line);
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

	/**
	 * @return the charset
	 */
	public String getCharset() {
		return charset;
	}

	/**
	 * @param charset the charset to set
	 */
	public void setCharset(String charset) {
		this.charset = charset;
	}
	
	/**
	 * @return the ignoreExitValue
	 */
	public boolean isIgnoreExitValue() {
		return ignoreExitValue;
	}

	/**
	 * @param ignoreExitValue the ignoreExitValue to set
	 */
	public void setIgnoreExitValue(boolean ignoreExitValue) {
		this.ignoreExitValue = ignoreExitValue;
	}

}


