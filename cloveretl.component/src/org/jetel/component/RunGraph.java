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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;

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
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;


/**
 *  <h3>System Execute Component</h3> 
 *  <!-- This component runs other graphs.-->
 * 
 *  <table border="1">
 *
 *    <th>
 *      Component:
 *    </th>
 *    <tr><td>
 *        <h4><i>Name:</i> </h4></td><td>RunGraph</td>
 *    </tr>
 *    <tr><td><h4><i>Category:</i> </h4></td><td></td>
 *    </tr>
 *    <tr><td><h4><i>Description:</i> </h4></td>
 *      <td>
 *  This component runs specified graphs. Filenames of the graphs to be executed 
 *  can be supplied by the input edge. The first field in input metadata must be String.
 *  In case the input is not connected a single filename must be specified as the
 *  attribute graphName. 
 *  Output includes a status message for each graph in the last field if it's of 
 *  the String type.
 *  Metadata of the output can be the same as input or it can contain an extra 
 *  String field specially for the status message.  
 *  The output of the cloverETL engine executed by this component can be directed
 *  to a file specified as the attribute outputFileName
 *  
 *      </td>
 *    </tr>
 *    <tr><td><h4><i>Inputs:</i> </h4></td>
 *    <td>
 *        [0]- filenames containing graphs to be executed <br>
 *    </td></tr>
 *    <tr><td> <h4><i>Outputs:</i> </h4>
 *      </td>
 *      <td>
 *        [0] - status of each graph
 *      </td></tr>
 *    <tr><td><h4><i>Comment:</i> </h4>
 *      </td>
 *      <td></td>
 *    </tr>
 *  </table>
 *  <br>
 *  <table border="1">
 *    <th>XML attributes:</th>
 *    <tr><td><b>type</b></td><td>RUN_GRAPH</td></tr>
 *    <tr><td><b>graphName</b></td><td> file containing graph<br>
 *    a file with .grf extension
 *    <tr><td><b>capturedErrorLines</b></td><td> number of lines that are print
 *    	 out if command finishes with errors</td></tr> // TODO
 *    <tr><td><b>outputFile</b></td><td>path to the output file</td></tr>
 *    <tr><td><b>outputAppend</b></td><td> whether to append data if output 
 *    file already exists or to replace it (values: true/false)</td></tr>
 *    </table>
 *    <h4>Example:</h4> <pre>&lt;Node id="RUN_GRAPH0" type="RUN_GRAPH"&gt;
 * &lt;attr name="capturedErrorLines"&gt;3&lt;/attr&gt;
 * &lt;attr name="graphName"&gt;graphAggregate.grf&lt;/attr&gt;
 * &lt;/Node&gt;
 *&lt;Node id="RUN_GRAPH0" type="RUN_GRAPH"&gt;
 *&lt;attr name="ouputAppend"&gt;true &lt;/attr&gt;
 *&lt;attr name="outputFile"&gt;data/data.txt&lt;/attr&gt;
 *&lt;/Node&gt;
 *</pre>
/**
* @author jvicenik <juraj.vicenik@javlinconsulting.cz> ; 
* (c) JavlinConsulting s.r.o.
*	www.javlinconsulting.cz
*	@created September 26, 2007
 */
public class RunGraph extends Node{
		
	private static final String XML_ERROR_LINES_ATTRIBUTE = "capturedErrorLines";
	private static final String XML_OUTPUT_FILE_ATTRIBUTE = "outputFile";
	private static final String XML_APPEND_ATTRIBUTE = "outputAppend";	
	private static final String XML_GRAPH_NAME_ATTRIBUTE = "graphName";
	private final static String cloverCmdLine = "java -cp";
	private final static String cloverRunClass = "org.jetel.main.runGraph";
	
	
	public static String COMPONENT_TYPE = "RUN_GRAPH";

	private final static int INPUT_PORT = 0;
	private final static int OUTPUT_PORT = 0;
	
	private final static int ERROR_LINES=2;

	public  final static long KILL_PROCESS_WAIT_TIME = 1000;
	
	 	
	private String graphName = null;
	private String classPath;
	private int capturedErrorLines;
	
	private FileWriter outputFile = null;
	private boolean append;
	private boolean outputStatusMsg = false;
	private int exitValue;		
	
	private String outputFileName;
	
	static Log logger = LogFactory.getLog(RunGraph.class);
	
	public RunGraph(String id) {
		super(id);		
	}
	
	protected void set(int errorLinesNumber)
	{					
		this.capturedErrorLines = errorLinesNumber;		
	}

	/**
	 * @param id of component	 
	 * @param errorLinesNumber number of error lines which will be logged
	 */
	public RunGraph(String id, int errorLinesNumber) {
		super(id);
		
		this.append = false;
		this.capturedErrorLines=errorLinesNumber;
		this.classPath = System.getProperty("java.class.path");
	}
	
	/**
	 * @param id of component
	 * @param graphName name of the file containing graph definition
 	 * @param errorLinesNumber number of error lines which will be logged 
	 * @param append whether to append to the output file
	 */
	public RunGraph(String id, int errorLinesNumber, boolean append) {
		super(id);
		
	
		this.capturedErrorLines = errorLinesNumber;
		this.append = append;
		this.classPath = System.getProperty("java.class.path");
	}
	/**
	 * this method interrupts thread
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
	 * This method interrupts process if it is alive after given time
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
	 * run graphs whose filenames are read from input or an attribute
	 *  
	 * @return FINISHED_OK if all the graphs were processed succesfully, ERROR otherwise
	 * @throws Exception
	 */

	@Override
	public Result execute() throws Exception {
		//Creating and initializing record from input port
		boolean success;
		DataRecord in_record=null;
		InputPort inPort = getInputPort(INPUT_PORT);
		//If there is input port read metadata, initialize in_record and create data formater
		if (inPort!=null) {
			DataRecordMetadata meta=inPort.getMetadata();
			
			in_record = new DataRecord(meta);
			in_record.init();			
		} 
				
//		Creating and initializing record to output port
		DataRecord out_record=null;
		OutputPort outPort=getOutputPort(OUTPUT_PORT);
		//If there is output port read metadadata, initialize out_record and create proper data parser
		if (outPort!=null) {
			DataRecordMetadata meta=outPort.getMetadata();
			out_record= new DataRecord(meta);
			out_record.init();			
		} 
		
		if(in_record == null) {
			Result res = runSingleGraph(graphName);
			writeOutputLine(out_record, graphName, res);			
			broadcastEOF();
			if (res==Result.FINISHED_OK) return res;
			else return Result.ERROR;			
		} else {
			
			success = true;
			while(in_record != null && runIt) {
				in_record = readRecord(INPUT_PORT, in_record);
				if(in_record == null) break;
				
				Result result;
				DataField field = in_record.getField(0);
				Object val = field.getValue();
				if(val == null) continue;
				String fname = val.toString();
				
				try{
					result = runSingleGraph(fname);
				} catch (Exception e) {					
					logger.error("Exception while processing " + fname + ": "+ e.getMessage());
					result = Result.ERROR;
				}
				
				if(result != Result.FINISHED_OK) success = false;
				/*if(result == Result.ABORTED)
					return Result.ABORTED; */
				writeOutputLine(out_record, fname, result);				
			} 
			broadcastEOF();	
		}
		if (outputFile!=null) {
			outputFile.close();
		}
		
		if (success) return Result.FINISHED_OK;
		else return Result.ERROR;
	}
	
	private void writeOutputLine(DataRecord outRec, String fname, Result r) throws IOException, InterruptedException 
	{		
		if(outRec == null) return; 
						
		outRec.getField(0).setValue(fname);
		if(outputStatusMsg) {
			// status message will be in the last field of output record
			org.jetel.data.DataField df = outRec.getField(outRec.getNumFields()-1);
			
			if(r == Result.ABORTED) df.setValue("The execution of the graph was aborted!");
			else if(r == Result.ERROR) df.setValue("Execution of the graph terminated with an error!");
			else if(r == Result.FINISHED_OK) df.setValue("Succesful termination.");
		}
		writeRecord(OUTPUT_PORT, outRec);					
	}
	
	private Result runSingleGraph(String graphName) throws Exception {
		boolean ok = true;		
		Process	process;
		
		logger.info("Processing file " + graphName);
		String commandLine = cloverCmdLine + " " + classPath + " " + cloverRunClass + " " + org.jetel.main.runGraph.cmdLineArgs + " " + graphName;
		logger.info("Executing command: \"" + commandLine + "\"");
		
		process = Runtime.getRuntime().exec(commandLine);
		
		//get process input and output streams
		BufferedInputStream process_out = new BufferedInputStream(process.getInputStream());		
		
		BufferedInputStream process_err=new BufferedInputStream(process.getErrorStream());
		// If there is input port read records and write them to input stream of the process	
		
		//If there is output port read output from process and send it to output port
		
		SendDataToFile sendDataToFile = null;
		SendDataToFile sendErrToFile = null;		
		if (outputFile!=null){
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
					logger.warn(graphName + ": " + line);
				if (i<=ERROR_LINES)
					errmes.append(line+"\n");
			}
			if (ERROR_LINES<i) errmes.append(".......\n");
			err.close();
			process_err.close();
			// logger.error(errmes.toString());
		}            
		
		// wait for executed process to finish
		// wait for SendData and/or GetData threads to finish work
		try {
			exitValue=process.waitFor();
			if (sendDataToFile!=null){
				sendDataToFile.join(KILL_PROCESS_WAIT_TIME);
				sendErrToFile.join(KILL_PROCESS_WAIT_TIME);
			}			
		} catch(InterruptedException ex){
			logger.error("InterruptedException in "+this.getId(),ex);
			process.destroy();
			//interrupt threads if they run still
		
			if(sendDataToFile.resultCode == Result.ERROR) {
				logger.error("Reason: error while reading stdout of the subprocess: " + sendDataToFile.resultMsg);
			} 
			if(sendErrToFile.resultCode == Result.ERROR) {
				logger.error("Reason: error while reading error output of the subprocess: " + sendErrToFile.resultMsg);
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
	
		
		String resultMsg = null;

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
		
		if (!runIt) {
			
			resultMsg = resultMsg + "\n" + "STOPPED";
			/*
			ok = false;;  */
			return Result.ABORTED;			
		}
		if (exitValue!=0){
			if (outputFile!=null) {
				logger.error(graphName + ": Process exit value not 0");
			}
			resultMsg = resultMsg + "\n" + graphName + ": Process exit value not 0";
			
			return Result.ERROR;
			
			/*
			resultMsg = "Process exit value not 0";
			ok = false;;
			throw new JetelException(resultMsg);
			*/
		}
		if (ok) {
			logger.info(graphName + ": Processing finished successfully");
			return Result.FINISHED_OK;
		}else{
			throw new JetelException(resultMsg);
		}
	}
		
	@Override
	public void free() {
		super.free();		
	}	

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#init()
	 */
	@Override public void init() throws ComponentNotReadyException {
		super.init();
	
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
        
        DataRecordMetadata inMetadata=null, outMetadata;
        
        InputPort inPort=getInputPort(INPUT_PORT);        
        if(inPort != null) {
        	inMetadata=inPort.getMetadata();
        	//input metadata - first field must be string
        	if(inMetadata.getFieldType(0) != DataFieldMetadata.STRING_FIELD) {
        		ConfigurationProblem problem = new ConfigurationProblem("Wrong input metadata", ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
        		status.add(problem);
        		return status;
        	}
        } else { 
        	if(graphName == null) {
        		ConfigurationProblem problem = new ConfigurationProblem("Input not connected and no graph name attribute specified.", ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
        		status.add(problem);
        		return status;
        	}        		
        }
        
        OutputPort outPort=getOutputPort(OUTPUT_PORT);        
        if(outPort != null) {
        	outMetadata=outPort.getMetadata();
        	        
        	if(inPort!=null) {
        		boolean error = false;
        		int inN = inMetadata.getNumFields();
        		int outN = outMetadata.getNumFields();
        		
        		// input and output metadata can be equal...
        		if(inN == outN) {
        			if(inN>1  &&  inMetadata.getFieldType(inN-1) == DataFieldMetadata.STRING_FIELD) outputStatusMsg = true;
        		// or there can be one extra String field for status messages in the output
        		} else if(inN + 1 == outN && outMetadata.getFieldType(outN-1) == DataFieldMetadata.STRING_FIELD) {
        			outputStatusMsg = true;
        		// otherwise the metadata is not correct
        		} else error = true;
        		
        		// now really check if they are equal (don't take the extra field in output into account)
        		if(!error) {
        			for(int i=0;i<inN;i++) {
        				if(inMetadata.getFieldType(i) != outMetadata.getFieldType(i)) {
        					error = true;
        					break;
        				}
        			}
        		}
        		
        		if(error) {
        			ConfigurationProblem problem = new ConfigurationProblem("Wrong input or output metadata", ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
        			status.add(problem);
        			return status;
        		}        	
        	}
        }
           
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
		RunGraph runG;
		try {
			runG = new RunGraph(xattribs.getString(XML_ID_ATTRIBUTE),										
					xattribs.getInteger(XML_ERROR_LINES_ATTRIBUTE,2),
					xattribs.getBoolean(XML_APPEND_ATTRIBUTE,false));
			
			
			
			if (xattribs.exists(XML_OUTPUT_FILE_ATTRIBUTE)){
				runG.setOutputFile(xattribs.getString(XML_OUTPUT_FILE_ATTRIBUTE));
			}
			if (xattribs.exists(XML_GRAPH_NAME_ATTRIBUTE)) {
				runG.setGraphName(xattribs.getString(XML_GRAPH_NAME_ATTRIBUTE));
			}
			return runG;
		} catch (Exception ex) {
	           throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}
	}
	
	   /* (non-Javadoc)
	 * @see org.jetel.graph.Node#toXML(org.w3c.dom.Element)
	 */
	@Override public void toXML(Element xmlElement) {
		super.toXML(xmlElement);		
		xmlElement.setAttribute(XML_ERROR_LINES_ATTRIBUTE,String.valueOf(capturedErrorLines));
		xmlElement.setAttribute(XML_GRAPH_NAME_ATTRIBUTE, graphName);
		xmlElement.setAttribute(XML_APPEND_ATTRIBUTE, String.valueOf(append));
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
	
	protected void setGraphName(String graphName) {
		this.graphName = graphName;
	}
	
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
	/**
	 * This is class for reading records from input port and writing them to Formatter  
	 * 
	 * @author avackova
	 *
	 */
	
