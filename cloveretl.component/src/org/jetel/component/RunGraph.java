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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.GraphExecutor;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.graph.runtime.WatchDog;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;


/**
* This component can run separated graph.
* * run in the same JVM:
* Graph is run by existing executor in current JVM instance. Log output is flushed together with current graph.
* * run in separated JVM:
* Graph is run as external process. Log output is written into specified file. Executed JVM will have the same classpath as current JVM.  
* 
* Attributes:
* <ul>
* <li>id
* <li>type
* <li>graphName - path to XML graph definition, which should be ran  
* <li>sameInstance - if it's true, graph will be executed in the same instance of JVM; otherwise it will be executed as external process
* </ul>
* Attributes for execution in separated instance of JVM (sameInstance==false) 
* <ul>
* <li>logFile - path to file for logging output of external process
* <li>logAppend - if true, log will be appended to existing, otherwise existing log will be overwritten 
* <li>alternativeJavaCmdLine - command line to execute external process; default is just "java"
* <li>graphExecClass - full class name which runs graph; default is "org.jetel.main.runGraph"
* <li>cloverCmdLineArgs - additional parameters for graph exec class; 
* </ul>
* 
* Outports:
* There are 3 possibilities:
* <ul>
* <li>no edge is connected - nothing happens on the output
* <li>one edge is connected - just one record is generated; it may describe fail or success
* <li>two edges are connected - just one record is generated; it is put to port 0 if success, to port 1 otherwise
* </ul> 
* Metadata of output record:
&lt;Metadata id="Metadata0">
&lt;Record name="outdata" recordSize="-1" type="delimited">
&lt;Field delimiter=";" name="graph" nullable="true" type="string"/>
&lt;Field delimiter=";" name="result" nullable="true" type="string"/>
&lt;Field delimiter=";" name="description" nullable="true" type="string"/>
&lt;Field delimiter=";" name="message" nullable="true" type="string"/>
&lt;Field delimiter="\\n" name="duration" nullable="true" type="long"/>
&lt;/Record>
&lt;/Metadata>

* Output metadata fields:
* <ul>
* <li>graph - path to file with executed graph
* <li>result "Finished OK" | "Aborted" | "Error"
* <li>descriptor - text description usefull when graph fails 
* <li>message - string value of org.jetel.graph.Result 
* <li>duration - graph execution duration in milliseconds
* </ul>
* 
* @author jvicenik <juraj.vicenik@javlinconsulting.cz> 
* (c) JavlinConsulting s.r.o.
*	www.javlinconsulting.cz
*	@created September 26, 2007
*/

public class RunGraph extends Node{
			
	private static final String XML_OUTPUT_FILE_ATTRIBUTE = "logFile";
	private static final String XML_APPEND_ATTRIBUTE = "logAppend";	
	private static final String XML_GRAPH_NAME_ATTRIBUTE = "graphName";
	private static final String XML_SAME_INSTANCE_ATTRIBUTE = "sameInstance";
	private static final String XML_ALTERNATIVE_JVM = "alternativeJavaCmdLine";
	private static final String XML_GRAPH_EXEC_CLASS = "graphExecClass";	
	private static final String XML_CLOVER_CMD_LINE = "cloverCmdLineArgs";
	private final static String JAVA_CMD_LINE = "java -cp";
	private final static String CLOVER_CMD_LINE = "";
	private final static String CLOVER_RUN_CLASS = "org.jetel.main.runGraph";
	private final static String ERROR_COMPONENT_REGEX = ".*Node ([A-Z](\\w)+) finished with status: ERROR.*";
	private final static String ERROR_LAST_MSG_REGEX = "ERROR\\s*\\[main\\]\\s*-\\s*(.*)";	
		
	public static String COMPONENT_TYPE = "RUN_GRAPH";
	
	private final static int INPUT_PORT = 0;
	private final static int OK_OUTPUT_PORT = 0;
	private final static int ERR_OUTPUT_PORT = 1;
		
	private final static int ERROR_LINES=100;

	public  final static long KILL_PROCESS_WAIT_TIME = 1000;
		 	
	private String graphName = null;
	private String classPath;
	private String javaCmdLine;
	private String cloverCmdLineArgs;
	private String cloverRunClass;
			
	private FileWriter outputFile = null;
	
	/** this indicates the mode in which a dummy record is sent to port 0 in case of successful termination of the 
	* graph specified as graphName OR to the port 1 of it terminated with an error
	*/  
	private boolean pipelineMode = false;
	/** whether to run the graph using another instance of JVM and clover (default: use this instance) */
	private boolean sameInstance = true;
	
	private boolean append;
	private boolean outputStatusMsg = false;
	private int exitValue;		
	
	private String outputFileName;
	
	static Log logger = LogFactory.getLog(RunGraph.class);
	
	private void setCloverCmdLineArgs(String cloverCmdLineArgs) {
		this.cloverCmdLineArgs = cloverCmdLineArgs;
	}

	public String getCloverCmdLine() {
		return javaCmdLine;
	}

	public void setJavaCmdLine(String javaCmdLine) {
		this.javaCmdLine = javaCmdLine;
	}

	public String getCloverRunClass() {
		return cloverRunClass;
	}

	public void setCloverRunClass(String cloverRunClass) {
		this.cloverRunClass = cloverRunClass;
	}

	/**
	 * @param id of component	 
	 * @param errorLinesNumber number of error lines which will be logged
	 */
	public RunGraph(String id) {
		super(id);
		
		this.append = false;		
		if(!this.sameInstance) {
			this.classPath = System.getProperty("java.class.path");
		}
	}
	
	/**
	 * @param id of component
	 * @param graphName name of the file containing graph definition
 	 * @param errorLinesNumber number of error lines which will be logged 
	 * @param append whether to append to the output file
	 * @param sameInst whether to run the graph in this instance of clover or run it over. 
	 */
	public RunGraph(String id, boolean append, boolean sameInst) {
		super(id);
				
		this.append = append;
		this.sameInstance = sameInst;
		if(!this.sameInstance) {
			this.classPath = System.getProperty("java.class.path");
		}
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
	 * run graphs whose filenames are determined by input port or an attribute
	 *  
	 * @return FINISHED_OK if all the graphs were processed succesfully, ERROR otherwise
	 * @throws Exception
	 */

	@Override	
	public Result execute() throws Exception {
		//Creating and initializing record from input port
		boolean success;
		DataRecord inRecord=null;
		InputPort inPort = getInputPort(INPUT_PORT);
		//If there is input port read metadata, initialize inRecord and create data formater
		if (inPort!=null) {
			DataRecordMetadata meta=inPort.getMetadata();
			
			inRecord = new DataRecord(meta);
			inRecord.init();			
		} 
								
//		Creating and initializing record to output port
		DataRecord outRec=null;
		OutputPort outPort, outPortErr;
		outPort=getOutputPort(OK_OUTPUT_PORT);
		//If there is output port read metadadata, initialize succOutRecord and create proper data parser
		if (outPort!=null) {
			DataRecordMetadata meta=outPort.getMetadata();			
			outRec = new DataRecord(meta);
			outRec.init();
		} 
	
		outPortErr=getOutputPort(ERR_OUTPUT_PORT);
		if(outRec==null && outPortErr!=null) {
			outRec = new DataRecord(outPortErr.getMetadata());
			outRec.init();
		}
		// if the second output port is connected, let's switch to the 'pipeline' mode
		if(pipelineMode) {						
			// if(outPortErr!=null && outPort!=null) {
			if(inRecord != null) {
				inRecord = readRecord(INPUT_PORT, inRecord);
				// if no record was received
				if(inRecord == null) {
					broadcastEOF();
					if (outputFile!=null) {
						outputFile.close();
					}
					return Result.FINISHED_OK;
				}
			}			
			if (runSingleGraph(graphName, outRec)) {				
				if(outPort!=null) {					
					outPort.writeRecord(outRec);
				}
			} else if(outPortErr!=null) {								
				outPortErr.writeRecord(outRec);
			}
			broadcastEOF();
			if (outputFile!=null) {
				outputFile.close();
			}
			return Result.FINISHED_OK;
		}			
			
		success = false;
		while(inRecord != null && runIt) {
			inRecord = readRecord(INPUT_PORT, inRecord);
			if(inRecord == null) break;
						
			DataField field = inRecord.getField(0);
			Object val = field.getValue();
			if(val == null) continue;
			String fname = val.toString();
						
			try{
				success = runSingleGraph(fname, outRec);
			} catch (Exception e) {					
				logger.error("Exception while processing " + fname + ": "+ e.getMessage());				
			}			
			
			
			outPort.writeRecord(outRec);
						
		} 
		broadcastEOF();	
		
		if (outputFile!=null) {
			outputFile.close();
		}
		
		if (success) return Result.FINISHED_OK;
		else return Result.ERROR;
	}		
	
	private boolean runSingleGraph(String graphName, DataRecord output) throws Exception {
		boolean ok = true;		
		Process	process;		
		String status = null;
		int duration = 0;
		String errComp = null;
		String lastErrMsg = null;
		
		if(sameInstance) {			
			logger.info("********Running graph " + graphName + " in the same instance.");
			if(runGraphThisInstance(graphName, output)) return true;
			else return false;
		}
		logger.info("Running graph " + graphName + " in separate instance of clover.");
		
						
		String commandLine = javaCmdLine + " " + classPath + " " + cloverRunClass + " " + cloverCmdLineArgs + " " + graphName;
		logger.info("Executing command: \"" + commandLine + "\"");
		
		process = Runtime.getRuntime().exec(commandLine);
		
		//get process input and output streams
		BufferedInputStream process_out = new BufferedInputStream(process.getInputStream());		
		
		BufferedInputStream process_err = new BufferedInputStream(process.getErrorStream());
		// If there is input port read records and write them to input stream of the process	
				
		//if output is not sent to file log process error stream						
	
			BufferedReader err=new BufferedReader(new InputStreamReader(process_err));
			BufferedReader out=new BufferedReader(new InputStreamReader(process_out));
			String line;			
			int i=0;
			
			
			while ((line=err.readLine())!=null){									
				if (i<=ERROR_LINES) {
					logger.warn("Processing " + graphName + ": " + line);
					// errmes.append(line+"\n");
				}
													
				/*
				if(line.compareTo(summaryBegin) == 0) {
					line=err.readLine();
					if(line==null) {
						
					}
					line=err.readLine();					
					for (Result res: Result.values()) {						
					}
				}
				*/
				if(outputFile != null) outputFile.write(line + "\n");								
			}
			
			Pattern patErr = Pattern.compile(ERROR_LAST_MSG_REGEX);
			Pattern pt = Pattern.compile(ERROR_COMPONENT_REGEX);
			while ((line=out.readLine())!=null){
				int pos;
				if(line.compareTo("Phase#            Finished Status         RunTime") == 0) {
					if ((line=out.readLine())==null) break;
					if(outputFile != null) outputFile.write(line);
					StringTokenizer tk = new StringTokenizer(line);
					for(int j=0; j<4 && tk.hasMoreTokens(); j++) tk.nextToken();
					if(i==4) {
						status = tk.nextToken();
					}
					// TODO					
				}
				
				// "finished with status: ERROR"
				
				Matcher match = pt.matcher(line);
				if(match.matches() && errComp==null) errComp = match.group(1); 
				
				if((pos=line.indexOf("WatchDog thread finished - total execution time")) != -1) { //TODO
					Scanner sc = new Scanner(line.substring(pos));
					while(sc.hasNext() && !sc.hasNextInt()) sc.next();
					if(sc.hasNextInt()) duration = sc.nextInt();					
				}
				
				Matcher matchErr = patErr.matcher(line);
				if(matchErr.matches()) lastErrMsg = matchErr.group(1);
				 					
				if(outputFile != null) outputFile.write(line + "\n");		
						
			}
								
			err.close();
			out.close();
			process_err.close();
			process_out.close();		
		
		// wait for executed process to finish
		// wait for SendData and/or GetData threads to finish work
		try {
			exitValue=process.waitFor();			
		} catch(InterruptedException ex){
			logger.error("InterruptedException in "+this.getId(),ex);
			process.destroy();
			//interrupt threads if they run still					
		}	
		
		String resultMsg = null;		
		
		if(output != null) {
			// if(status == null) status="*Finished OK";
			output.getField(0).setValue(graphName);
			if(status!=null) output.getField(1).setValue(status); 
			output.getField(3).setValue(errComp); 			
			output.getField(4).setValue(duration);
		}
		
		if (!runIt) {			
			resultMsg = (resultMsg == null ? "" : resultMsg) + "\n" + "STOPPED";
			
			if(output != null) {
				if(status == null) output.getField(1).setValue("Aborted");
				output.getField(2).setValue((lastErrMsg!=null) ? lastErrMsg : resultMsg);
			}
						
			return false;
		}
		if (exitValue!=0){			
			resultMsg = (resultMsg == null ? "" : resultMsg) + "\n" + graphName + ": Process exit value not 0";
				
			if(output != null) {
				if(status == null) output.getField(1).setValue("Error");
				output.getField(2).setValue((lastErrMsg != null) ? lastErrMsg : resultMsg);
			}			
			logger.info(graphName + ": Processing with an ERROR: " + resultMsg);
			return false;
			
			/*
			resultMsg = "Process exit value not 0";
			ok = false;;
			throw new JetelException(resultMsg);
			*/
		}				
		
		if (ok) {
			logger.info(graphName + ": Processing finished successfully");
			if(status == null && output != null) output.getField(1).setValue("Finished successfully");			
			return true;
		} else {
			logger.info(graphName + ": Processing with an ERROR: " + resultMsg);
			if(output!=null) output.getField(2).setValue((lastErrMsg != null) ? lastErrMsg : resultMsg);
			throw new JetelException(resultMsg);
		}
	}
	
	private boolean runGraphThisInstance(String graphFileName, DataRecord output) {
		InputStream in = null;		
		WatchDog watchdog;
		
		output.getField(0).setValue(graphFileName);
		output.getField(1).setValue("Error");
 
		output.getField(3).setValue(""); 			
		output.getField(4).setValue(0);
		
		try {
            in = Channels.newInputStream(FileUtils.getReadableChannel(null, graphFileName));
        } catch (IOException e) {        	
			output.getField(2).setValue("Error - graph definition file can't be read: " + e.getMessage());	
			return false;
        }
        
        GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
		watchdog = getGraph().getWatchDog();
		
		//GraphExecutor graphExecutor = new GraphExecutor();
		GraphExecutor graphExecutor =  watchdog.getGraphExecutor();
		
        Future<Result> futureResult = null;                
        
        String password = null; // TODO
        
        long startTime = System.currentTimeMillis();
		try {
			futureResult = graphExecutor.runGraph(in, runtimeContext, password);
			// graphExecutor.getWatchDog();
        } catch (XMLConfigurationException e) {            
            output.getField(2).setValue("Error in reading graph from XML: " + e.getMessage());
            return false;
        } catch (GraphConfigurationException e) {
        	output.getField(2).setValue("Error - graph's configuration invalid: " + e.getMessage());            
            return false;
		} catch (ComponentNotReadyException e) {
			output.getField(2).setValue("Error during graph initialization: " + e.getMessage());           
            return false;
        } catch (RuntimeException e) {
        	output.getField(2).setValue("Error during graph initialization: " +  e.getMessage());           
            return false;
        }
        
        Result result = Result.N_A;
		try {
			result = futureResult.get();
		} catch (InterruptedException e) {
			output.getField(2).setValue("Graph was unexpectedly interrupted !" + e.getMessage());            
            return false;
		} catch (ExecutionException e) {
			output.getField(2).setValue("Error during graph processing !" + e.getMessage());            
            return false;
		}
        long totalTime = System.currentTimeMillis() - startTime;
		
        switch (result) {
	        case FINISHED_OK:        	
	    		output.getField(1).setValue("Finished OK");
	    		output.getField(2).setValue("");
	    		output.getField(3).setValue(""); 			
    			output.getField(4).setValue(totalTime); 
	            return true;
	        case ABORTED:
	        	output.getField(1).setValue("Aborted");
	        	output.getField(2).setValue("Graph execution aborted. ");
        		output.getField(3).setValue(result.message());
        		output.getField(4).setValue(totalTime);
	        	System.err.println(graphFileName + ": " + "Execution of graph aborted !");
	            return false;
	            
	        default:
	        	output.getField(1).setValue(result.message());        	
	        	output.getField(2).setValue("Execution of graph failed !");
        		output.getField(3).setValue(result.message());
        		output.getField(4).setValue(totalTime);
	            System.err.println(graphFileName + ": " + "Execution of graph failed !");
	            return false;
        }
	}
		
	@Override
	public void free() {
        if(!isInitialized()) return;
		super.free();		
	}	

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#init()
	 */
	@Override public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
	
		if(graphName!=null) {
			pipelineMode = true;
		} 
		
		//prepare output file
		// if (getOutPorts().size()==0 && outputFileName!=null){
		if (outputFileName!=null){
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
	
	/**
	 * this method checks the metadata
	 * 
	 * @param meta metadata to be checked
	 * @return true if the metadata is OK, false otherwise
	 *	 
	 */
	
	boolean checkMetadata(DataRecordMetadata meta)
	{
		if(meta.getFields().length < 5) return false;
		for(int i=0;i<4;i++) {
			if(meta.getFieldType(i) != DataFieldMetadata.STRING_FIELD) {
				return false;
			}
		}        		
		if(meta.getFieldType(4) != DataFieldMetadata.DECIMAL_FIELD) return false;
		
		return true;
	}
	

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#checkConfig()
	 */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
   		super.checkConfig(status);
		 
		if(!checkInputPorts(status, 0, 1)
				|| !checkOutputPorts(status, 0, 2)) {
			return status;
		}
        
        DataRecordMetadata inMetadata=null;
        
        if(graphName!=null) pipelineMode=true;
            
        OutputPort outPort1, outPort2;
        outPort1=getOutputPort(OK_OUTPUT_PORT);
        outPort2=getOutputPort(ERR_OUTPUT_PORT);
        
        InputPort inPort1=getInputPort(INPUT_PORT);
        
        if(outPort1!=null && !checkMetadata(outPort1.getMetadata())  || 
        	outPort2!=null && !checkMetadata(outPort2.getMetadata())) {
    		ConfigurationProblem problem = new ConfigurationProblem("Wrong output metadata", ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
        	status.add(problem);
        	return status;
        }
        
        if(inPort1!=null) { 
        	inMetadata=inPort1.getMetadata();
        	if(pipelineMode) {
        		if(!checkMetadata(inMetadata)) {
        			ConfigurationProblem problem = new ConfigurationProblem("Wrong input metadata", ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
                	status.add(problem);
                	return status;
        		}	        	
        	} else {
        		if(inMetadata.getFieldType(0) != DataFieldMetadata.STRING_FIELD) {
        	
	            	ConfigurationProblem problem = new ConfigurationProblem("Wrong input metadata - first field should be string", ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
	            	status.add(problem);
	            	return status;            	
        		}        		
            }        	                    
        } else if(!pipelineMode) {
        	ConfigurationProblem problem = new ConfigurationProblem("If no graph is specified as an attribute, the input port must be connected", ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
        	status.add(problem);
        	return status;
        }
        
        if(!sameInstance && (cloverCmdLineArgs == null || cloverCmdLineArgs.equals(""))) {
        	ConfigurationProblem problem = new ConfigurationProblem("If the graph is executed in separate instance of clover, supplying the command line for clover is necessary (at least the -plugins argument)" , 
        		ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
        	status.add(problem);
        	return status;
        }
        	
        try {
            init();
        } catch (ComponentNotReadyException e) {
            ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
            if(!StringUtils.isEmpty(e.getAttributeName())) {
                problem.setAttributeName(e.getAttributeName());
            }
            status.add(problem);
        } finally {
        	free();
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
					xattribs.getBoolean(XML_APPEND_ATTRIBUTE,false),
					xattribs.getBoolean(XML_SAME_INSTANCE_ATTRIBUTE, true));
			
			runG.setJavaCmdLine(JAVA_CMD_LINE);
			runG.setCloverRunClass(CLOVER_RUN_CLASS);
			runG.setCloverCmdLineArgs(CLOVER_CMD_LINE);
									
			if (xattribs.exists(XML_OUTPUT_FILE_ATTRIBUTE)){
				runG.setOutputFile(xattribs.getString(XML_OUTPUT_FILE_ATTRIBUTE));
			}
			if (xattribs.exists(XML_GRAPH_NAME_ATTRIBUTE)) {				
				runG.setGraphName(xattribs.getString(XML_GRAPH_NAME_ATTRIBUTE));
			}
			
			if (xattribs.exists(XML_ALTERNATIVE_JVM)) {				
				runG.setJavaCmdLine(xattribs.getString(XML_ALTERNATIVE_JVM));
			}
			if (xattribs.exists(XML_GRAPH_EXEC_CLASS)) {				
				runG.setCloverRunClass(xattribs.getString(XML_GRAPH_EXEC_CLASS));
			}	
			if (xattribs.exists(XML_CLOVER_CMD_LINE)) {
				runG.setCloverCmdLineArgs(xattribs.getString(XML_CLOVER_CMD_LINE));
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
		xmlElement.setAttribute(XML_GRAPH_NAME_ATTRIBUTE, graphName);
		xmlElement.setAttribute(XML_APPEND_ATTRIBUTE, String.valueOf(append));
		xmlElement.setAttribute(XML_SAME_INSTANCE_ATTRIBUTE, String.valueOf(append));
		if (outputFile!=null){
			xmlElement.setAttribute(XML_OUTPUT_FILE_ATTRIBUTE,outputFileName);
		}
		if (javaCmdLine.compareTo(JAVA_CMD_LINE) != 0) {
			xmlElement.setAttribute(XML_ALTERNATIVE_JVM, javaCmdLine);
		}
		if (cloverRunClass.compareTo(CLOVER_RUN_CLASS) != 0) {
			xmlElement.setAttribute(XML_GRAPH_EXEC_CLASS, cloverRunClass);
		}
		if (cloverCmdLineArgs.compareTo(CLOVER_CMD_LINE) != 0) {
			xmlElement.setAttribute(XML_CLOVER_CMD_LINE, cloverCmdLineArgs);
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
	
