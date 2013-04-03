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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.JetelException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.graph.runtime.IAuthorityProxy.RunStatus;
import org.jetel.graph.runtime.tracker.ComponentTokenTracker;
import org.jetel.graph.runtime.tracker.ReformatComponentTokenTracker;
import org.jetel.main.runGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.plugin.PluginLocation;
import org.jetel.plugin.Plugins;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.exec.DataConsumer;
import org.jetel.util.exec.PlatformUtils;
import org.jetel.util.exec.ProcBox;
import org.jetel.util.file.FileUtils;
import org.jetel.util.primitive.TypedProperties;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.CloverString;
import org.jetel.util.string.Concatenate;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;


/**
* This component can run separated graph.
* * run in the same JVM:
* Graph is run by existing executor in current JVM instance. Log output is flushed together with current graph.
* * run in separated JVM:
* Graph is run as external process. Log output is written into specified file. Executed JVM will have the same classpath as current JVM.
* <br/>In this mode, supplying the command line for clover is necessary (at least the -plugins argument).
* Command line arguments can be supplied by cloverCmdLineArgs attribute or by second field in input port 
* (Supplying by field can be made only in in/out mode).
* If cloverCmdLineArgs attribute is defined and input port is connected then field from input port has 
* higher priority. Thus when some graph defined in input port hasn't assigned command line arguments
* cloverCmdLineArgs attribute is used.
* 
* Attributes:
* <ul>
* <li>id
* <li>type
* <li>graphName - path to XML graph definition, which should be ran  
* <li>sameInstance - if it's true, graph will be executed in the same instance of JVM; otherwise it will be executed as external process
* <li>ignoreGraphFail - if more graphs is executed and any of them fails, then execution next graphs depends on this attribute. 
* Default value = false (when any graph fails no other is executed). Note: used only if in/out mode is used.  
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
* <ul>
* <li>in/out mode- one edge is connected - one record is generated for each input record; it may describe fail or success
* <li>pipeline mode - two edges are connected - just one record is generated; it is put to port 0 if success, to port 1 otherwise
* </ul> 
* Metadata of input record:
* 1st field - graph name; type=string
* 2nd field - clover command line arguments; type=string
* 
* Metadata of output record:
* 1st field - graph name; type=string
* 2nd field - result; type=string
* 3rd field - description; type=string
* 4th field - message; type=string
* 5th field - duration; type=integer/long/decimal
* 6th field - runId; type=long - optional
*
* Output metadata fields:
* <ul>
* <li>graph - path to file with executed graph
* <li>result "Finished OK" | "Aborted" | "Error"
* <li>descriptor - text description usefull when graph fails 
* <li>message - string value of org.jetel.graph.Result 
* <li>duration - graph execution duration in milliseconds
* <li>runId - execution ID - optional field
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
	public static final String XML_GRAPH_NAME_ATTRIBUTE = "graphName";
	private static final String XML_SAME_INSTANCE_ATTRIBUTE = "sameInstance";
	private static final String XML_ALTERNATIVE_JVM = "alternativeJavaCmdLine";
	private static final String XML_GRAPH_EXEC_CLASS = "graphExecClass";	
	private static final String XML_CLOVER_CMD_LINE = "cloverCmdLineArgs";
	private static final String XML_IGNORE_GRAPH_FAIL = "ignoreGraphFail";
	private static final String XML_PARAMS_TO_PASS = "paramsToPass";

	private final static String DEFAULT_JAVA_CMD_LINE = "java -cp";
	private final static String DEFAULT_CLOVER_CMD_LINE = "";
	private final static String DEFAULT_GRAPH_EXEC_CLASS = "org.jetel.main.runGraph";
	private final static boolean DEFAULT_IGNORE_GRAPH_FILE = false;
	
	private final static int INPUT_PORT = 0;
	private final static int OUTPUT_PORT = 0;
	private final static int ERR_OUTPUT_PORT = 1;
	
	public final static String COMPONENT_TYPE = "RUN_GRAPH";
	
	private InputPort inPort;
	private OutputPort outPort;
	private OutputPort outPortErr;
	
	private String graphName = null;
	private String classPath;
	private String javaCmdLine;
	private String cloverCmdLineArgs;
	private String cloverRunClass;
	
	/** List of names of parameters which are passed from the graph to executed graph. 
	 * Makes sense only if sameInstance is true. Params names are separated with  */
	private String paramsToPass;
	private boolean ignoreGraphFail;
			
	private FileWriter outputFile = null;
	private BufferedWriter fileWriter = null;
	
	/** 
	 * this indicates the mode in which a dummy record is sent to port 0 in case of successful termination of the 
	 * graph specified as graphName OR to the port 1 of it terminated with an error
	*/  
	private boolean pipelineMode = false;
	
	/** whether to run the graph using another instance of JVM and clover (default: use this instance) */
	private boolean sameInstance;
	
	private boolean append;
	private int exitValue;		
	private String outputFileName;
	private static Log logger = LogFactory.getLog(RunGraph.class);
	
	private void setCloverCmdLineArgs(String cloverCmdLineArgs) {
		this.cloverCmdLineArgs = cloverCmdLineArgs;
	}

	private void setJavaCmdLine(String javaCmdLine) {
		this.javaCmdLine = javaCmdLine;
	}

	private void setCloverRunClass(String cloverRunClass) {
		this.cloverRunClass = cloverRunClass;
	}
	
	private void setIgnoreGraphFail(boolean ignoreGraphFail) {
		this.ignoreGraphFail = ignoreGraphFail;
	}
	
	public void setParamsToPass(String paramsToPass) {
		this.paramsToPass = paramsToPass;
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
		if (!this.sameInstance) {
			this.classPath = System.getProperty("java.class.path");
		}
	}	
	
	private DataRecord initInRecord() {
		if (inPort != null) {
			DataRecord inRecord = DataRecordFactory.newRecord(inPort.getMetadata());
			inRecord.init();	
			return inRecord;
		}
		
		return null;
	}
	
	private DataRecord initOutRecord() {
		if (outPort != null) {
			DataRecord outRec = DataRecordFactory.newRecord(outPort.getMetadata());
			outRec.init();
			return outRec;
		} 
	
		if (outPortErr != null) {
			DataRecord outRec = DataRecordFactory.newRecord(outPortErr.getMetadata());
			outRec.init();
			return outRec;
		}
		
		return null;
	}
	

    @Override
    public void preExecute() throws ComponentNotReadyException {
    	super.preExecute();
   		initGraphOutputFile();
    }    
	

	/**
	 * run graphs whose filenames are determined by input port or an attribute
	 *  
	 * @return FINISHED_OK if all the graphs were processed succesfully, ERROR otherwise
	 * @throws Exception
	 */
	@Override	
	public Result execute() throws Exception {
		DataRecord outRec = initOutRecord();
		if (pipelineMode) {
			Result result = runSingleGraph(graphName, outRec, cloverCmdLineArgs);
			writeRecord(result, outRec);
			return Result.FINISHED_OK;
		}			

		// in-out mode
		DataRecord inRecord = initInRecord();
		String graphNameFromPort = null;
		while (inRecord != null && runIt) {
			inRecord = inPort.readRecord(inRecord);
			if (inRecord == null) {
				break;
			}
			
			graphNameFromPort = readGraphName(inRecord);
			if (graphNameFromPort == null) {
				continue;
			}
			String cloverCommandLineArgs = "";

			if (!sameInstance) {
			    cloverCommandLineArgs = getCloverCommandLineArgs(inRecord);
			}

			try {
				Result result = runSingleGraph(graphNameFromPort, outRec, cloverCommandLineArgs);
				writeRecord(result, outRec);
			} catch (IOException e) {					
				logError("Exception while processing '" + graphNameFromPort + "'. ", e);
			}			
		} 
		
		return Result.FINISHED_OK;
	}
	
   @Override
    public void postExecute() throws ComponentNotReadyException {
    	super.postExecute();
    	
		if (outputFile != null) {
			try {
				outputFile.close();
			} catch (Exception e) {
				throw new ComponentNotReadyException(e);
			}
		}
    }


   private void writeRecord(Result result, DataRecord record) throws IOException, InterruptedException {
		if (result.code() == Result.FINISHED_OK.code()) {
			writeOutRecord(record);
		} else {
			writeErrRecord(record);
		}
   }
   
	private void writeOutRecord(DataRecord record) throws IOException, InterruptedException {
		if (outPort != null) {
			outPort.writeRecord(record);
		}
	}

	private void writeErrRecord(DataRecord record) throws IOException, InterruptedException {
		if (outPortErr != null) {
			outPortErr.writeRecord(record);
		}
	}
	
	/**
	 * Read graph name - the first field on input port.
	 * @param inRecord
	 * @return
	 */
	private static String readGraphName(DataRecord inRecord) {
		return getStrValueFromField(inRecord, 0);
	}
	
	/**
	 * Return clover command line arguments . Read from input port, 
	 * if reading from port isn't success then cloverCmdLineArgs is used.
	 * @param inRecord
	 * @return clover command line arguments
	 */
	private String getCloverCommandLineArgs(DataRecord inRecord) {
		String result = readCloverCommandLineArgs(inRecord);
		if (StringUtils.isEmpty(result)) {
			result = cloverCmdLineArgs;
		}
		
		return result;
	}

	/**
	 * Read clover command arguments - the second field on input port.
	 * @param inRecord
	 * @return
	 */
	private static String readCloverCommandLineArgs(DataRecord inRecord) {
		return getStrValueFromField(inRecord, 1);
	}
	
	private static String getStrValueFromField(DataRecord inRecord, int fieldNum) {
		if (inRecord.getNumFields() < fieldNum + 1) {
			return null;
		}
		
		DataField field = inRecord.getField(fieldNum);
		Object val = field.getValue();
		if (val == null) {
			return null;
		}
		return val.toString().trim();
	}
	
	private Result runSingleGraph(String graphName, DataRecord output, String cloverCommandLineArgs) throws IOException, InterruptedException {
		OutputRecordData outputRecordData = new OutputRecordData(output, graphName);
		if (sameInstance) {			
			logger.info("Running graph " + graphName + " in the same instance.");
			return runGraphThisInstance(graphName, outputRecordData);
		} else {
			logger.info("Running graph " + graphName + " in separate instance of clover.");
			return runGraphSeparateInstance(graphName, outputRecordData, cloverCommandLineArgs);
		}
	}
	
	private Result runGraphSeparateInstance(String graphName, OutputRecordData outputRecordData, String cloverCommandLineArgs) throws IOException {
		List<String> commandList = new ArrayList<String>();

		for (String javaCommand : StringUtils.split(javaCmdLine, " ")) {
			commandList.add(javaCommand);
		}
		commandList.add(classPath);
		commandList.add(cloverRunClass);

		List<String> args = Arrays.asList(StringUtils.split(cloverCommandLineArgs, " "));
		
		if (!args.contains(runGraph.PLUGINS_SWITCH)) {
            Concatenate pluginsDir = new Concatenate(";");
            for (PluginLocation pluginLocation : Plugins.getPluginLocations()) {
                pluginsDir.append(StringUtils.backslashToSlash(FileUtils.convertUrlToFile(pluginLocation.getLocation()).getPath()));
            }
            commandList.add(runGraph.PLUGINS_SWITCH);
            commandList.add(pluginsDir.toString());
        }

		for (String cloverCommandArg : args) {
			commandList.add(StringUtils.unquote2(cloverCommandArg));
		}
		
		if (!args.contains(runGraph.CONTEXT_URL_SWITCH) && getGraph().getRuntimeContext().getContextURL() != null) {
			commandList.add(runGraph.CONTEXT_URL_SWITCH);
			commandList.add(getGraph().getRuntimeContext().getContextURL().toString());
		}
		// TODO - hotfix - clover can't run two graphs simultaneously with enable edge debugging
		// after resolve issue 1748 (http://home.javlinconsulting.cz/view.php?id=1748) next line should be removed
		commandList.add(runGraph.NO_DEBUG_SWITCH);

		commandList.add(graphName);

		DataConsumer consumer = new OutDataConsumer(fileWriter, outputRecordData);
		DataConsumer errConsumer = new ErrorDataConsumer(fileWriter, logger, graphName);

		String[] command = commandList.toArray(new String[commandList.size()]);
		
		//the arguments have to be quoted on windows platform 
		//@see http://bugs.sun.com/view_bug.do?bug_id=6468220
		if (PlatformUtils.isWindowsPlatform()) {
			quoteArguments(command);
		}

		logger.info("Executing command: " + StringUtils.quote(Arrays.toString(command)));
		
		Process process = Runtime.getRuntime().exec(command);
		ProcBox procBox = new ProcBox(process, null, consumer, errConsumer);
		registerChildThreads(procBox.getChildThreads()); //register workers as child threads of this component
		
		// wait for executed process to finish
		// wait for SendData and/or GetData threads to finish work
		try {
			exitValue = procBox.join();
		} catch (InterruptedException ex) {
			logError("InterruptedException in " + this.getId(), ex);
			process.destroy();
			return Result.ABORTED;					
		}	
			
		if (!runIt) { //this is useless, isn't it?			
			outputRecordData.setResult("Aborted");
			outputRecordData.setDescription("\nSTOPPED");
			return Result.ABORTED;
		}
		if (exitValue != 0) {			
			String resultMsg = "Process exit value = " + exitValue;
			outputRecordData.setResult("Error");
			outputRecordData.setDescription(resultMsg);
						
            logError("Execution of graph '" + graphName + "' failed! " + resultMsg, null);
			return Result.ERROR;
		}				
		
		logger.info(graphName + ": Processing finished successfully");
		outputRecordData.setResult("Finished successfully");			
		return Result.FINISHED_OK;
	}
	
	/**
	 * Use this JVM to execute graph.
	 * It delegates execution to @link IAuthorityProxy
	 * 
	 * @param graphFileName
	 * @param outputRecordData
	 * @return
	 * @throws InterruptedException 
	 */
	private Result runGraphThisInstance(String graphFileName, OutputRecordData outputRecordData) throws InterruptedException {
		GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
		runtimeContext.setAdditionalProperties(extractNeededGraphProperties(this.getGraph().getGraphProperties()));
		runtimeContext.setContextURL(this.getGraph().getRuntimeContext().getContextURL());
		runtimeContext.setLogLocation(outputFileName);
		runtimeContext.setUseJMX(this.getGraph().getRuntimeContext().useJMX());
		
		RunStatus rs = this.getGraph().getAuthorityProxy().executeGraphSync( graphFileName, runtimeContext, null);
		
		outputRecordData.setDescription(assembleDescription(rs));
		outputRecordData.setDuration(rs.duration);
		outputRecordData.setGraphName(graphFileName);
		outputRecordData.setMessage(rs.status.message());
		outputRecordData.setRunId(rs.runId);
		if (rs.status == Result.ABORTED) {
        	outputRecordData.setResult("Aborted");
        	outputRecordData.setDescription("Execution of graph '" + graphFileName + "' aborted!");
        	logError("Execution of graph '" + graphFileName + "' aborted!", null);
		} else if (rs.status == Result.FINISHED_OK) {
        	outputRecordData.setResult("Finished successfully");
    		outputRecordData.setDescription("");
		} else {
        	outputRecordData.setResult(Result.ERROR.equals(rs.status) ? "Error" : rs.status.message());
        	outputRecordData.setDescription("Execution of graph '" + graphFileName + "' failed! " + assembleDescription(rs));
            logError("Execution of graph '" + graphFileName + "' failed!", assembleException(rs));
		}
		return rs.status;
	}

	private String assembleDescription(RunStatus runStatus) {
		String message = runStatus.errMessage;
		String exception = runStatus.errException;
		if (!StringUtils.isEmpty(message)) {
			return message + (!StringUtils.isEmpty(exception) ? "\nInner exception: " + exception : "");
		} else {
			return (!StringUtils.isEmpty(exception)) ? "Inner exception: " + exception : "";
		}
	}

	private Exception assembleException(RunStatus runStatus) {
		return new JetelRuntimeException(assembleDescription(runStatus));
	}

	private void logError(String message, Throwable e) {
		if (!ignoreGraphFail) {
			throw new JetelRuntimeException(message, e);
		} else {
			logger.info(message, e);
		}
	}
		
	/**
	 * Returns Properties with some of specified properties according to "paramsToPass" attribute. 
	 * @param graphProperties
	 * @return
	 */
	private Properties extractNeededGraphProperties(TypedProperties graphProperties) {
		Properties p = new Properties();
		if (paramsToPass == null || paramsToPass.trim().length()==0)
			return p;
		String[] paramNames = paramsToPass.split(Defaults.DEFAULT_PATH_SEPARATOR_REGEX);
		for (String paramName : paramNames) {
			paramName = paramName.trim();
			if (graphProperties.containsKey(paramName))
				p.put(paramName, graphProperties.get(paramName));
		}// for
		return p;
	}

	@Override
	public void free() {
        if (!isInitialized()) return;
		super.free();
		
		if (outputFile != null) {
			try {
				outputFile.close();
			} catch (IOException ioe) {
				logger.warn("File " + StringUtils.quote(outputFileName) + " wasn't closed.", ioe);
			}
		}
	}	

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#init()
	 */
	@Override
	public void init() throws ComponentNotReadyException {
        if (isInitialized()) return;
		super.init();
	
		ConfigurationStatus status = new ConfigurationStatus();
		if (!checkMetadata(status) || !checkParams(status)) {
			throw new ComponentNotReadyException(this, status.getLast().getMessage());
		}
		
		pipelineMode = isPipelineMode();
		
		inPort = getInputPort(INPUT_PORT);
		outPort = getOutputPort(OUTPUT_PORT);
		outPortErr = getOutputPort(ERR_OUTPUT_PORT);
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#getType()
	 */
	@Override public String getType(){
		return COMPONENT_TYPE;
	}
	
	private boolean isPipelineMode() {
		if (graphName != null) {
			return true;
		}
		return false;
	}
	
	/**
	 * this method checks the out metadata
	 * 
	 * @param meta metadata to be checked
	 * @return true if the metadata is OK, false otherwise
	 *	 
	 */
	private boolean checkOutMetadata(DataRecordMetadata meta) {
		if (meta.getFields().length < 5) {
			return false;
		}
		for (int i = 0; i < 4; i++) {
			if (meta.getFieldType(i) != DataFieldMetadata.STRING_FIELD) {
				return false;
			}
		}        		
		if (!meta.getField(4).isNumeric()) {
			return false;
		}
		
		return true;
	}
	
	/**
	 * this method checks the in metadata
	 * 
	 * @param meta metadata to be checked
	 * @return true if the metadata is OK, false otherwise
	 *	 
	 */
	private boolean checkInMetadata(DataRecordMetadata meta) {
		if (meta.getFields().length < 1) {
			return false;
		}
       		
		if (meta.getFieldType(0) != DataFieldMetadata.STRING_FIELD) {
			return false;
		}
		
		// field 2 = Command line arguments (optional)
		if (meta.getFields().length >= 2 && 
				meta.getFieldType(1) != DataFieldMetadata.STRING_FIELD) {
			return false;
		}
		
		return true;
	}

	private boolean checkMetadata(ConfigurationStatus status) {
		OutputPort outPort = getOutputPort(OUTPUT_PORT);
        OutputPort errOutPort = getOutputPort(ERR_OUTPUT_PORT);
        InputPort inPort = getInputPort(INPUT_PORT);
        
        //both output metadata need to have required fields
        if (outPort != null && !checkOutMetadata(outPort.getMetadata())  || 
        	errOutPort != null && !checkOutMetadata(errOutPort.getMetadata())) {
    		ConfigurationProblem problem = new ConfigurationProblem("Wrong output metadata", 
    				Severity.ERROR, this, Priority.NORMAL);
        	status.add(problem);
        	return false;
        }
        
        //both output metadata need to have same structure (see CL-2416)
        if (outPort != null && errOutPort != null && !outPort.getMetadata().equals(errOutPort.getMetadata(), false)) {
    		ConfigurationProblem problem = new ConfigurationProblem("Both output metadata must have same structure.", 
    				Severity.ERROR, this, Priority.NORMAL);
        	status.add(problem);
        	return false;
        }
        
       	pipelineMode = isPipelineMode();
       	
        
        if (inPort != null) { 
        	DataRecordMetadata inMetadata = inPort.getMetadata();
        	if (pipelineMode) { 
            	ConfigurationProblem warning = new ConfigurationProblem("If graph name is specified as an attribute (pipeline mode), " +
            			"the input port does not need to be connected.", Severity.WARNING, this, Priority.NORMAL);
            	warning.setAttributeName(XML_GRAPH_NAME_ATTRIBUTE);
            	status.add(warning);
        	} else { // in/out mode
        		if (!checkInMetadata(inMetadata)) {
	            	ConfigurationProblem problem = new ConfigurationProblem("Wrong input metadata" +
	            			" - first field should be string", Severity.ERROR, this, Priority.NORMAL);
	            	status.add(problem);
	            	return false;
        		}        		
            }        	                    
        } else if (!pipelineMode) {
        	ConfigurationProblem problem = new ConfigurationProblem("If no graph is specified as an attribute, " +
        			"the input port must be connected", Severity.ERROR, this, Priority.NORMAL);
        	status.add(problem);
        	return false;
        }
        
        return true;
	}
	
	private boolean checkParams(ConfigurationStatus status) {
		if (!sameInstance && StringUtils.isEmpty(cloverCmdLineArgs) &&
				(isPipelineMode() || getInputPort(INPUT_PORT) == null)) {
        	ConfigurationProblem problem = new ConfigurationProblem("If the graph is " +
        			"executed in separate instance of clover, supplying the command " +
        			"line for clover is necessary (at least the -plugins argument)." +
        			"Command line arguments can be supplied by cloverCmdLineArgs attribute" +
        			"or by second field in input port (Supplying by field can be made only in in/out mode)." , 
        			Severity.ERROR, this, Priority.NORMAL);
        	status.add(problem);
        	return false;
        }
		
		return true;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#checkConfig()
	 */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
   		super.checkConfig(status);
		if(!checkInputPorts(status, 0, 1)
				|| !checkOutputPorts(status, 0, 2, false)) {
			return status;
		}

		checkMetadata(status);
        checkParams(status);
       	
        try {
            init();
        } catch (ComponentNotReadyException e) {
            ConfigurationProblem problem = new ConfigurationProblem(ExceptionUtils.getMessage(e), 
            		Severity.ERROR, this, Priority.NORMAL);
            if (!StringUtils.isEmpty(e.getAttributeName())) {
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
	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException, AttributeNotFoundException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		RunGraph runG;
					
		runG = new RunGraph(xattribs.getString(XML_ID_ATTRIBUTE),															
				xattribs.getBoolean(XML_APPEND_ATTRIBUTE,false),
				xattribs.getBoolean(XML_SAME_INSTANCE_ATTRIBUTE, true));
		
		runG.setJavaCmdLine(DEFAULT_JAVA_CMD_LINE);
		runG.setCloverRunClass(DEFAULT_GRAPH_EXEC_CLASS);
		runG.setCloverCmdLineArgs(DEFAULT_CLOVER_CMD_LINE);
								
		if (xattribs.exists(XML_OUTPUT_FILE_ATTRIBUTE)){
			runG.setOutputFile(xattribs.getStringEx(XML_OUTPUT_FILE_ATTRIBUTE, RefResFlag.SPEC_CHARACTERS_OFF));
		}
		if (xattribs.exists(XML_GRAPH_NAME_ATTRIBUTE)) {				
			runG.setGraphName(xattribs.getString(XML_GRAPH_NAME_ATTRIBUTE));
		}
		if (xattribs.exists(XML_PARAMS_TO_PASS)) {				
			runG.setParamsToPass(xattribs.getString(XML_PARAMS_TO_PASS));
		}	
		if (xattribs.exists(XML_ALTERNATIVE_JVM)) {				
			runG.setJavaCmdLine(xattribs.getString(XML_ALTERNATIVE_JVM));
		}
		if (xattribs.exists(XML_GRAPH_EXEC_CLASS)) {				
			runG.setCloverRunClass(xattribs.getString(XML_GRAPH_EXEC_CLASS));
		}	
		if (xattribs.exists(XML_CLOVER_CMD_LINE)) {
			runG.setCloverCmdLineArgs(xattribs.getStringEx(XML_CLOVER_CMD_LINE, RefResFlag.SPEC_CHARACTERS_OFF));
		}
		if (xattribs.exists(XML_IGNORE_GRAPH_FAIL)) {
			runG.setIgnoreGraphFail(xattribs.getBoolean(XML_IGNORE_GRAPH_FAIL));
		}
									
		return runG;
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

	/**
	 * Prepares file for log output of executed graph.
	 * @throws ComponentNotReadyException
	 */
	private void initGraphOutputFile() throws ComponentNotReadyException {
		if (outputFileName != null){
			try {
				File outFile = new File(FileUtils.getFile(getGraph().getRuntimeContext().getContextURL(), outputFileName));
				outFile.createNewFile();
				outputFile = new FileWriter(outFile, append);
			} catch (IOException ex) {
				throw new ComponentNotReadyException(ex);
			}
			fileWriter = new BufferedWriter(outputFile);
		}
	}
	
	/*
	 * Class that encapsulate output data record.
	 */
	private static class OutputRecordData {
		private final static int GRAPH_NAME_FIELD = 0;
		private final static int RESULT_FIELD = 1;
		private final static int DESC_FIELD = 2;
		private final static int MESSAGE_FIELD = 3;
		private final static int DURATION_FIELD = 4;
		private final static int RUNID_FIELD = 5;
		private DataRecord record = null;
		
		public OutputRecordData(DataRecord record, String graphName) {
			if (record != null) {
				record.reset();
				this.record = record;
				setGraphName(graphName);
			}
		}
		
		private void setGraphName(String graphName) {
			if (record != null) {
				record.getField(GRAPH_NAME_FIELD).setValue(graphName);
			}
		}
		
		private void setResult(String result) {
			if (record != null) {
				record.getField(RESULT_FIELD).setValue(result);
			}
		}
		
		/**
		 * Set a description if it wasn't set yet.
		 */
		private void setDescription(String description) {
			if (record == null) {
				return;
			}
			if (record.getField(DESC_FIELD).getValue() == null || 
					((CloverString)(record.getField(2).getValue())).length() < 1) {
				record.getField(DESC_FIELD).setValue(description);
			}
		}
		
		private void setMessage(String message) {
			if (record != null) {
				record.getField(MESSAGE_FIELD).setValue(message);
			}
		}

		private void setDuration(long duration) {
			if (record != null) {
				record.getField(DURATION_FIELD).setValue(duration);
			}
		}
		
		private void setRunId(long runId) {
			if (record != null && record.getNumFields()>RUNID_FIELD) {
				record.getField(RUNID_FIELD).setValue(runId);
			}
		}
	}
	
	/*
	 * Read lines from process' output and write them to the output file 
	 * and consequently parse them and fill outputRecord.
	 */
	private static class OutDataConsumer extends AbstractDataConsumer {
		private final static String ERROR_COMPONENT_REGEX = ".*(Node \\w+ finished with status: ERROR.*)";
		private final static String EXEC_TIME_REGEX = ".*WatchDog thread finished - total execution time: (\\d+).*"; 
		private final static String PHASE_FINISHED_REGEX = ".*Phase#\\s+Finished Status\\s+RunTime\\(sec\\)\\s+MemoryAllocation.*";
													//	    "INFO    [WatchDog] - 0    FINISHED_OK...";
		private final static String FINISHED_STATUS_REGEX = "\\w+\\s+\\[\\w+\\] - \\d+\\s+(\\w+).*";
		
		private OutputRecordData outputRecord;
		private Matcher matcherErrorComponent;
		private Matcher matcherExecTime;
		private Matcher matcherPhaseFinished;
		private Matcher matcherFinishedStatus;
		
		public OutDataConsumer(BufferedWriter writer, OutputRecordData outputRecord) {
			super(writer);
			this.outputRecord = outputRecord;
			
			Pattern patternErrorComponent = Pattern.compile(ERROR_COMPONENT_REGEX);
			Pattern patternExecTime = Pattern.compile(EXEC_TIME_REGEX);
			Pattern patternPhaseFinished = Pattern.compile(PHASE_FINISHED_REGEX);
			Pattern patternFinishedStatus = Pattern.compile(FINISHED_STATUS_REGEX);
			
			matcherErrorComponent = patternErrorComponent.matcher("");
			matcherExecTime = patternExecTime.matcher("");
			matcherPhaseFinished = patternPhaseFinished.matcher("");
			matcherFinishedStatus = patternFinishedStatus.matcher("");
		}
		
		@Override
		public boolean consume() throws JetelException {
			String line = readAndWriteLine();
			if (line == null) {
				return false;
			}
			
			matcherPhaseFinished.reset(line);
			if (matcherPhaseFinished.matches()) {
				if ((line = readAndWriteLine()) == null) {
					return false;
				}
				
				// expected line: "INFO [WatchDog] - 0 FINISHED_OK ..."
				matcherFinishedStatus.reset(line);
				if (matcherFinishedStatus.matches()) {
					outputRecord.setMessage(matcherFinishedStatus.group(1));
					return true;
				} else {
					outputRecord.setMessage("UNKNOWN");
					logger.warn("Finished status of graph can't be find out.");
				}
			}
			
			// expected line: "... finished with status: ERROR ..."
			matcherErrorComponent.reset(line);
			if (matcherErrorComponent.matches()) {
				outputRecord.setDescription(matcherErrorComponent.group(1));
				return true;
			}
			
			// expected line: "... WatchDog thread finished - total execution time: ..."
			matcherExecTime.reset(line);
			if (matcherExecTime.matches()) {
				long duration = 0;
				try {
					duration = Integer.parseInt(matcherExecTime.group(1));
					duration *= 1000; // convert seconds to milliseconds
				} catch (NumberFormatException nfe) {
					logger.warn("Duration of execution of graph can't be find out.");
					duration = -1;
				}
				outputRecord.setDuration(duration);
			}

			return true;
		}
	}
	
	/**
	 *  Read lines from error process' output and write them to the output file 
	 *  and consequently write them by logger. 
	 */
	private static class ErrorDataConsumer extends AbstractDataConsumer {
		private final static int MAX_ERROR_LINES_IN_LOG = 100;
		
		private Log logger;
		private final String graphName;
		private int linesRead;
		
		public ErrorDataConsumer(BufferedWriter writer, Log logger, String graphName) {
			super(writer);
			this.logger = logger;
			this.graphName = graphName;
			this.linesRead = 0;
		}
		
		@Override
		public boolean consume() throws JetelException {
			String line = readAndWriteLine();
			if (line == null) {
				return false;
			}
			
			linesRead++;
			
			if (linesRead <= MAX_ERROR_LINES_IN_LOG) {
				logger.warn("Processing " + graphName + ": " + line);
			}
			
			return true;
		}
	}
	
	private abstract static class AbstractDataConsumer implements DataConsumer {
		protected BufferedReader reader;
		protected BufferedWriter writer;
		
		public AbstractDataConsumer(BufferedWriter writer) {
			this.writer = writer;
		}
		
		/**
		 * @see org.jetel.util.exec.DataConsumer
		 */
		@Override
		public void setInput(InputStream stream) {
			reader = new BufferedReader(new InputStreamReader(stream));		
		}
		
		/**
		 * @see org.jetel.util.exec.DataConsumer
		 */
		@Override
		public void close() {
			if (writer != null) {
				try {
					writer.flush();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		private void writeLine(String line) throws JetelException {
			if (writer == null || line == null) {
				return;
			}
			try {
				synchronized (writer) {
					writer.write(line);
					writer.write("\n");
				}
			} catch (IOException e) {
				throw new JetelException("Error while writing data", e);				
			}
		}
		
		/**
		 * Reads line by reader and return it. Readed line is simultaneously written by writer.
		 * @return readed line
		 * @throws JetelException
		 */
		protected String readAndWriteLine() throws JetelException {
			String line = null;
			try {
				line = reader.readLine();
			} catch (IOException ioe) {
				throw new JetelException("Error while reading input data", ioe);
			}
			
			writeLine(line);
			return line;
		}
	}
	
	/**
	 * Quotes all given arguments if necessary.
	 * @param arguments
	 */
	private static void quoteArguments(String[] arguments) {
		int i = 0;
		for (String arg : arguments) {
			arguments[i++] = winQuote(arg);
		}
	}
	
	/**
	 * This is a just workaround for windows platform. Unfortunately the JVM is not able 
	 * to pass an argument with quoting character via Runtime.exec() method. So the arguments
	 * has to be quoted in this way.
	 * @param s
	 * @return
	 * @see http://bugs.sun.com/view_bug.do?bug_id=6468220
	 */
	private static String winQuote(String s) {
		if (!needsQuoting(s))
			return s;
		s = s.replaceAll("([\\\\]*)\"", "$1$1\\\\\"");
		s = s.replaceAll("([\\\\]*)\\z", "$1$1");
		return "\"" + s + "\"";
	}

	/**
	 * Checks whether the quoting of a command line argument is necessary.
	 * @param s
	 * @return
	 * @see #winQuote(String)
	 * @see http://bugs.sun.com/view_bug.do?bug_id=6468220
	 */
	private static boolean needsQuoting(String s) {
		int len = s.length();
		if (len == 0) // empty string have to be quoted
			return true;
		for (int i = 0; i < len; i++) {
			switch (s.charAt(i)) {
			case ' ':
			case '\t':
			case '\\':
			case '"':
				return true;
			}
		}
		return false;
	}

	@Override
	protected ComponentTokenTracker createComponentTokenTracker() {
		return new ReformatComponentTokenTracker(this);
	}

}