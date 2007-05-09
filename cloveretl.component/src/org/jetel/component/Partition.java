/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Enumeration;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.partition.HashPartition;
import org.jetel.component.partition.PartitionFunction;
import org.jetel.component.partition.PartitionTL;
import org.jetel.component.partition.RangePartition;
import org.jetel.component.partition.RoundRobinPartition;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.HashKey;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.ByteBufferUtils;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.DynamicJavaCode;
import org.jetel.util.FileUtils;
import org.jetel.util.StringUtils;
import org.jetel.util.SynchronizeUtils;
import org.w3c.dom.Element;

/**
 *  <h3>Partition Component</h3> <!-- Partitions input data into
 * set of partitions (each connected output port becomes one partition.
 * Data is partitioned using different algorithms.  -->
 *
 * <table border="1">
 * <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>Partition</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Partitions data into distinct flows. Each connected port becomes
 * one flow. For each record read, the partition function chosen denotes which output
 * flow (port) will be the record sent to.<br>
 * Which partition algorithm becomes active depends on following:
 * <ul>
 * <li>If <code>partitionClass</code> or <code>partitionSource</code> is defined, there
 * is used specified function for partition
 * <li>If <b>NO </b><code>partitionClass</code> <b>NOR </b>, <code>partitionSource</code>,
 *  <b>NO </b><code>partitionKey</code> is  specified/defined, RoundRobin algorithm is used.
 * <li>If <code>partitionKey</code> <b>IS</b> specified and <b>NO</b> <code>ranges</code>
 * <b>NOR </b><code>partitionClass</code> <b>NOR </b> <code>partitionSource</code> is specified, then
 * partition by calculated hash value is used. The formula used is: <code>hashValue / MAX_HASH_VALUE * #connected_output_ports) MOD #connected_output_ports</code>
 * <li>If <b>BOTH</b> <code>partitionKey</code> and <code>ranges</code> are specified 
 * (but not <code>partitionClass</code> nor <code>partitionSource</code>), then partition by
 * range is used - <i>partitionKey's</i> value is sequentially compared with defined range boundaris. If
 * the value is less or equal to specified boundary, then the record is sent out through the port corresponding
 * to that boundary.<br><i>Note: when boundaries are used, only 1 field can be specified as partitionKey.</i>
 * </ul>
 * </td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0]- input records</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>[0..n] - one or more output ports connected</td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"PARTITION"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>partitionClass</b><br><i>optional</i></td><td>name of the class to be
 *   used for partioning data
 *  <tr><td><b>partitionSource</b><br><i>optional</i></td><td>partition function for partition dataset. Can be java code or in CloverETL transform language
 *  <tr><td><b>partitonURL</b></td><td>path to the file with parttion code</td></tr>
 *  <tr><td><b>charset</b><i>optional</i></td><td>encoding of extern source</td></tr>
 *  <tr><td><b>partitionKey</b><br><i>optional</i></td><td>key which specifies which fields (one or more) will be used for calculating hash valu
 * used for determinig output port. If ranges attribute is not defined, then partition method partition by hash will be used.</td>
 * <tr><td><b>ranges</b><br><i>optional</i></td><td>definition of intervals against which
 * partitionKey will be checked. If key's value belongs to interval then interval's order number is converted
 * to output port number. <br>If this option is used, the partitioning is range partitioning and partition key can
 * be composed of ONE field only (actually 2nd and additional fields will be ignored).<br>
 * <i>Note:Boundaries are first sorted in ascending order, then checked.When checking interval boundaries, the
 * &lt;= operator is used.</i></td>
 *  </tr>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node id="PARTITION_BY_KEY" type="PARTITION" partitionKey="age"/&gt;</pre>
 *  <pre>&lt;Node id="PARTITION_BY_RANGE" type="PARTITION" partitionKey="age" ranges="18;30;60;80"/&gt;</pre>
 *  <pre>&lt;Node id="PARTITION" type="PARTITION"&gt;
 *  &lt;attr name="partitionSource"&gt;
 *  //#TL
 *  function getOutputPort(){
 *  if ($EmployeeID .lt. 3) return 0
 *  else if ($EmployeeID .lt. 5) return 1
 *  else return 2
 *  }
 *  &lt;/attr&gt;
 *  &lt;/Node&gt;
 *
 * @author      dpavlis
 * @since       February 28, 2005
 * @revision    $Revision$
 */
public class Partition extends Node {

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "PARTITION";

	private final static int READ_FROM_PORT=0;
	
	private String[] partitionKeyNames = null;
	private String[] partitionRanges = null;
	private String partitionClass = null;
	private String partitionSource = null;
	private String charset = null;

	private RecordKey partitionKey;
	private HashKey hashKey;

	//	 instantiate proper partitioning function
	private PartitionFunction partitionFce;
	private Properties parameters;

	private static final String XML_PARTITIONKEY_ATTRIBUTE = "partitionKey";
	private static final String XML_RANGES_ATTRIBUTE = "ranges";
	private static final String XML_PARTITIONCLASS_ATTRIBUTE = "partitionClass";
	private static final String XML_PARTIONSOURCE_ATTRIBUTE = "partitionSource";
	private static final String XML_PARTITIONURL_ATTRIBUTE = "partitionURL";
	private static final String XML_CHARSET_ATTRIBUTE = "charset";

	static Log logger = LogFactory.getLog(Partition.class);
	public static final Pattern PATTERN_TL_CODE = Pattern.compile("function\\s+" + PartitionTL.GETOUTPUTPORT_FUNCTION_NAME); 


    /**
	 *  Constructor for the Partition object
	 *
	 * @param  id         Description of the Parameter
	 */
	public Partition(String id, PartitionFunction fce) {
		super(id);
		partitionFce=fce;
	}
	
	public Partition(String id) {
		this(id, null);
	}


    /**
     * @param partitionKeyNames The partitionKeyNames to set.
     */
    public void setPartitionKeyNames(String[] partitionKeyNames) {
        this.partitionKeyNames = partitionKeyNames;
    }
	
	/**
	 * Method which can be used to set custom partitioning function
	 * 
	 * @param fce class implementing PartitionFunction interface
	 */
	public void setPartitionFunction(PartitionFunction fce){
	    this.partitionFce=fce;
	}

	public void setPartitionFunction(String source){
	    this.partitionSource=source;
	}

    public void setPartitionFunction(ReadableByteChannel partitionReader) throws ComponentNotReadyException{
		try {
			ByteBuffer buffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
			CharsetDecoder decoder = charset != null ? decoder = Charset.forName(charset).newDecoder(): 
				Charset.forName(Defaults.DataParser.DEFAULT_CHARSET_DECODER).newDecoder();
			int pos;
			partitionSource = new String(); 
			do {
				pos = ByteBufferUtils.reload(buffer, partitionReader);
				buffer.flip();
				partitionSource += decoder.decode(buffer).toString();
				buffer.rewind();
			} while (pos == Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		} catch (IOException e) {
			ComponentNotReadyException ex = new ComponentNotReadyException(this, "Can't read extern transformation", e);
			ex.setAttributeName(XML_PARTIONSOURCE_ATTRIBUTE);
			throw ex;
		}
}

	
	@Override
	public Result execute() throws Exception {
		InputPort inPort;
		OutputPort[] outPorts;

		inPort=getInputPort(READ_FROM_PORT);
		//get array of all output ports defined/connected - use collection Collection - getOutPorts();
		outPorts = (OutputPort[]) getOutPorts().toArray(new OutputPort[0]);
		//create array holding incoming records
		DataRecord inRecord = new DataRecord(inPort.getMetadata());
		inRecord.init();
		while (inRecord != null && runIt) {
			inRecord = inPort.readRecord(inRecord);
			if (inRecord != null) {
				outPorts[partitionFce.getOutputPort(inRecord)]
						.writeRecord(inRecord);
			}
			SynchronizeUtils.cloverYield();
		}
		broadcastEOF();
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}
	


	/**
	 *  Description of the Method
	 *
	 * @exception  ComponentNotReadyException  Description of the Exception
	 * @since                                  April 4, 2002
	 */
	public void init() throws ComponentNotReadyException {
		super.init();
		// initialize partition key - if defined
	    if (partitionKeyNames != null) {
			partitionKey = new RecordKey(partitionKeyNames,
					getInputPort(0).getMetadata());
		}
		if (partitionKey != null) {
			try {
				partitionKey.init();
			} catch (Exception e) {
				throw new ComponentNotReadyException(e.getMessage());
			}
		}
		//create parttion function if still is not created
		if (partitionFce == null) {
			partitionFce = createPartitionDynamic(partitionSource);
		}else{
			partitionClass = partitionFce.getClass().getName();
		}
		partitionFce.init(outPorts.size(),partitionKey);
	}

	/**
	 * Method for creation parttion function from CloverETL language or java source
	 * 
	 * @param psorCode source code
	 * @return Partition function
	 * @throws ComponentNotReadyException
	 */
	private PartitionFunction createPartitionDynamic(String psorCode) throws ComponentNotReadyException {
		//check if source code is in CloverETL format
		if (psorCode.contains(WrapperTL.TL_TRANSFORM_CODE_ID) ||
				PATTERN_TL_CODE.matcher(psorCode).find()) {
			PartitionTL function =  new PartitionTL(psorCode, 
					getInputPort(0).getMetadata(), parameters, logger);
			function.setGraph(getGraph());
			return function;
		}else{//get partition function form java code
			DynamicJavaCode dynCode = new DynamicJavaCode(psorCode, this.getClass().getClassLoader());
	        dynCode.setCaptureCompilerOutput(true);
	        logger.info(" (compiling dynamic source) ");
	        // use DynamicJavaCode to instantiate transformation class
	        Object transObject = null;
	        try {
	            transObject = dynCode.instantiate();
	        } catch (RuntimeException ex) {
	            logger.debug(dynCode.getCompilerOutput());
	            logger.debug(dynCode.getSourceCode());
	            throw new ComponentNotReadyException("Parttion code is not compilable.\n" + "Reason: " + ex.getMessage());
	        }
	        if (transObject instanceof PartitionFunction) {
	            return (PartitionFunction)transObject;
	        } else {
	            throw new ComponentNotReadyException("Provided partition class doesn't implement required interface.");
	        }
		}
    }

	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Returned Value
	 * @since     May 21, 2002
	 */
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);

		if (partitionKeyNames != null) {
			StringBuffer buf = new StringBuffer(partitionKeyNames[0]);
			for (int i=1; i<partitionKeyNames.length; i++) {
			buf.append(Defaults.Component.KEY_FIELDS_DELIMITER + partitionKeyNames[i]);
			}
			
			xmlElement.setAttribute(XML_PARTITIONKEY_ATTRIBUTE,buf.toString());
		}else if (partitionClass != null){
			xmlElement.setAttribute(XML_PARTITIONCLASS_ATTRIBUTE, partitionClass);
		}else{
			xmlElement.setAttribute(XML_PARTIONSOURCE_ATTRIBUTE, partitionSource);
		}
		
		if (partitionRanges != null) {
			StringBuffer buf = new StringBuffer(partitionRanges[0]);
			for (int i=1; i<partitionRanges.length; i++) {
				buf.append(Defaults.Component.KEY_FIELDS_DELIMITER + partitionRanges[i]);
			}
			
			xmlElement.setAttribute(XML_RANGES_ATTRIBUTE,buf.toString());
		}
		Enumeration propertyAtts = parameters.propertyNames();
		while (propertyAtts.hasMoreElements()) {
			String attName = (String)propertyAtts.nextElement();
			xmlElement.setAttribute(attName,parameters.getProperty(attName));
		}
	}


	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @since           May 21, 2002
	 */
	   public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		Partition partition;
		PartitionFunction partitionFce = null;
		try {
		    partition = new Partition(xattribs.getString(XML_ID_ATTRIBUTE));
		    if (xattribs.exists(XML_PARTITIONCLASS_ATTRIBUTE)) {//load class with partition function
		        try {
		        	//TODO move creating function to init method
		        	partitionFce =  (PartitionFunction)Class.forName(xattribs.getString(XML_ID_ATTRIBUTE)).newInstance();
		        }catch (InstantiationException ex){
		            throw new ComponentNotReadyException("Can't instantiate partition function class: "+ex.getMessage());
		        }catch (IllegalAccessException ex){
		            throw new ComponentNotReadyException("Can't instantiate partition function class: "+ex.getMessage());
		        }catch (ClassNotFoundException ex) {
		            throw new ComponentNotReadyException("Can't find specified partition function class: " + xattribs.getString(XML_ID_ATTRIBUTE));
		        }
		    }else if (xattribs.exists(XML_PARTIONSOURCE_ATTRIBUTE)){//set source for parttion function to load dynamic in init() method
		    	partition.setPartitionFunction(xattribs.getString(XML_PARTIONSOURCE_ATTRIBUTE));
		    }else if (xattribs.exists(XML_PARTITIONURL_ATTRIBUTE)){
		    	if (xattribs.exists(XML_CHARSET_ATTRIBUTE)) {
		    		partition.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE));
		    	}
		    	partition.setPartitionFunction(FileUtils.getReadableChannel(
		    			partition.getGraph().getProjectURL(), xattribs.getString(XML_PARTITIONURL_ATTRIBUTE)));
		    }else{//set proper standard partition function
			    if (xattribs.exists(XML_RANGES_ATTRIBUTE)) {
			    	partitionFce = new RangePartition(xattribs.getString(XML_RANGES_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
			    }
			    if (xattribs.exists(XML_PARTITIONKEY_ATTRIBUTE)){
						partition.setPartitionKeyNames(xattribs.getString(XML_PARTITIONKEY_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
						if (partitionFce == null){
							partitionFce = new HashPartition();
						}
			    }
			    if (partitionFce == null){
			    	partitionFce = new RoundRobinPartition();
			    }
		    }
			if (partitionFce != null) {
				partition.setPartitionFunction(partitionFce);
			}	
			partition.setFunctionParameters(xattribs.attributes2Properties(
					new String[]{XML_ID_ATTRIBUTE,XML_PARTIONSOURCE_ATTRIBUTE,
							XML_PARTITIONCLASS_ATTRIBUTE, XML_PARTITIONKEY_ATTRIBUTE,
							XML_RANGES_ATTRIBUTE}));
			return partition;
		} catch (Exception ex) {
	           throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}
	}

		/**
	     * @param functionParameters The functionParameters to set.
	     */
	    public void setFunctionParameters(Properties parameters) {
	        this.parameters = parameters;
	    }
	    

	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Return Value
	 */
        @Override
        public ConfigurationStatus checkConfig(ConfigurationStatus status) {
    		super.checkConfig(status);
   		 
    		checkInputPorts(status, 1, 1);
            checkOutputPorts(status, 1, Integer.MAX_VALUE);

            try {
            	
        	    if (partitionKeyNames != null) {
        			partitionKey = new RecordKey(partitionKeyNames,
        					getInputPort(0).getMetadata());
        		}
        		if (partitionKey != null) {
        			try {
        				partitionKey.init();
        			} catch (Exception e) {
        				throw new ComponentNotReadyException(e.getMessage());
        			}
        		}
            	
            	
//                init();
//                free();
            } catch (ComponentNotReadyException e) {
                ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
                if(!StringUtils.isEmpty(e.getAttributeName())) {
                    problem.setAttributeName(e.getAttributeName());
                }
                status.add(problem);
            }
            
            return status;
        }
	
	public String getType(){
		return COMPONENT_TYPE;
	}

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}


	
}

