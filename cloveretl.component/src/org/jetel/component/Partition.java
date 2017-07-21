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

import java.nio.charset.Charset;
import java.util.Properties;

import org.jetel.component.partition.PartitionFunction;
import org.jetel.component.partition.PartitionFunctionFactory;
import org.jetel.component.partition.RangePartition;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.RecordKey;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.InputPortDirect;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPortDirect;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.tracker.BasicComponentTokenTracker;
import org.jetel.graph.runtime.tracker.ComponentTokenTracker;
import org.jetel.lookup.RangeLookupTable;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;
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
 * <li>If <b>NO </b><code>partitionClass</code> <b>NOR </b> <code>partitionSource</code>,
 *  <b>NOR </b><code>partitionKey</code> is  specified/defined, RoundRobin algorithm is used.
 * <li>If <code>partitionKey</code> <b>IS</b> specified and <b>NO</b> <code>ranges</code>
 * <b>NOR </b><code>partitionClass</code> <b>NOR </b> <code>partitionSource</code> is specified, then
 * partition by calculated hash value is used. The formula used is: <code>hashValue / MAX_HASH_VALUE * #connected_output_ports) MOD #connected_output_ports</code>
 * <li>If <b>BOTH</b> <code>partitionKey</code> and <code>ranges</code> are specified 
 * (but not <code>partitionClass</code> nor <code>partitionSource</code>), then partition by
 * range is used</i>
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
 *  <tr><td><b>charset</b><br><i>optional</i></td><td>encoding of extern source</td></tr>
 *  <tr><td><b>partitionKey</b><br><i>optional</i></td><td>key which specifies which fields (one or more) will be used for calculating hash valu
 * used for determinig output port. If ranges attribute is not defined, then partition method partition by hash will be used.</td>
 * <tr><td><b>ranges</b><br><i>optional</i></td><td>definition of intervals against which
 * partitionKey will be checked. If key's value belongs to interval then interval's order number is converted
 * to output port number. There is created {@link RangeLookupTable} based on this attribute. 
 * Each interval consist of parts, which specify start end end values of given key parts; each part 
 * is pair is built from two values separated by "," and enclosed with brackes: "(" or ")" if the 
 * value is to be excluded from interval and "<" or ">" if the value is to be included to the interval.<br> 
 * Eg. for partionKey:<i>EmployeeID;BirthDate</i> and ranges: <i><1,9)(,31/12/1959>;<9,)(,31/12/1959>;<1,9)(31/12/1959,);<9,)(31/12/1959,)</i>
 * there are specified four output flows <ul>
 * <li> 0 - EmployeeID between 1 included and 9 excluded, and BirthDate at most 31/12/1959</li>
 * <li> 1 - EmployeeID greater or equal 9, and BirthDate at most 31/12/1959</li>
 * <li> 2 - EmployeeID between 1 included and 9 excluded, and BirthDate greater then 31/12/1959</li>
 * <li> 3 - EmployeeID greater or equal 9, and BirthDate greater then 31/12/1959</li>
 * </ul> When there are connected more output ports then ranges defined, records which don't
 * have corresponding interval will be sent to last connected ouput port.
 *  </tr>
 *  <tr><td><b>useI18N</b><br><i>optional</i></td><td>true/false perform comparing according to national rules - e.g. Czech or German handling of characters like "i","??". Default
 *  is false.<br>Use it only if you are comparing data according to key which can contain accented characters or
 *  you want sorter to follow certain locale specific rules.</td></tr>
 *  <tr><td><b>locale</b><br><i>optional</i></td><td>locale to be used when sorting using I18N rules. If not specified, then system
 *  default is used.<br><i>Example: "fr"</i></td></tr>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node id="PARTITION_BY_KEY" type="PARTITION" partitionKey="age"/&gt;</pre>
 *  <pre>&lt;Node id="PARTITION_BY_RANGE" type="PARTITION" partitionKey="EmployeeID;BirthDate" 
 *  ranges="&lt;1,9)(,31/12/1959&gt;;&lt;9,)(,31/12/1959&gt;;&lt;1,9)(31/12/1959,);&lt;9,)(31/12/1959,)"/&gt;</pre>
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
 */
public class Partition extends Node {

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "PARTITION";

	/**
	 * This delimiter is used to separate particular ranges i.e. <1,2>;<5,6>
	 */
	public final static String RANGES_DELIMITER = ";";
	
	private final static int READ_FROM_PORT=0;
	
	private String[] partitionKeyNames = null;
	private String[] partitionRanges = null;
	private String partitionClass = null;
	private String partitionSource = null;
	private String partitionURL = null;
	private String charset = null;
	private boolean useI18N;
	private String locale = null;

	private RecordKey partitionKey;

	//	 instantiate proper partitioning function
	private PartitionFunction partitionFce;
	private Properties parameters;

	public static final String XML_PARTITIONKEY_ATTRIBUTE = "partitionKey";
	private static final String XML_RANGES_ATTRIBUTE = "ranges";
	private static final String XML_PARTITIONCLASS_ATTRIBUTE = "partitionClass";
	private static final String XML_PARTIONSOURCE_ATTRIBUTE = "partitionSource";
	private static final String XML_PARTITIONURL_ATTRIBUTE = "partitionURL";
	private static final String XML_CHARSET_ATTRIBUTE = "charset";
    private static final String XML_USE_I18N_ATTRIBUTE = "useI18N";
    private static final String XML_LOCALE_ATTRIBUTE = "locale";

    /**
	 *  Constructor for the Partition object
	 *
	 * @param  id         Description of the Parameter
	 */
	public Partition(String id, PartitionFunction fce) {
		super(id);
		partitionFce=fce;
	}
	
	/**
	 *  Constructor for the Partition object
	 *  
	 * @param id
	 * @param partition partition source
	 * @param partitionClasspartition class name
	 * @param partitionURL URL to the extern partition class source
	 * @param partitionKey partition key
	 * @param partitionRanges ranges for partition key
	 */
	public Partition(String id, TransformationGraph graph) {
		super(id, graph);
	}

	/**
	 * Returns the name of the attribute which contains transformation
	 * 
	 * @return the name
	 */
	public static String getTransformAttributeName() {
		return XML_PARTIONSOURCE_ATTRIBUTE;
	}

 	
	@Override
	public Result execute() throws Exception {
		InputPort inPort;
		inPort=getInputPort(READ_FROM_PORT);
		OutputPortDirect[] outPorts = (OutputPortDirect[]) getOutPorts().toArray(new OutputPortDirect[0]);
		
		
		if (partitionFce.supportsDirectRecord()){
			executeDirect((InputPortDirect)inPort,outPorts);
		}else{
			executeNonDirect((InputPortDirect)inPort,outPorts); 
		}
		
		broadcastEOF();
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#preExecute()
	 */
	@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		partitionFce.preExecute();
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#postExecute(org.jetel.graph.TransactionMethod)
	 */
	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
		partitionFce.postExecute();
	}
	
	private void executeNonDirect(InputPortDirect inPort,
			OutputPortDirect[] outPorts) throws Exception {
		DataRecord inRecord = DataRecordFactory.newRecord(inPort.getMetadata());
		inRecord.init();
		CloverBuffer inRecordDirect = CloverBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);

		while (runIt) {
			if (!inPort.readRecordDirect(inRecordDirect)) {
				break;
			}

			inRecord.deserialize(inRecordDirect);
			inRecordDirect.rewind();

			int portNo = -1;

			try {
				portNo = partitionFce.getOutputPort(inRecord);
			} catch (Exception exception) {
				portNo = partitionFce.getOutputPortOnError(exception, inRecord);
			}

			try {
				outPorts[portNo].writeRecordDirect(inRecordDirect);
			} catch (ArrayIndexOutOfBoundsException e) {
				if (portNo == RangePartition.NONEXISTENT_REJECTED_PORT) {
					throw new JetelException(
							"Not found output port for record:\n" + inRecord);
				} else {
					throw new JetelException(
							"Not found output port for record:\n" + inRecord
									+ "Port number " + portNo
									+ " not connected", e);
				}
			}
			SynchronizeUtils.cloverYield();
		}
	}

	private void executeDirect(InputPortDirect inPort, OutputPortDirect[] outPorts) throws Exception {
		CloverBuffer inRecord = CloverBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);

		while (runIt) {
			if (!inPort.readRecordDirect(inRecord)) {
				break;
			}

			int portNo = -1;

			try {
				portNo = partitionFce.getOutputPort(inRecord);
			} catch (Exception exception) {
				portNo = partitionFce.getOutputPortOnError(exception, inRecord);
			}

			try {
				outPorts[portNo].writeRecordDirect(inRecord);
			} catch (ArrayIndexOutOfBoundsException e) {
				if (portNo == RangePartition.NONEXISTENT_REJECTED_PORT) {
					throw new JetelException("Not found output port for record:\n" + inRecord);
				} else {
					throw new JetelException("Not found output port for record:\n" + inRecord + 
							"Port number " + portNo + " not connected",e);
				}
			}

			SynchronizeUtils.cloverYield();
		}
	}

	/**
	 *  Description of the Method
	 *
	 * @exception  ComponentNotReadyException  Description of the Exception
	 * @since                                  April 4, 2002
	 */
	@Override
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
		DataRecordMetadata inMetadata = getInputPort(0).getMetadata();
		// initialize partition key - if defined
	    if (partitionKeyNames != null) {
			partitionKey = new RecordKey(partitionKeyNames, inMetadata);
		}
		if (partitionKey != null) {
			try {
				partitionKey.init();
			} catch (Exception e) {
				throw new ComponentNotReadyException(e);
			}
		}
		
		//create partition function if still is not created
		if (partitionFce == null) {
			partitionFce = getPartitionFunctionFactory().createPartitionFunction(partitionSource, partitionClass, partitionURL);
		}
		
		partitionFce.init(outPorts.size(), partitionKey, parameters, inMetadata);
	}

	private PartitionFunctionFactory getPartitionFunctionFactory() {
		PartitionFunctionFactory partitionFceFactory = new PartitionFunctionFactory();
		partitionFceFactory.setNode(this);
		partitionFceFactory.setMetadata(getInputPort(0).getMetadata());
		partitionFceFactory.setPartitionKeyNames(partitionKeyNames);
		partitionFceFactory.setPartitionRanges(partitionRanges);
		partitionFceFactory.setCharset(charset);
		partitionFceFactory.setLocale(locale);
		partitionFceFactory.setUseI18N(useI18N);
		return partitionFceFactory;
	}

    public static Node fromXML(TransformationGraph transformationGraph, Element xmlElement) throws XMLConfigurationException, AttributeNotFoundException {
        Partition partition = null;

        ComponentXMLAttributes componentAttributes = new ComponentXMLAttributes(xmlElement, transformationGraph);

    	partition = new Partition(componentAttributes.getString(XML_ID_ATTRIBUTE), transformationGraph);

    	partition.loadAttributesFromXML(componentAttributes);

        return partition;
    }
    
	protected void loadAttributesFromXML(ComponentXMLAttributes xattribs) throws XMLConfigurationException {
		try {
			String[] key = xattribs.exists(XML_PARTITIONKEY_ATTRIBUTE) ? 
					StringUtils.split(xattribs.getString(XML_PARTITIONKEY_ATTRIBUTE)) :
					null;
			setPartitionKeyNames(key);
			String[] ranges = xattribs.exists(XML_RANGES_ATTRIBUTE) ?
					StringUtils.split(xattribs.getString(XML_RANGES_ATTRIBUTE), RANGES_DELIMITER) :
						null;	
			setPartitionRanges(ranges);
			setPartitionSource(xattribs.getStringEx(XML_PARTIONSOURCE_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF));
			setPartitionClass(xattribs.getString(XML_PARTITIONCLASS_ATTRIBUTE, null));
			setPartitionURL(xattribs.getStringEx(XML_PARTITIONURL_ATTRIBUTE, null, RefResFlag.URL));
			setFunctionParameters(xattribs.attributes2Properties(
					new String[]{XML_ID_ATTRIBUTE,XML_PARTIONSOURCE_ATTRIBUTE,
							XML_PARTITIONCLASS_ATTRIBUTE, XML_PARTITIONURL_ATTRIBUTE, 
							XML_PARTITIONKEY_ATTRIBUTE, XML_RANGES_ATTRIBUTE, 
							XML_CHARSET_ATTRIBUTE}));
			if (xattribs.exists(XML_CHARSET_ATTRIBUTE)) {
				setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE));
			}
			setUseI18N(xattribs.getBoolean(XML_USE_I18N_ATTRIBUTE, false));
			if (xattribs.exists(XML_LOCALE_ATTRIBUTE)) {
				setLocale(xattribs.getString(XML_LOCALE_ATTRIBUTE));
			}
        } catch (AttributeNotFoundException exception) {
            throw new XMLConfigurationException("Missing a required attribute!", exception);
        } catch (Exception exception) {
            throw new XMLConfigurationException("Error creating the component!", exception);
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
	 
		if (!checkInputPorts(status, 1, 1)
				|| !checkOutputPorts(status, 1, Integer.MAX_VALUE)) {
			return status;
		}

   		if (charset != null && !Charset.isSupported(charset)) {
           	status.add(new ConfigurationProblem(
               		"Charset "+charset+" not supported!", 
               		ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
         }

        checkMetadata(status, getInMetadata(), getOutMetadata());

        DataRecordMetadata inMetadata = getInputPort(0).getMetadata();
        try {
        	
    	    if (partitionKeyNames != null) {
    			partitionKey = new RecordKey(partitionKeyNames, inMetadata);
    		}
    		if (partitionKey != null) {
    			try {
    				partitionKey.init();
    			} catch (Exception e) {
    				throw new ComponentNotReadyException(this, 
    						XML_PARTITIONKEY_ATTRIBUTE, e);
    			}
    		}
        } catch (ComponentNotReadyException e) {
            ConfigurationProblem problem = new ConfigurationProblem(ExceptionUtils.getMessage(e), ConfigurationStatus.Severity.WARNING, this, ConfigurationStatus.Priority.NORMAL);
            if(!StringUtils.isEmpty(e.getAttributeName())) {
                problem.setAttributeName(e.getAttributeName());
            }
            status.add(problem);
        }

        // transformation source for checkconfig
		if (partitionFce == null) {
			getPartitionFunctionFactory().checkConfig(status, partitionSource, partitionClass, partitionURL);
		}
		
        return status;
    }
	
	@Override
	public String getType(){
		return COMPONENT_TYPE;
	}

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public String getLocale() {
		return locale;
	}

	public void setLocale(String locale) {
		this.locale = locale;
	}

	public boolean isUseI18N() {
		return useI18N;
	}

	public void setUseI18N(boolean useI18N) {
		this.useI18N = useI18N;
	}

	/**
	 * @return the partitionSource
	 */
	public String getPartitionSource() {
		return partitionSource;
	}

	/**
	 * @param partitionSource the partitionSource to set
	 */
	public void setPartitionSource(String partitionSource) {
		this.partitionSource = partitionSource;
	}

	/**
	 * @return the partitionClass
	 */
	public String getPartitionClass() {
		return partitionClass;
	}

	/**
	 * @param partitionClass the partitionClass to set
	 */
	public void setPartitionClass(String partitionClass) {
		this.partitionClass = partitionClass;
	}

	/**
	 * @return the partitionKeyNames
	 */
	public String[] getPartitionKeyNames() {
		return partitionKeyNames;
	}

	/**
	 * @param partitionKeyNames the partitionKeyNames to set
	 */
	public void setPartitionKeyNames(String[] partitionKeyNames) {
		this.partitionKeyNames = partitionKeyNames;
	}

	/**
	 * @return the partitionRanges
	 */
	public String[] getPartitionRanges() {
		return partitionRanges;
	}

	/**
	 * @param partitionRanges the partitionRanges to set
	 */
	public void setPartitionRanges(String[] partitionRanges) {
		this.partitionRanges = partitionRanges;
	}

	/**
	 * @return the partitionURL
	 */
	public String getPartitionURL() {
		return partitionURL;
	}

	/**
	 * @param partitionURL the partitionURL to set
	 */
	public void setPartitionURL(String partitionURL) {
		this.partitionURL = partitionURL;
	}

	@Override
	protected ComponentTokenTracker createComponentTokenTracker() {
		return new BasicComponentTokenTracker(this);
	}

}

