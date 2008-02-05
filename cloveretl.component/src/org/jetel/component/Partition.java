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

import java.util.Arrays;
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
import org.jetel.data.parser.DelimitedDataParser;
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
import org.jetel.lookup.RangeLookupTable;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.compile.DynamicJavaCode;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
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
 *  <tr><td><b>useI18N</b><br><i>optional</i></td><td>true/false perform comparing according to national rules - e.g. Czech or German handling of characters like "i","í". Default
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
 * @revision    $Revision$
 */
public class Partition extends Node {

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "PARTITION";

	private final static int READ_FROM_PORT=0;
	private final static char EQUAL = '=';
	public final static char COMMA = ',';
	public final static char START_OPENED = '(';
	public final static char START_CLOSED = '<';
	public final static char END_OPENED = ')';
	public final static char END_CLOSED = '>';
	private static final String PORT_NO_FIELD_NAME = "portNo"; 
	
	private String[] partitionKeyNames = null;
	private String[] partitionRanges = null;
	private String partitionClass = null;
	private String partitionSource = null;
	private String partitionURL = null;
	private String charset = null;
	private boolean useI18N;
	private String locale = null;
	protected boolean[] startInclude;
	protected boolean[] endInclude;

	private RecordKey partitionKey;
	private HashKey hashKey;

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
	public Partition(String id, String partition, String partitionClass, 
			String partitionURL, String[] partitionKey, String[] partitionRanges) {
		super(id);
        this.partitionSource = partition;
        this.partitionClass = partitionClass;
        this.partitionURL = partitionURL;
        this.partitionKeyNames = partitionKey;
        this.partitionRanges = partitionRanges;
	}


 	
	@Override
	public Result execute() throws Exception {
		InputPort inPort;
		OutputPort[] outPorts;

		int portNo;
		inPort=getInputPort(READ_FROM_PORT);
		//get array of all output ports defined/connected - use collection Collection - getOutPorts();
		outPorts = (OutputPort[]) getOutPorts().toArray(new OutputPort[0]);
		//create array holding incoming records
		DataRecord inRecord = new DataRecord(inPort.getMetadata());
		inRecord.init();
		while (inRecord != null && runIt) {
			inRecord = inPort.readRecord(inRecord);
			if (inRecord != null) {
				portNo = partitionFce.getOutputPort(inRecord);
				try {
					outPorts[portNo].writeRecord(inRecord);
				} catch (ArrayIndexOutOfBoundsException e) {
					if (portNo == RangePartition.NONEXISTENT_REJECTED_PORT) {
						throw new JetelException("Not found output port for record:\n" + inRecord);
					}else{
						throw new JetelException("Not found output port for record:\n" + inRecord + 
								"Port number " + portNo + " not connected",e);
					}
				}
			}
			SynchronizeUtils.cloverYield();
		}
		broadcastEOF();
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}
	
	/**
	 * This method prepares data for DelimitedDataParser in RangeLookupTable 
	 * 
	 * @param ranges ranges defined by user
	 * @return data prepared for lookup table (0=start00,end00,start01,end01,...,start0n,end0n;1=start10,end10,start11,end11,...;m=startmn,endmn;)
	 */
	private String analizeRangesString(String[] ranges) throws ComponentNotReadyException{
		StringBuilder rangesData = new StringBuilder();
		int intervalNumbers = ranges[0].split("[(<]").length - 1;
		if (intervalNumbers == 0) {//old reprezentation: x1;x2;..;xn
			//corresponding new form: (,x1>;(x1,x2>;(x2,.....,xn>;(xn,>
			String[] newRanges = new String[ranges.length + 1];
			String start = "";
			String end = ranges[0];
			for (int i=0;i<newRanges.length; i++){
				newRanges[i] = START_OPENED + start + COMMA + end + END_CLOSED;
				start = end;
				end = i+1 < newRanges.length - 1 ? ranges[i+1] : "";
			}
			intervalNumbers = 1;
			ranges = newRanges;
		}
		//prepare default start/endInclude
		startInclude = new boolean[intervalNumbers];
		endInclude = new boolean[intervalNumbers];
		Arrays.fill(startInclude, true);
		Arrays.fill(endInclude, false);
		
		int intervalNumber;
		char c;
		//remove brackets, set requested start/endInclude
		for (int i=0; i < ranges.length; i++){
			intervalNumber = 0;
			ranges[i] = ranges[i].trim();
			rangesData.append(i);
			rangesData.append(EQUAL);
			for (int index = 0; index < ranges[i].length(); index++){
				c = ranges[i].charAt(index);
				switch (c) {
				case START_OPENED:
					if (i == 0) {//first '(' - set startInclude to "false"
						startInclude[intervalNumber] = false;
					}else if (startInclude[intervalNumber]) {//there was "<" for this interval before
						logger.warn("startInclude[" + intervalNumber + "] was set to \"true\" before");
					}
					if (intervalNumber != 0){
						rangesData.append(COMMA);
					}
					break;
				case START_CLOSED:
					if (!startInclude[intervalNumber]) {//there was "(" for this interval before
						logger.warn("startInclude[" + intervalNumber + "] was set to \"false\" before");
					}
					break;
				case END_OPENED:
					if (endInclude[intervalNumber]) {//there was ">" for this interval before
						logger.warn("endInclude[" + intervalNumber + "] was set to \"true\" before");
					}
					intervalNumber++;
					break;
				case END_CLOSED:
					if (i == 0) {//first '>' - set endInclude to "true"
						endInclude[intervalNumber] = true;
					}else if (!endInclude[intervalNumber]) {//there was ")" for this interval before
						logger.warn("endInclude[" + intervalNumber + "] was set to \"false\" before");
					}
					intervalNumber++;
					break;
				default:
					rangesData.append(c);
					break;
				}
			}
			rangesData.append(Defaults.Component.KEY_FIELDS_DELIMITER);
		}
		
		return rangesData.toString();
	}

	/**
	 *  Description of the Method
	 *
	 * @exception  ComponentNotReadyException  Description of the Exception
	 * @since                                  April 4, 2002
	 */
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
				throw new ComponentNotReadyException(e.getMessage());
			}
		}
		//create parttion function if still is not created
		if (partitionFce != null) {
			partitionClass = partitionFce.getClass().getName();
		}else if (partitionClass != null){
			partitionFce = createPartitionFce(partitionClass);
		}else {
			if (partitionSource == null && partitionURL != null) {
				partitionSource = FileUtils.getStringFromURL(
						getGraph().getProjectURL(), partitionURL, 
						charset);
			}
			if (partitionSource != null) {
				partitionFce = createPartitionDynamic(partitionSource);
			}else if (partitionRanges != null){
				//create RangePartition function from parttionKey and partitionRanges
				String rangesData = analizeRangesString(partitionRanges);
				//create metadata for RangeLookupTable
				DataRecordMetadata lookupMetadata = new DataRecordMetadata("lookup_metadata",
						DataRecordMetadata.DELIMITED_RECORD);
				lookupMetadata.addField(new DataFieldMetadata(PORT_NO_FIELD_NAME, 
						DataFieldMetadata.INTEGER_FIELD, String.valueOf(EQUAL)));
				DataFieldMetadata keyField, startField, endField;
				String[] startFields = new String[partitionKeyNames.length];
				String[] endFields = new String[partitionKeyNames.length];
				//create startField and endField for each key field
				for (int i=0; i<partitionKeyNames.length; i++) {
					keyField = inMetadata.getField(partitionKeyNames[i]);
					startField = keyField.duplicate();
					startField.setName(keyField.getName() + "_start");
					startField.setDelimiter(String.valueOf(COMMA));
					startFields[i] = startField.getName(); 
					endField = keyField.duplicate();
					endField.setName(keyField.getName() + "_end");
					endField.setDelimiter(i == partitionKeyNames.length -1 ? 
							Defaults.Component.KEY_FIELDS_DELIMITER : String.valueOf(COMMA));
					endFields[i] = endField.getName();
					lookupMetadata.addField(startField);
					lookupMetadata.addField(endField);
				}
				//create RangeLookupTable 
				RangeLookupTable table = new RangeLookupTable(this.getId() + "_lookupTable", 
						lookupMetadata, startFields, endFields, new DelimitedDataParser());
				table.setUseI18N(useI18N);
				table.setLocale(locale);
				table.setData(rangesData);
				table.setStartInclude(startInclude);
				table.setEndInclude(endInclude);
				table.setLookupKey(partitionKey);
				int rejectedPort = getOutPorts().size();
				partitionFce = new RangePartition(table,
						lookupMetadata.getFieldPosition(PORT_NO_FIELD_NAME), 
						partitionRanges.length < rejectedPort ? rejectedPort -1 : RangePartition.NONEXISTENT_REJECTED_PORT);
			}else if (partitionKeyNames != null){
				partitionFce = new HashPartition();
			}else{
				partitionFce = new RoundRobinPartition();
			}
		}
		partitionFce.init(outPorts.size(),partitionKey);
	}

	/**
	 * Creates parttition function from givev class name
	 * 
	 * @param partitionClass
	 * @return
	 * @throws ComponentNotReadyException
	 */
	private PartitionFunction createPartitionFce(String partitionClass) throws ComponentNotReadyException{
		PartitionFunction function;
        try {
         	function =  (PartitionFunction)Class.forName(partitionClass).newInstance();
        }catch (InstantiationException ex){
            throw new ComponentNotReadyException("Can't instantiate partition function class: "+ex.getMessage());
        }catch (IllegalAccessException ex){
            throw new ComponentNotReadyException("Can't instantiate partition function class: "+ex.getMessage());
        }catch (ClassNotFoundException ex) {
            throw new ComponentNotReadyException("Can't find specified partition function class: " + partitionClass);
        }
        return function;
	}
	
	/**
	 * Method for creation parttion function from CloverETL language or java source
	 * 
	 * @param partitionCode source code
	 * @return Partition function
	 * @throws ComponentNotReadyException
	 */
	private PartitionFunction createPartitionDynamic(String partitionCode) throws ComponentNotReadyException {
		//check if source code is in CloverETL format
		if (partitionCode.contains(WrapperTL.TL_TRANSFORM_CODE_ID) ||
				PATTERN_TL_CODE.matcher(partitionCode).find()) {
			PartitionTL function =  new PartitionTL(partitionCode, 
					getInputPort(0).getMetadata(), parameters, logger);
			function.setGraph(getGraph());
			return function;
		}else{//get partition function form java code
			DynamicJavaCode dynCode = new DynamicJavaCode(partitionCode, this.getClass().getClassLoader());
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
		
		xmlElement.setAttribute(XML_USE_I18N_ATTRIBUTE, String.valueOf(useI18N));
		if (locale != null) {
			xmlElement.setAttribute(XML_LOCALE_ATTRIBUTE, locale);
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
		try {
			String[] key = xattribs.exists(XML_PARTITIONKEY_ATTRIBUTE) ? 
					StringUtils.split(xattribs.getString(XML_PARTITIONKEY_ATTRIBUTE)) :
					null;		
			String[] ranges = xattribs.exists(XML_RANGES_ATTRIBUTE) ?
					StringUtils.split(xattribs.getString(XML_RANGES_ATTRIBUTE)) :
						null;		
		    partition = new Partition(xattribs.getString(XML_ID_ATTRIBUTE),
		    		xattribs.getString(XML_PARTIONSOURCE_ATTRIBUTE, null, false),
		    		xattribs.getString(XML_PARTITIONCLASS_ATTRIBUTE, null),
		    		xattribs.getString(XML_PARTITIONURL_ATTRIBUTE, null), 
		    		key, ranges);
			partition.setFunctionParameters(xattribs.attributes2Properties(
					new String[]{XML_ID_ATTRIBUTE,XML_PARTIONSOURCE_ATTRIBUTE,
							XML_PARTITIONCLASS_ATTRIBUTE, XML_PARTITIONURL_ATTRIBUTE, 
							XML_PARTITIONKEY_ATTRIBUTE, XML_RANGES_ATTRIBUTE, 
							XML_CHARSET_ATTRIBUTE}));
			if (xattribs.exists(XML_CHARSET_ATTRIBUTE)) {
				partition.setCharset(XML_CHARSET_ATTRIBUTE);
			}
			partition.setUseI18N(xattribs.getBoolean(XML_USE_I18N_ATTRIBUTE, false));
			if (xattribs.exists(XML_LOCALE_ATTRIBUTE)) {
				partition.setLocale(xattribs.getString(XML_LOCALE_ATTRIBUTE));
			}
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
   		 
    		if(!checkInputPorts(status, 1, 1)
    				|| !checkOutputPorts(status, 1, Integer.MAX_VALUE)) {
    			return status;
    		}
    		
            checkMetadata(status, getInMetadata(), getOutMetadata());

            try {
            	
        	    if (partitionKeyNames != null) {
        			partitionKey = new RecordKey(partitionKeyNames,
        					getInputPort(0).getMetadata());
        		}
        		if (partitionKey != null) {
        			try {
        				partitionKey.init();
        			} catch (Exception e) {
        				throw new ComponentNotReadyException(this, 
        						XML_PARTITIONKEY_ATTRIBUTE, e.getMessage());
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

	/*
	 * (non-Javadoc)
	 * @see org.jetel.graph.Node#reset()
	 */
	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
		// no implementation neeeded
	}


	
}

