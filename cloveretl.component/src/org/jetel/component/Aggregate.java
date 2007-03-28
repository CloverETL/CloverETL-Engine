/**
 * 
 */
package org.jetel.component;

import java.util.Iterator;

import org.jetel.component.aggregate.AggregateProcessor;
import org.jetel.component.aggregate.AggregateProcessorException;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
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
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.StringUtils;
import org.w3c.dom.Element;

/**
 * @author Jaroslav Urban
 *
<<<<<<< .mine
=======
 * <!-- Aggregate functions ara applied on input data flow base on specified key.-->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>Aggregate</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Aggregate functions are applied on input data flow base on specified key.<br>
 *  The key is name (or combination of names) of field(s) from input record.
 *  Data flow can be sorted or not. On this component you cannot set any transformation function
 *  to map aggregation results on the output metadata. Output metadata has to correspond accurately
 *  to the settings of aggregate component. Key of aggregation is mapped first and then follow
 *  all aggregate function results.</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0]- input records</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>At least one connected output port.</td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"AGGREGATE"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>aggregateKey</b></td><td>field names separated by :;|  {colon, semicolon, pipe}</td>
 *  <tr><td><b>aggregateFunction</b></td><td>aggregate functions separated by :;|  {colon, semicolon, pipe} available functions are count, min, max, sum, avg, stdev, CRC32, MD5, FIRST, LAST</td>
 *  <tr><td><b>sorted</b></td><td>if input data flow is sorted (true)</td>
 *  <tr><td><b>equalNULL</b><br><i>optional</i></td><td>specifies whether two fields containing NULL values are considered equal. Default is FALSE.</td></tr>
 *  <tr><td><b>charset</b></td><td>character encoding of the input data stream for CRC32 and MD5 functions (if not specified, then value from defaultProperties DataFormatter.DEFAULT_CHARSET_ENCODER is used)</td>
 *  </tr>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node id="AGGREGATE_NODE" type="AGGREGATE" aggregateKey="FirstName" aggregateFunctions="count(); min(Age); avg(Salery); min(HireDate)" sorted="false" /&gt;</pre>
 *
 * @author      Martin Zatopek, OpenTech, s.r.o (www.opentech.cz)
 * @since       June 27, 2005
 * @revision    $Revision: 2670 $
>>>>>>> .r2681
 */
public class Aggregate extends Node {
	public final static String COMPONENT_TYPE = "AGGREGATE";
	
	// required attributes
	private static final String XML_AGGREGATE_KEY_ATTRIBUTE = "aggregateKey";
	private static final String XML_MAPPING_ATTRIBUTE = "mapping";
	private static final String XML_SORTED_ATTRIBUTE = "sorted";
	// optional attributes
    private static final String XML_EQUAL_NULL_ATTRIBUTE = "equalNULL";
    private static final String XML_CHARSET_ATTRIBUTE = "charset";

	// used ports
	private final static int WRITE_TO_PORT = 0;
	private final static int READ_FROM_PORT = 0;

	private String[] aggregateKeys;
	private String[] mapping;
	private boolean sorted;
	
	private boolean equalNULLs;
	private String charset;

	private AggregateProcessor processor;
	private RecordKey recordKey;

	/**
	 * 
	 * Allocates a new <tt>Aggregate</tt> object.
	 *
	 * @param id unique ID of the component.
	 * @param aggregateKeys aggregation keys.
	 * @param mapping aggregation function mapping.
	 * @param sorted specifies if the input is sorted.
	 */
	public Aggregate(String id, String[] aggregateKeys, String[] mapping, boolean sorted) {
		super(id);
		
		this.aggregateKeys = aggregateKeys;
		this.mapping = mapping;
		this.sorted = sorted;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#execute()
	 */
	@Override
	public Result execute() throws Exception {
		if (sorted) {
			boolean firstLoop = true;
			InputPort inPort = getInputPort(READ_FROM_PORT);
			OutputPort outPort = getOutputPort(WRITE_TO_PORT);
			DataRecord currentRecord = new DataRecord(inPort.getMetadata());
			DataRecord previousRecord = new DataRecord(inPort.getMetadata());
			DataRecord tempRecord;
			DataRecord outRecord = new DataRecord(outPort.getMetadata());

			currentRecord.init();
			previousRecord.init();
			outRecord.init();

			while (currentRecord != null && runIt) {
				currentRecord = inPort.readRecord(currentRecord);
				if (!firstLoop) {
					if (currentRecord == null
							|| recordKey.compare(currentRecord, previousRecord) != 0) { // next group founded
						writeRecordBroadcast(processor.getCurrentSortedAggregationOutput(outRecord));
					}
				} else {
					firstLoop = false;
				}
				// switch previous and current record
				if (currentRecord != null) {
					processor.addRecord(currentRecord, outRecord);

					tempRecord = previousRecord;
					previousRecord = currentRecord;
					currentRecord = tempRecord;
				}
			}
		} else { // sorted == false
			InputPort inPort = getInputPort(READ_FROM_PORT);
			OutputPort outPort = getOutputPort(WRITE_TO_PORT);
			DataRecord currentRecord = new DataRecord(inPort.getMetadata());
			DataRecord outRecord = new DataRecord(outPort.getMetadata());

			currentRecord.init();
			outRecord.init();

			// read all data from input port to aggregateRecord
			while ((currentRecord = inPort.readRecord(currentRecord)) != null && runIt) {
				processor.addRecord(currentRecord, outRecord);
			}
			
			for (Iterator<DataRecord> results = processor.getUnsortedAggregationOutput();
				results.hasNext(); ) {
				writeRecordBroadcast(results.next());
			}
		}
		
		broadcastEOF();
		return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#init()
	 */
	@Override
	public void init() throws ComponentNotReadyException {
		super.init();
		
		recordKey = new RecordKey(aggregateKeys, getInputPort(READ_FROM_PORT).getMetadata());
		recordKey.init();
		// specify whether two fields with NULL value indicator set are considered equall
		recordKey.setEqualNULLs(equalNULLs);
		
		try {
			processor = new AggregateProcessor(mapping, recordKey, sorted, 
					getInputPort(READ_FROM_PORT).getMetadata(), getOutputPort(WRITE_TO_PORT).getMetadata(),
					charset);
		} catch (AggregateProcessorException e) {
			throw new ComponentNotReadyException(e);
		}
	}

	/**
	 * 
	 * @param graph
	 * @param xmlElement
	 * @return
	 * @throws XMLConfigurationException
	 */
	public static Node fromXML(TransformationGraph graph, Element xmlElement)throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		String[] aggregateKey = null;
		String[] mapping = null;
        boolean sorted = true;
		try {
            //read aggregate key attribute
            if(xattribs.exists(XML_AGGREGATE_KEY_ATTRIBUTE)) {
                aggregateKey = xattribs.getString(XML_AGGREGATE_KEY_ATTRIBUTE).split(
                		Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);                
            }
            
            //read mapping attribute
            if (xattribs.exists(XML_MAPPING_ATTRIBUTE)) {
            	mapping = xattribs.getString(XML_MAPPING_ATTRIBUTE).split(
            			Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
            }
            
            //read sorted attribute
            if(xattribs.exists(XML_SORTED_ATTRIBUTE)) {
                sorted = xattribs.getBoolean(XML_SORTED_ATTRIBUTE);                
            }
            //make an instance of the component
		    Aggregate aggregate = new Aggregate(xattribs.getString("id"), 
		    		aggregateKey,
					mapping,
                    sorted);
			
			// read optional attributes
			if (xattribs.exists(XML_EQUAL_NULL_ATTRIBUTE)){
				aggregate.setEqualNULLs(xattribs.getBoolean(XML_EQUAL_NULL_ATTRIBUTE));
			}
            if (xattribs.exists(XML_CHARSET_ATTRIBUTE)){
            	aggregate.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE));
            }
            
			return aggregate;
		} catch (Exception e) {
            throw new XMLConfigurationException(COMPONENT_TYPE + ":" 
            		+ xattribs.getString(XML_ID_ATTRIBUTE,"unknown ID") + ":" + e.getMessage(),e);
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#toXML(org.w3c.dom.Element)
	 */
	@Override
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);

        if (aggregateKeys.length > 0){
        	xmlElement.setAttribute(XML_AGGREGATE_KEY_ATTRIBUTE,
        			StringUtils.stringArraytoString(aggregateKeys, Defaults.Component.KEY_FIELDS_DELIMITER.charAt(0)));
        }
        if (mapping.length > 0){
        	xmlElement.setAttribute(XML_MAPPING_ATTRIBUTE,
        			StringUtils.stringArraytoString(mapping, Defaults.Component.KEY_FIELDS_DELIMITER.charAt(0)));
        }
        xmlElement.setAttribute(XML_SORTED_ATTRIBUTE, String.valueOf(sorted));
        
        xmlElement.setAttribute(XML_EQUAL_NULL_ATTRIBUTE, String.valueOf(equalNULLs));
        if (charset != null) {
        	xmlElement.setAttribute(XML_CHARSET_ATTRIBUTE, charset);
        }
	}

	
	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#checkConfig(org.jetel.exception.ConfigurationStatus)
	 */
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		
        checkInputPorts(status, 1, 1);
        checkOutputPorts(status, 1, Integer.MAX_VALUE);

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
	 * @see org.jetel.graph.Node#getType()
	 */
	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}

	/**
	 * @return the equalNULLs
	 */
	public boolean isEqualNULLs() {
		return equalNULLs;
	}

	/**
	 * @param equalNULLs the equalNULLs to set
	 */
	public void setEqualNULLs(boolean equalNULLs) {
		this.equalNULLs = equalNULLs;
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
}
