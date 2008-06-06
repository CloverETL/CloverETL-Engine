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

import java.io.StringReader;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.parser.XPathParser;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ParserExceptionHandlerFactory;
import org.jetel.exception.PolicyType;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.MultiFileReader;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

/**
 *  <h3>XML XPath Reader Component</h3>
 *
 * <!-- Parses xml input data file base on xpaths and broadcasts the records to specific connected out ports -->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>XmlXPathReader</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Parses xml input data file base on xpaths and broadcasts the records to specific connected out ports.</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>At least one output port defined/connected.</td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"XML_XPATH_READER"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>fileURL</b></td><td>path to the input files</td>
 *  <tr><td><b>dataPolicy</b></td><td>specifies how to handle misformatted or incorrect data.  'Strict' (default value) aborts processing, 'Controlled' logs the entire record while processing continues, and 'Lenient' attempts to set incorrect data to default values while processing continues.</td>
 *  <tr><td><b>skipRows</b><br><i>optional</i></td><td>specifies how many records/rows should be skipped from the source file. Good for handling files where first rows is a header not a real data. Dafault is 0.</td>
 *  <tr><td><b>numRecords</b></td><td>max number of parsed records</td>
 *  When not set, strings are trimmed depending on "trim" attribute of metadata.</td>
 *  </tr>
 *  </table>
 *
 *<br>
 * Mapping attribute contains mapping hierarchy in XML form. DTD of mapping:<br>
 * <code>
 * &lt;!ELEMENT Context (Context* | Mapping*)&gt;<br>
 * &lt;!ELEMENT Mapping&gt;<br>
 * 
 * &lt;!ELEMENT Context (Context* | Mapping*)&gt;<br>
 * &lt;!ATTLIST Context<br>
 * &nbsp;xpath NMTOKEN #REQUIRED<br>      
 * &nbsp;&nbsp;//xpath query to the xml node<br>  
 * &nbsp;outPort NMTOKEN #REQUIRED<br>
 * &nbsp;&nbsp;//name of output port for this mapped XML element<br>
 * &nbsp;parentKey NMTOKEN #IMPLIED<br>     
 * &nbsp;&nbsp;//field name of parent record, which is copied into field of the current record<br>
 * &nbsp;&nbsp;//passed in generatedKey atrribute<br> 
 * &nbsp;generatedKey NMTOKEN #IMPLIED<br>  
 * &nbsp;&nbsp;//see parentKey comment<br>
 * &nbsp;sequenceField NMTOKEN #IMPLIED<br> 
 * &nbsp;&nbsp;//field name, which will be filled by value from sequence<br>
 * &nbsp;&nbsp;//(can be used to generate new key field for relative records)<br> 
 * &nbsp;sequenceId NMTOKEN #IMPLIED<br>    
 * &nbsp;&nbsp;//we can supply sequence id used to fill a field defined in a sequenceField attribute<br>
 * &nbsp;&nbsp;//(if this attribute is omited, non-persistent PrimitiveSequence will be used)<br>

 * &lt;!ELEMENT Mapping&gt;<br>
 * &lt;!ATTLIST Mapping<br>
 * &nbsp;cloverFields NMTOKEN #REQUIRED<br>  
 * &nbsp;&nbsp;//name of metadata filed<br>
 * &nbsp;xpath NMTOKEN #REQUIRED<br>      
 * &nbsp;&nbsp;//xpath query to the xml value<br>  
 * &nbsp;nodeName NMTOKEN #IMPLIED<br>  
 * &nbsp;&nbsp;//direct xml node from where is taken a text, it is guicker to xpath<br>
 * &nbsp;trim NMTOKEN #IMPLIED<br>  
 * &nbsp;&nbsp;//trims leading and trailing space<br>
 * &gt;<br>
 * </code>
 * 
 * Each context element mentioned in context hierarchy in mapping attribute of this component iterates
 * over all matched xml nodes (results of XPath query). A nested context element query is evaluated on
 * each result of the parent context. A translation xml nodes to clover data records is provided 
 * by mapping elements of appropriate context. All mapping xpaths or nodeName, that are defined in mapping
 * elements, bind results to clover fields. XML elements and clover fields with same names are mapped 
 * by this component automatically on each other. XPath attribute can mapped arbitrary node value 
 * by contrast to nodeName that can mapped only element from the query result. Mapping definition via 
 * nodeName is quicker, so it is better to use nodeName than xpath if it is possible.
 * 
 * Record from nested Context element could be connected via key fields with parent record produced by
 * parent Mapping element (see parentKey and generatedKey attribute notes). In case that retrieved values
 * are not suitable to compose unique key, extractor could fill one or more fields with values comming
 * from sequence (see sequenceField and sequenceId attribute). * 
 * 
 *  <h4>Example:</h4>
 *  <pre>&lt;Node type="XML_XPATH_READER" id="XML_XPATH_READER0" fileURL="mydata.xml"&gt; <br>
 * &nbsp;&lt;attr name="mapping"&gt;<br>
 * &nbsp;&nbsp;&lt;Context xpath="/forum//message" outPort="0" sequenceField="seq_key"&gt;<br>
 * &nbsp;&nbsp;&nbsp;&lt;Mapping xpath="author/text()" cloverField="a1"/&gt;<br>
 * &nbsp;&nbsp;&nbsp;&lt;Mapping xpath="title/text()" cloverField="a1"/&gt;<br>
 * &nbsp;&nbsp;&nbsp;&lt;Mapping xpath="message/text()" trim="false" cloverField="a1"/&gt;<br>
 * &nbsp;&nbsp;&lt;/Mapping&gt; <br>
 * &nbsp;&lt;/attr&gt;<br>
 * &lt;/node&gt;<br>
 *  </pre>
 *
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 * @created 20.5.2007
 */
public class XmlXPathReader extends Node {

    static Log logger = LogFactory.getLog(XmlXPathReader.class);

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "XML_XPATH_READER";

	/** XML attribute names */
	private final static String XML_FILE_ATTRIBUTE = "fileURL";
	private final static String XML_MAPPING_ATTRIBUTE = "mapping";
	private final static String XML_DATAPOLICY_ATTRIBUTE = "dataPolicy";
    private static final String XML_SKIP_ROWS_ATTRIBUTE = "skipRows";
    private static final String XML_NUMRECORDS_ATTRIBUTE = "numRecords";
    private static final String XML_CHARSET_ATTRIBUTE = "charset";
	
	private final static int OUTPUT_PORT = 0;
	private final static int INPUT_PORT = 0;
	private String fileURL;

	private XPathParser parser;
    private MultiFileReader reader;
    private PolicyType policyType;
    private int skipRows=0; // do not skip rows by default
    private int numRecords = -1;
    private Object[] ports;

	private String charset;

	/**
	 *Constructor for the DelimitedDataReaderNIO object
	 *
	 * @param  id       Description of the Parameter
	 * @param  fileURL  Description of the Parameter
	 */
	public XmlXPathReader(String id, String fileURL, Document mapping) {
		super(id);
		this.fileURL = fileURL;
		parser = new XPathParser(mapping);
	}

	@Override
	public Result execute() throws Exception {
		// we need to create data record - take the metadata from first output port
		DataRecord[] records = new DataRecord[getOutPorts().size()];
		OutputPort outputPort;
		int outputPortNumber;
		for (int i=0; i<ports.length; i++) {
			outputPortNumber = (Integer)ports[i];
			outputPort = getOutputPort(outputPortNumber);
			if (outputPort == null) 
				throw new ComponentNotReadyException("Error: output port '"+ outputPortNumber +"' doesn't exist");
			records[outputPortNumber] = new DataRecord(outputPort.getMetadata());
			records[outputPortNumber].init();
			parser.assignRecord(records[outputPortNumber], outputPortNumber);
		}
		try {
			Object record;
			while (runIt) {
			    try {
					record = reader.getNext();
					if (record == null) break;
					outputPortNumber = parser.getActualPort();
			        writeRecord(outputPortNumber, records[outputPortNumber].duplicate());
			    } catch(BadDataFormatException bdfe) {
			        if(policyType == PolicyType.STRICT) {
			            throw bdfe;
			        } else {
			            logger.info(bdfe.getMessage());
			        }
			    }
			    SynchronizeUtils.cloverYield();
			}
		} catch (Exception e) {
			throw e;
		}finally{
			reader.close();
			broadcastEOF();
		}
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
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
		TransformationGraph graph = getGraph();
		
        // initialize multifile reader based on prepared parser
        reader = new MultiFileReader(parser, graph != null ? graph.getProjectURL() : null, fileURL);
        reader.setLogger(logger);
        reader.setSkip(skipRows);
        reader.setNumRecords(numRecords);
        parser.setGraph(getGraph());
        reader.setInputPort(getInputPort(INPUT_PORT)); //for port protocol: ReadableChannelIterator reads data
        reader.setCharset(charset);
        reader.setDictionary(graph.getDictionary());
        reader.init(getOutputPort(OUTPUT_PORT).getMetadata());
        ports = parser.getPorts().toArray();
	}

	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
		parser.reset();
		reader.reset();
	}

	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Returned Value
	 * @since     May 21, 2002
	 */
	public void toXML(Element xmlElement) {
	    super.toXML(xmlElement);
		xmlElement.setAttribute(XML_FILE_ATTRIBUTE, this.fileURL);
		xmlElement.setAttribute(XML_DATAPOLICY_ATTRIBUTE, policyType.toString());
		xmlElement.setAttribute(XML_SKIP_ROWS_ATTRIBUTE, String.valueOf(skipRows));
		xmlElement.setAttribute(XML_NUMRECORDS_ATTRIBUTE, String.valueOf(numRecords));
		xmlElement.setAttribute(XML_CHARSET_ATTRIBUTE, charset);
	}


	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @since           May 21, 2002
	 */
    public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		XmlXPathReader aXmlXPathReader = null;
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		xmlElement.getAttributeNode(XML_MAPPING_ATTRIBUTE);
		try {
			aXmlXPathReader = new XmlXPathReader(
					xattribs.getString(XML_ID_ATTRIBUTE),
					xattribs.getString(XML_FILE_ATTRIBUTE),
					createDocumentFromString(xattribs.getString(XML_MAPPING_ATTRIBUTE)));
			aXmlXPathReader.setPolicyType(xattribs.getString(XML_DATAPOLICY_ATTRIBUTE, null));
            if (xattribs.exists(XML_SKIP_ROWS_ATTRIBUTE)){
                aXmlXPathReader.setSkipRows(xattribs.getInteger(XML_SKIP_ROWS_ATTRIBUTE));
            }
            if (xattribs.exists(XML_NUMRECORDS_ATTRIBUTE)){
                aXmlXPathReader.setNumRecords(xattribs.getInteger(XML_NUMRECORDS_ATTRIBUTE));
            }
            if (xattribs.exists(XML_CHARSET_ATTRIBUTE)){
                aXmlXPathReader.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE));
            }
		} catch (Exception ex) {
	           throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}

		return aXmlXPathReader;
	}

    /**
     * Creates org.w3c.dom.Document object from the given String.
     * 
     * @param inString
     * @return
     * @throws XMLConfigurationException
     */
    public static Document createDocumentFromString(String inString) throws XMLConfigurationException {
        InputSource is = new InputSource(new StringReader(inString));
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        Document doc;
        try {
            doc = dbf.newDocumentBuilder().parse(is);
        } catch (Exception e) {
            throw new XMLConfigurationException("Mapping parameter parse error occur.", e);
        }
        return doc;
    }

    public void setPolicyType(String strPolicyType) {
        setPolicyType(PolicyType.valueOfIgnoreCase(strPolicyType));
    }
    
	/**
	 * Adds BadDataFormatExceptionHandler to behave according to DataPolicy.
	 *
	 * @param  handler
	 */
	public void setPolicyType(PolicyType policyType) {
        this.policyType = policyType;
        parser.setExceptionHandler(ParserExceptionHandlerFactory.getHandler(policyType));
	}


	/**
	 * Return data checking policy
	 * @return User defined data policy, or null if none was specified
	 * @see org.jetel.exception.BadDataFormatExceptionHandler
	 */
	public PolicyType getPolicyType() {
		return this.parser.getPolicyType();
	}
	
	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Return Value
	 */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        
        if(!checkInputPorts(status, 0, 1)
        		|| !checkOutputPorts(status, 1, Integer.MAX_VALUE)) {
        	return status;
        }

//        try {
//            init();
//        } catch (ComponentNotReadyException e) {
//            ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
//            if(!StringUtils.isEmpty(e.getAttributeName())) {
//                problem.setAttributeName(e.getAttributeName());
//            }
//            status.add(problem);
//        } finally {
//        	free();
//        }
        
        return status;
    }
	
	public String getType(){
		return COMPONENT_TYPE;
	}
    
     /**
     * @return Returns the skipRows.
     */
    public int getSkipRows() {
        return skipRows;
    }
    /**
     * @param skipRows The skipRows to set.
     */
    public void setSkipRows(int skipRows) {
        this.skipRows = skipRows;
    }
    
    public void setNumRecords(int numRecords) {
        this.numRecords = Math.max(numRecords, 0);
    }
    
    private void setCharset(String charset) {
    	this.charset = charset;
	}

}

