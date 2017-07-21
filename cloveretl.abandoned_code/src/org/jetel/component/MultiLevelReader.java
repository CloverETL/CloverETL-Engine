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
/*
 *  Created on Mar 19, 2003
 *
 *  To change this generated comment go to
 *  Window>Preferences>Java>Code Generation>Code and Comments
 */
package org.jetel.component;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.denormalize.RecordDenormalize;
import org.jetel.data.DataRecord;
import org.jetel.data.parser.MultiLevelParser;
import org.jetel.data.parser.TypeSelector;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.ParserExceptionHandlerFactory;
import org.jetel.exception.PolicyType;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.MultiFileReader;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.compile.DynamicJavaCode;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 *  <h3>Multi Level Reader Component</h3>
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>MultiLevelReader</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Parses multi-level input files. These files may contain data records of different types (metadata).
 * Each type is associated with one output port. Data type record is decided by special object (type selector)
 * specified by the user. Type selector must implement interface TypeSelector.
 * All the data types must have fixed length.
 * </td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>One port for each data type.</td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"MULTI_LEVEL_READER"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>fileURL</b></td><td>path to the input file</td>
 *  <tr><td><b>charset</b></td><td>character encoding of the input file (if not specified, then ISO-8859-1 is used)</td>
 *  <tr><td><b>dataPolicy</b></td><td>specifies how to handle misformatted or incorrect data.  'Strict' (default value) aborts processing, 'Controlled' logs the entire record while processing continues, and 'Lenient' attempts to set incorrect data to default values while processing continues.</td>
 *  <tr><td><b>skipRows</b><br><i>optional</i></td><td>specifies how many records/rows should be skipped from the source file. Good for handling files where first rows is a header not a real data. Dafault is 0.</td>
 *  <tr><td><b>numRecords</b></td>optional<td>max number of parsed records (defaults to 0 - unlimited)</td>
 *  <tr><td><b>selectorCode</b></td><td>Inline Java code defining type selector class</td>
 *  <tr><td><b>selectorClass</b></td><td>Name of selector class (defaults to CyclicTypeSelector)</td>
 *  </tr>
 *  </table>
 * <h4>Example:</h4>
 * <pre>&lt;Node enabled="enabled" id="Input" type="MULTI_LEVEL_READER" fileURL="${WORKSPACE}/data/fixlen.dat"
 * selectorClass="org.jetel.data.parser.CiclicTypeSelector"/&gt;</pre>
 *
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 15/12/06  
 * @see         TypeSelector
 */

public class MultiLevelReader extends Node {

	private static final String XML_FILEURL_ATTRIBUTE = "fileURL";
	private static final String XML_CHARSET_ATTRIBUTE = "charset";
	private final static String XML_DATAPOLICY_ATTRIBUTE = "dataPolicy";
	private static final String XML_SKIP_ROWS_ATTRIBUTE = "skipRows";
    private static final String XML_NUMRECORDS_ATTRIBUTE = "numRecords";
    private static final String XML_SELECTORCLASS_ATTRIBUTE = "selectorClass";
    private static final String XML_SELECTORCODE_ATTRIBUTE = "selectorCode";
	
	static Log logger = LogFactory.getLog(MultiLevelReader.class);
	
	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "MULTI_LEVEL_READER";
	private final static int INPUT_PORT = 0;

	private String fileURL;

	private MultiLevelParser parser;
    private MultiFileReader reader;
    private PolicyType policyType;
    private boolean skipFirstLine = false;
	private int skipRows = 0; // do not skip rows by default
    private int numRecords = -1;
    private String charset;
    private String seltorCode;
    private String seltorClass;
	private TypeSelector seltor;
    private Properties seltorProperties;
    
    private OutputPort output[];
    

	/**
	 *Constructor for the FixLenDataReaderNIO object
	 *
	 * @param  id       Description of the Parameter
	 * @param  fileURL  Description of the Parameter
	 * @param  charset  Description of the Parameter
	 */
	public MultiLevelReader(String id, String fileURL, String charset, String dataPolicy, int skipRows, int numRecords,
			String seltorCode, String seltorClass, Properties seltorProperties) {
		super(id);
		this.fileURL = fileURL;
		this.charset = charset;
		this.policyType = PolicyType.valueOfIgnoreCase(dataPolicy);
		this.skipRows = skipRows;
		this.numRecords = numRecords;
		this.seltorCode = seltorCode;
		this.seltorClass = seltorClass;
		this.seltorProperties = seltorProperties;
	}

	public MultiLevelReader(String id, String fileURL, String charset, String dataPolicy, int skipRows, int numRecords,
			TypeSelector seltor, Properties seltorProperties) {
		super(id);
		this.fileURL = fileURL;
		this.charset = charset;
		this.policyType = PolicyType.valueOfIgnoreCase(dataPolicy);
		this.skipRows = skipRows;
		this.numRecords = numRecords;
		this.seltor = seltor;
		this.seltorProperties = seltorProperties;
	}
	

	@Override
	public Result execute() throws Exception {
		DataRecord record;
		try {
			while (runIt) {
			    try {
					//broadcast the record to all connected Edges
			        if((record = reader.getNext(null)) == null) {	// no more records
			        	break;
			        }
			        output[parser.getTypeIdx()].writeRecord(record);
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
			broadcastEOF();
		}
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	/*
	 * (non-Javadoc)
	 * @see org.jetel.graph.Node#reset()
	 */
	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
		reader.reset();
	}

	/*
	 * (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#free()
	 */
	@Override
	public synchronized void free() {
		super.free();
		if (reader != null) {
			reader.close();
		}
	}

	/**
	 * Creates denormalization instance using specified class.
	 * @param denormClass
	 * @return
	 * @throws ComponentNotReadyException
	 */
	private TypeSelector createSelector(String seltorClass) throws ComponentNotReadyException {
		TypeSelector seltor;
        try {
            seltor =  (TypeSelector)Class.forName(seltorClass).newInstance();
        }catch (InstantiationException ex){
            throw new ComponentNotReadyException("Can't instantiate selector class: "+ex.getMessage());
        }catch (IllegalAccessException ex){
            throw new ComponentNotReadyException("Can't instantiate selector class: "+ex.getMessage());
        }catch (ClassNotFoundException ex) {
            throw new ComponentNotReadyException("Can't find specified selector class: " + seltorClass);
        }
		return seltor;
	}

	/**
	 * Creates normalization instance using given Java source.
	 * @param denormCode
	 * @return
	 * @throws ComponentNotReadyException
	 */
	private TypeSelector createSelectorDynamic(String seltorCode) throws ComponentNotReadyException {
		DynamicJavaCode dynCode = new DynamicJavaCode(seltorCode, this.getClass().getClassLoader());
        logger.info(" (compiling dynamic source) ");
        // use DynamicJavaCode to instantiate transformation class
        Object transObject = null;
        try {
            transObject = dynCode.instantiate();
        } catch (RuntimeException ex) {
            logger.debug(dynCode.getCompilerOutput());
            logger.debug(dynCode.getSourceCode());
            throw new ComponentNotReadyException("Type selector code is not compilable.\n" + "Reason: " + ex.getMessage());
        }
        if (transObject instanceof RecordDenormalize) {
            return (TypeSelector)transObject;
        } else {
            throw new ComponentNotReadyException("Provided type selector class doesn't implement TypeSelector.");
        }
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
		
		if (seltor == null && seltorCode != null) {
			seltor = createSelectorDynamic(seltorCode);
		} else {
			seltor = createSelector(seltorClass);
		}
        parser = new MultiLevelParser(charset, seltor, getOutMetadata().toArray(new DataRecordMetadata[0]), seltorProperties);
        parser.setExceptionHandler(ParserExceptionHandlerFactory.getHandler(policyType));

        // initialize multifile reader based on prepared parser
		TransformationGraph graph = getGraph();
        reader = new MultiFileReader(parser, graph != null ? graph.getProjectURL() : null, fileURL);
        reader.setLogger(logger);
        reader.setFileSkip(skipFirstLine ? 1 : 0);
        reader.setSkip(skipRows);
        reader.setNumRecords(numRecords);
        reader.setInputPort(getInputPort(INPUT_PORT)); //for port protocol: ReadableChannelIterator reads data
        reader.setCharset(charset);
        reader.setDictionary(graph.getDictionary());
        reader.init(null);
        output = (OutputPort[])getOutPorts().toArray(new OutputPort[0]);
	}


	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Returned Value
	 * @since     May 21, 2002
	 */
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
		xmlElement.setAttribute(XML_FILEURL_ATTRIBUTE,this.fileURL);
		
		if (this.parser.getCharsetName() != null) {
			xmlElement.setAttribute(XML_CHARSET_ATTRIBUTE, this.parser.getCharsetName());
		}
		
		if (this.skipRows>0){
		    xmlElement.setAttribute(XML_SKIP_ROWS_ATTRIBUTE, String.valueOf(skipRows));
		}
		xmlElement.setAttribute(XML_DATAPOLICY_ATTRIBUTE, policyType.toString());		
	}


	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @since           May 21, 2002
	 */
    public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		MultiLevelReader reader = null;
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		
		try {
			reader = new MultiLevelReader(xattribs.getString(XML_ID_ATTRIBUTE),
						xattribs.getString(XML_FILEURL_ATTRIBUTE, ""),
						xattribs.getString(XML_CHARSET_ATTRIBUTE, null),
			            xattribs.getString(XML_DATAPOLICY_ATTRIBUTE, null),			
						xattribs.getInteger(XML_SKIP_ROWS_ATTRIBUTE, 0),
						xattribs.getInteger(XML_NUMRECORDS_ATTRIBUTE, 0),
						xattribs.getString(XML_SELECTORCODE_ATTRIBUTE, null),
						xattribs.getString(XML_SELECTORCLASS_ATTRIBUTE, "org.jetel.data.parser.CyclicTypeSelector"),
						xattribs.attributes2Properties(
								new String[] {XML_ID_ATTRIBUTE, XML_FILEURL_ATTRIBUTE,
										XML_CHARSET_ATTRIBUTE, XML_DATAPOLICY_ATTRIBUTE,
										XML_SKIP_ROWS_ATTRIBUTE, XML_NUMRECORDS_ATTRIBUTE,
										XML_SELECTORCODE_ATTRIBUTE,	XML_SELECTORCLASS_ATTRIBUTE,}));
						
		} catch (Exception ex) {
	           throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}

		return reader;
	}


	/**
	 * Adds BadDataFormatExceptionHandler to behave according to DataPolicy.
	 *
	 * @param  handler
	 */
	public void setExceptionHandler(IParserExceptionHandler handler) {
		parser.setExceptionHandler(handler);
	}


	/**  Description of the Method */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        
        if(!checkInputPorts(status, 0, 1)
        		|| !checkOutputPorts(status, 1, Integer.MAX_VALUE)) {
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
	
	public String getType(){
		return COMPONENT_TYPE;
	}
	
}

