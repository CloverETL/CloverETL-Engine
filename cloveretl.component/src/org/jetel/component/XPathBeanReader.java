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

import java.net.URL;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.xpathparser.AbortParsingException;
import org.jetel.component.xpathparser.DataRecordProvider;
import org.jetel.component.xpathparser.DataRecordReceiver;
import org.jetel.component.xpathparser.XPathSequenceProvider;
import org.jetel.component.xpathparser.XPathPushParser;
import org.jetel.component.xpathparser.bean.JXPathEvaluator;
import org.jetel.component.xpathparser.mappping.MalformedMappingException;
import org.jetel.component.xpathparser.mappping.MappingContext;
import org.jetel.component.xpathparser.mappping.MappingElementFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.sequence.Sequence;
import org.jetel.data.sequence.SequenceFactory;
import org.jetel.data.tree.bean.parser.BeanValueHandler;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.dictionary.DictionaryEntry;
import org.jetel.sequence.PrimitiveSequence;
import org.jetel.util.AutoFilling;
import org.jetel.util.XmlUtils;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * A component to process Java beans using XPath.
 * 
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 6.12.2011
 */
public class XPathBeanReader
	extends Node
	implements DataRecordProvider, DataRecordReceiver, XPathSequenceProvider {

	static Log logger = LogFactory.getLog(XPathBeanReader.class);

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "BEAN_READER";

	/** XML attribute names */
	public final static String XML_FILE_ATTRIBUTE = "fileURL";
	public final static String XML_MAPPING_URL_ATTRIBUTE = "mappingURL";
	public final static String XML_MAPPING_ATTRIBUTE = "mapping";
	public final static String XML_DATA_POLICY_ATTRIBUTE = "dataPolicy";
	
	private static int sequenceId;
	
	private String fileURL;
	private String mappingURL;
	private Document mapping;

	private XPathPushParser parser;
	private MappingContext mapppingRoot;
    private PolicyType policyType;
    
    private DataRecord outputRecords[];
    private OutputPort outputPorts[];
    private List<Object> inputs;
    
    private Map<MappingContext, Sequence> sequences;
    
    private AutoFilling autoFilling = new AutoFilling();

	/**
	 * Constructor
	 * @param  id        Description of the Parameter
	 * @param  fileURL   Description of the Parameter
	 * @param mapping    Description of the Parameter
	 */
	public XPathBeanReader(String id, String fileURL, Document mapping) {
		super(id);
		this.fileURL = fileURL;
		this.mapping = mapping;
		parser = new XPathPushParser(this, this, new JXPathEvaluator(), new BeanValueHandler(), this);
	}

	/**
	 * Constructor
	 * @param  id        Description of the Parameter
	 * @param  fileURL   Description of the Parameter
	 * @param mappingURL Description of the Parameter
	 */
	public XPathBeanReader(String id, String fileURL, String mappingURL) {
		super(id);
		this.fileURL = fileURL;
		this.mappingURL = mappingURL;
		parser = new XPathPushParser(this, this, new JXPathEvaluator(), new BeanValueHandler(), this);
	}
	
    @Override
    public void preExecute() throws ComponentNotReadyException {
    	super.preExecute();

		/*
		 * process file URL  - so far, dictionary entry is expected
		 */
		inputs = new ArrayList<Object>();
		for (String entry : fileURL.split(";")) {
			if (entry.startsWith("dict:")) {
				String name = entry.substring(entry.indexOf(':') + 1);
				DictionaryEntry e =  getGraph().getDictionary().getEntry(name);
				/*
				 * TODO check type?
				 */
				inputs.add(e.getValue());
			} else {
				throw new ComponentNotReadyException("unsupported source URL: " + entry);
			}
		}
		
		sequences = new HashMap<MappingContext, Sequence>();
		autoFilling.reset();
    }

	@Override
	public Result execute() throws Exception {
		/*
		 * process output ports and create data records for parser
		 * TODO what other should be set on records?
		 */
		int portCount = getOutPorts().size();
		outputRecords = new DataRecord[portCount];
		outputPorts = new OutputPort[portCount];
		for (int i = 0; i < portCount; ++i) {
			OutputPort port = getOutputPort(i);
			DataRecord record = new DataRecord(port.getMetadata());
			record.init();
			outputRecords[i] = record;
			outputPorts[i] = port;
			autoFilling.addAutoFillingFields(port.getMetadata());
		}
		try {
			for (Iterator<Object> i = inputs.iterator(); i.hasNext() && runIt;) {
				parser.parse(mapppingRoot, i.next());
				autoFilling.resetSourceCounter();
				autoFilling.resetGlobalSourceCounter();
			}
		} catch (AbortParsingException e) {
			if (!runIt) {
				/*
				 * we have aborted intentionally
				 */
			}
		} catch (Exception e) {
			throw e;
		} finally {
			broadcastEOF();
		}
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	@Override
	public Sequence getSequence(MappingContext context) {
		
		Sequence sequence = sequences.get(context);
		if (sequence == null) {
			if (context.getSequenceId() != null) {
				sequence = getGraph().getSequence(context.getSequenceId());
			}
			if (sequence == null) {
				String id = "XPathBeanReaderSeq_" + sequenceId++;
				sequence = SequenceFactory.createSequence(getGraph(), PrimitiveSequence.SEQUENCE_TYPE,
						new Object[] {id, getGraph(), context.getSequenceField()},
						new Class[] {String.class, TransformationGraph.class, String.class}
				);
			}
			sequences.put(context, sequence);
		}
		return sequence;
	}

	@Override
	public void receive(DataRecord record, int port) throws AbortParsingException {
		
		if (runIt) {
			try {
				autoFilling.setAutoFillingFields(record);
				outputPorts[port].writeRecord(record);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		} else {
			throw new AbortParsingException();
		}
	}

	@Override
	public void exceptionOccurred(BadDataFormatException e) throws AbortParsingException {
		
		if (policyType == PolicyType.STRICT) {
			logger.error("Could not assign data field.", e);
			throw new AbortParsingException();
		}
	}

	@Override
	public DataRecord getDataRecord(int port) throws AbortParsingException {
		
		if (runIt) {
			return outputRecords[port];
		} else {
			throw new AbortParsingException();
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
		
		if (mapping == null) {
			if (mappingURL != null) {
    			TransformationGraph graph = getGraph();
    			URL contextURL = graph != null ? graph.getRuntimeContext().getContextURL() : null;
    			try {
    				ReadableByteChannel ch = FileUtils.getReadableChannel(contextURL, mappingURL);
   					mapping = XmlUtils.createDocumentFromChannel(ch);
    			} catch (Exception e) {
    				throw new ComponentNotReadyException("Mapping parameter parse error occured.", e);
    			}
    		} else {
    			throw new ComponentNotReadyException("mapping not found");
    		}
		}
		
		/*
		 * TODO validate mapping model in checkConfig with respect to port metadata
		 * i.e. that there are all fields available and all mentioned ports are connected
		 */
		try {
			mapppingRoot = new MappingElementFactory().readMapping(mapping);
		} catch (MalformedMappingException e) {
			throw new ComponentNotReadyException("Input mapping is not valid.", e);
		}
		
	}

	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Returned Value
	 * @since     May 21, 2002
	 */
	@Override
	public void toXML(Element xmlElement) {
	    super.toXML(xmlElement);
		xmlElement.setAttribute(XML_FILE_ATTRIBUTE, fileURL);
		xmlElement.setAttribute(XML_DATA_POLICY_ATTRIBUTE, policyType.toString());
	}


	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @since           May 21, 2002
	 */
    public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		XPathBeanReader reader = null;
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		try {
			String mappingURL = xattribs.getStringEx(XML_MAPPING_URL_ATTRIBUTE, null,RefResFlag.SPEC_CHARACTERS_OFF);
			if (mappingURL != null) {
				reader = new XPathBeanReader(
						xattribs.getString(XML_ID_ATTRIBUTE),
						xattribs.getStringEx(XML_FILE_ATTRIBUTE,RefResFlag.SPEC_CHARACTERS_OFF),
						mappingURL);
			} else {
				Document mappingDocument = null;
				try {
					mappingDocument = XmlUtils.createDocumentFromString(xattribs.getString(XML_MAPPING_ATTRIBUTE));
				} catch (JetelException e) {
					throw new XMLConfigurationException("Mapping parameter parse error occurs.", e);
				}
				
				reader = new XPathBeanReader(
						xattribs.getString(XML_ID_ATTRIBUTE),
						xattribs.getStringEx(XML_FILE_ATTRIBUTE,RefResFlag.SPEC_CHARACTERS_OFF),
						mappingDocument);
			}
			
			reader.setPolicyType(xattribs.getString(XML_DATA_POLICY_ATTRIBUTE, null));
		} catch (Exception ex) {
	           throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}

		return reader;
	}

    public void setPolicyType(String strPolicyType) {
        policyType = PolicyType.valueOfIgnoreCase(strPolicyType);
    }
    
	/**
	 * Return data checking policy
	 * @return User defined data policy, or null if none was specified
	 * @see org.jetel.exception.BadDataFormatExceptionHandler
	 */
	public PolicyType getPolicyType() {
		return policyType;
	}
	
	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Return Value
	 */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        
        if (!checkOutputPorts(status, 1, Integer.MAX_VALUE)) {
        	return status;
        }
        
        return status;
    }
	
	@Override
	public String getType(){
		return COMPONENT_TYPE;
	}
}
