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
package org.jetel.data.parser;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.ErrorListener;
import javax.xml.transform.TransformerException;
import javax.xml.transform.sax.SAXSource;

import net.sf.saxon.om.NamespaceResolver;
import net.sf.saxon.sxpath.IndependentContext;
import net.sf.saxon.sxpath.XPathEvaluator;
import net.sf.saxon.trans.XPathException;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.sequence.Sequence;
import org.jetel.data.sequence.SequenceFactory;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
import org.jetel.exception.StrictParserExceptionHandler;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;

/**
 * This class is parser for Xml XPath reader. 
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 * @created 20.5.2007
 */
public class XPathParser extends AbstractParser {

	//                                                        str1 =     "|'   str2    "|'
	private final static Pattern NAMESPACE = Pattern.compile("(.+)[=]([\"]|['])(.+)([\"]|['])$");
	//                                                                     "|'   str2    "|'
	private final static Pattern NAMESPACE_DEFAULT = Pattern.compile("^([\"]|['])(.+)([\"]|['])$");

	private static final String ELEMENT_CONTEXT = "Context";
	private static final String ELEMENT_XPATH = "Mapping";
	
	private static final String ATTRIBUTE_XPATH = "xpath";
	private static final String ATTRIBUTE_CLOVERFIELD = "cloverField";
	private static final String ATTRIBUTE_TRIM = "trim";
	private static final String ATTRIBUTE_OUTPORT = "outPort";
	private static final String ATTRIBUTE_PARENT_KEY= "parentKey";
	private static final String ATTRIBUTE_GENERATED_KEY= "generatedKey";
    private static final String ATTRIBUTE_NODE_NAME = "nodeName";
    private static final String XML_SEQUENCEFIELD = "sequenceField";
    private static final String XML_SEQUENCEID = "sequenceId";
    private static final String ATTRIBUTE_NAMESPACE_PATHS = "namespacePaths";

    private static final String PRIMITIVE_SEQUENCE = "PRIMITIVE_SEQUENCE";
    private static final String FEATURES_DELIMETER = ";";
    private static final String FEATURES_ASSIGN = ":=";

    public static enum SupportedDataModels {CLOVER_ETL, W3C_XSD};
    
	private DataRecord recordResult;
    private TransformationGraph graph;
    private XPathContext xpathContext;
	private Document xpathDocument;
	
	private IParserExceptionHandler exceptionHandler;

	private List<Integer> ports;

	private boolean isReseted;
	private int skipRows;
	private int numRecords = -1;
	private XMLReader reader;
	private String xmlFeatures;

	private List<XPathEvaluator> xPathEvaluators = new ArrayList<XPathEvaluator>();

	private SupportedDataModels dataModel = SupportedDataModels.CLOVER_ETL;
	
	public XPathParser() {
	}
	
	public XPathParser(Document document) {
		this.xpathDocument = document;
	}
	
	public XPathContext parseXPath(Document xmlDocument) throws SAXException, IOException, ParserConfigurationException, TransformerException, XPathException, DOMException, ComponentNotReadyException {
		// create and process document 
		return parseDocument(xmlDocument);
	}
	
	public XPathContext parseXPath(InputStream inputStream) throws SAXException, IOException, ParserConfigurationException, TransformerException, XPathException, DOMException, ComponentNotReadyException {
		// create and process document 
		Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(inputStream);
		return parseDocument(doc);
	}

	private XPathContext parseDocument(Document doc) throws TransformerException, XPathException, DOMException, ComponentNotReadyException {
		// get just one context element
		ports = new LinkedList<Integer>();
	    NodeList list = doc.getChildNodes();
		Node node = null;
		boolean found = false;
		for (int i=0; i<list.getLength(); i++) {
			if (list.item(i).getNodeName().equalsIgnoreCase(ELEMENT_CONTEXT)) {
				if (found) {
					found = false;
					break;
				} else {
					node = list.item(i);
					found = true;
				}
			}
		}
	    if (!found) 
	    	throw new TransformerException("Every xpath must contain just one " + ELEMENT_CONTEXT + " element!");
	    
		return parseXpathContext(node, new HashMap<String, String>(), null);
	}
	
	private XPathContext parseXpathContext(Node context, Map<String, String> mNamespaces, String sDefaultNamespace) throws DOMException, TransformerException, ComponentNotReadyException {
	    // create xpathContext class
	    Node aNodeSet = context.getAttributes().getNamedItem(ATTRIBUTE_XPATH);
	    Node aOutPort = context.getAttributes().getNamedItem(ATTRIBUTE_OUTPORT);
	    Node aOutParentKey = context.getAttributes().getNamedItem(ATTRIBUTE_PARENT_KEY);
	    Node aOutGenKey = context.getAttributes().getNamedItem(ATTRIBUTE_GENERATED_KEY);
	    Node aSeqField = context.getAttributes().getNamedItem(XML_SEQUENCEFIELD);
	    Node aSeqId = context.getAttributes().getNamedItem(XML_SEQUENCEID);
	    Node aNamespacePaths = context.getAttributes().getNamedItem(ATTRIBUTE_NAMESPACE_PATHS);

	    mNamespaces.putAll(getNamespaces(aNamespacePaths));
	    String newDefaultNamespace = getDefaultNamespace(aNamespacePaths);
	    if (newDefaultNamespace != null) {
	    	sDefaultNamespace = newDefaultNamespace;
	    }
	    XPathEvaluator evaluator = getXPathEvaluator(mNamespaces, sDefaultNamespace);
	    
       	if (aNodeSet == null) throw new ComponentNotReadyException("Attribute '" + ATTRIBUTE_XPATH + "' not found for the context element.");
       	String sXpath = aNodeSet.getNodeValue();
		if (sXpath == null)	throw new ComponentNotReadyException("The 'xpath' attribute is null.");
		
	    XPathContext xpathContext = new XPathContext(evaluator.createExpression(sXpath), sXpath);
	    if (aOutPort != null) {
		    int port = Integer.parseInt(aOutPort.getNodeValue());
		    ports.add(port);
    		xpathContext.setPort(port);
	    }
	    xpathContext.setSkip(skipRows);
	    xpathContext.setNumRecords(numRecords);
	    skipRows = 0;
	    numRecords = -1;

	    //sequence field
        if (aSeqId != null) {
        	Sequence sequence = graph.getSequence(aSeqId.getNodeValue());
        	if (sequence == null) {
        		throw new TransformerException("Wrong value " + XML_SEQUENCEID + "='"+ aSeqId.getNodeValue() + "'.");
        	}
    	    //sequence fields initialization
        	sequence.init();
        	xpathContext.setSequence(sequence); // String id, TransformationGraph graph, String name
        } else if (aSeqField != null) {
        	xpathContext.setSequence(
        			SequenceFactory.createSequence(
                			graph, 
                			PRIMITIVE_SEQUENCE, 
                			new Object[] {aSeqField.getNodeValue(), graph, aSeqField.getNodeValue()},
                			new Class[] {String.class, TransformationGraph.class, String.class}));
        }
       	xpathContext.setSequenceField(aSeqField != null ? aSeqField.getNodeValue(): null);
	    
	    String[] parentKeys = null;
	    String sParentKeys = "";
	    if (aOutParentKey != null) {
	    	sParentKeys = aOutParentKey.getNodeValue();
	    	if (!sParentKeys.equals("")) {
	    		parentKeys = sParentKeys.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
	    		xpathContext.setParentKeys(parentKeys);
	    	}
	    }
	    String[] generatedKeys = null;
	    String sGenKey = "";
	    if (aOutGenKey != null) {
	    	sGenKey = aOutGenKey.getNodeValue();
	    	if (!sGenKey.equals("")) {
	    		generatedKeys = sGenKey.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
	    		xpathContext.setGeneratedKeys(generatedKeys);
	    	}
	    }
	    if (generatedKeys==null ^ parentKeys==null)
	    	throw new TransformerException("Wrong value " + ATTRIBUTE_PARENT_KEY + "='"+ sParentKeys +
	    			"' or " + ATTRIBUTE_GENERATED_KEY +"='"+ sGenKey + "'.");
	    if (generatedKeys!=null && parentKeys!=null && 
	    		!((generatedKeys.length==1 && parentKeys.length>1) || generatedKeys.length == parentKeys.length)) {
	    	throw new TransformerException("Wrong value " + ATTRIBUTE_PARENT_KEY + "='"+ sParentKeys +
	    			"' or " + ATTRIBUTE_GENERATED_KEY +"='"+ sGenKey + "'.");
	    }
	    
	    
	    // parse xpath elements
	    NodeList xpaths = context.getChildNodes();
	    XPathElement xpathElement;
	    Node node;
	    for (int i=0; i<xpaths.getLength(); i++) {
	    	node = xpaths.item(i);
	    	if (node.getNodeName().equalsIgnoreCase(ELEMENT_XPATH)) {
	    		Map<String, String> mNamespacesCopy = new HashMap<String, String>();
	    		mNamespacesCopy.putAll(mNamespaces);
	    		xpathElement = parseXpathElement((Element)node, mNamespacesCopy, sDefaultNamespace);
		    	if (xpathElement != null) {
			    	xpathContext.assignXPath(xpathElement);
		    	}
		    	continue;
	    	}
	    	if (node.getNodeName().equalsIgnoreCase(ELEMENT_CONTEXT)) {
	    		Map<String, String> mNamespacesCopy = new HashMap<String, String>();
	    		mNamespacesCopy.putAll(mNamespaces);
	    		xpathContext.assignXPathContext(parseXpathContext(node, mNamespacesCopy, sDefaultNamespace));
	    		continue;
	    	}
	    }
	    return xpathContext;
	}
	
	private XPathElement parseXpathElement(Element element, Map<String, String> mNamespaces, String sDefaultNamespace) throws DOMException, TransformerException, ComponentNotReadyException {
	    Node xpathAttribute = element.getAttributes().getNamedItem(ATTRIBUTE_XPATH);
	    Node nodeNameAttribute = element.getAttributes().getNamedItem(ATTRIBUTE_NODE_NAME);
	    Node cloverFieldAttribute = element.getAttributes().getNamedItem(ATTRIBUTE_CLOVERFIELD);
	    Node trimAttribute = element.getAttributes().getNamedItem(ATTRIBUTE_TRIM);
	    Node aNamespacePaths = element.getAttributes().getNamedItem(ATTRIBUTE_NAMESPACE_PATHS);
	    
	    if (xpathAttribute == null && nodeNameAttribute == null)
	    	throw new TransformerException("Attribute '" + ATTRIBUTE_XPATH + "' or '" + ATTRIBUTE_NODE_NAME + "' not found.");

	    if (xpathAttribute != null && nodeNameAttribute != null)
	    	throw new TransformerException("There must be defined just one attribute '" + ATTRIBUTE_XPATH + "' or '" + ATTRIBUTE_NODE_NAME + "'.");
	    
	    if (cloverFieldAttribute == null)
	    	throw new TransformerException("Attribute " + ATTRIBUTE_CLOVERFIELD + " not found.");

	    mNamespaces.putAll(getNamespaces(aNamespacePaths));
	    String newDefaultNamespace = getDefaultNamespace(aNamespacePaths);
	    if (newDefaultNamespace != null) {
	    	sDefaultNamespace = newDefaultNamespace;
	    }
	    XPathEvaluator evaluator = getXPathEvaluator(mNamespaces, sDefaultNamespace);
	    	    
	    XPathElement xpathElement;
	    if (xpathAttribute == null) {
	    	switch(dataModel) {
	    		case W3C_XSD:
	    			xpathElement = new XPathElementOfXSDType(
	    		    		nodeNameAttribute.getNodeValue(), 
	    		    		cloverFieldAttribute.getNodeValue());
	    			break;
	    		default:
	    			xpathElement = new XPathElement(
	    		    		nodeNameAttribute.getNodeValue(), 
	    		    		cloverFieldAttribute.getNodeValue());
	    			break;
	    	}
	    } else {
	    	switch(dataModel) {
    		case W3C_XSD:
    			xpathElement = new XPathElementOfXSDType(
    					evaluator.createExpression(xpathAttribute.getNodeValue()), 
    		    		cloverFieldAttribute.getNodeValue());
    			break;
    		default:
    			xpathElement = new XPathElement(
    					evaluator.createExpression(xpathAttribute.getNodeValue()), 
    		    		cloverFieldAttribute.getNodeValue());
    			break;
	    	}
	    }
	    
	    if (trimAttribute != null)
	    	xpathElement.setTrim(Boolean.parseBoolean(trimAttribute.getNodeValue()));
	    
	    return xpathElement; 
	}

	private String getDefaultNamespace(Node namespacePaths) throws ComponentNotReadyException {
		if (namespacePaths == null) {
			return null;
		}

		String path;
		for (String namespacePath: namespacePaths.getNodeValue().split(";")) {
			Matcher matcherDefault = NAMESPACE_DEFAULT.matcher(namespacePath);
			Matcher matcher = NAMESPACE.matcher(namespacePath);
			if (!matcherDefault.find()) {
				if (!matcher.find()) {
					throw new ComponentNotReadyException("The namespace expression '"+ namespacePath +"' is not valid.");
				} else {
					continue;
				}
			}
			if ((path = matcherDefault.group(2)) != null) {
				return path;
			} 
		}
		return null;
	}
	
	
	private Map<String, String> getNamespaces(Node namespacePaths) throws ComponentNotReadyException {
		Map<String, String> namespaces = new HashMap<String, String>();
		if (namespacePaths == null) return namespaces;
		String ns;
		String path;
		for (String namespacePath: namespacePaths.getNodeValue().split(";")) {
			Matcher matcher = NAMESPACE.matcher(namespacePath);
			Matcher matcherDefault = NAMESPACE_DEFAULT.matcher(namespacePath);

			if (!matcher.find()) {
				if (!matcherDefault.find()) throw new ComponentNotReadyException("The namespace expression '"+ namespacePath +"' is not valid.");
				else continue;
			} 
			if ((ns = matcher.group(1)) != null && (path = matcher.group(3)) != null) {
				namespaces.put(ns, path);
			}
		}
		return namespaces;
	}
	
	private XPathEvaluator getXPathEvaluator(Map<String, String> namespacePaths, String defaultNamespace) {
		XPathEvaluator evaluator = new XPathEvaluator();
		if (defaultNamespace != null) {
			evaluator.setDefaultElementNamespace(defaultNamespace);
		}
		evaluator.setNamespaceResolver(getNamespacesResolver(namespacePaths));
		xPathEvaluators.add(evaluator);
		
		return evaluator;
	}
	
	private NamespaceResolver getNamespacesResolver(Map<String, String> namespacePaths) {
		IndependentContext context = new IndependentContext();
		if (namespacePaths != null) {
			for (Entry<String, String> entry : namespacePaths.entrySet()) {
				context.declareNamespace(entry.getKey(), entry.getValue());
			}
		}
		return context.getNamespaceResolver();
	}
	
	/**
	 *  Returs next data record parsed from input stream or NULL if no more data
	 *  available The specified DataRecord's fields are altered to contain new
	 *  values.
	 *
	 *@param  record           Description of Parameter
	 *@return                  The Next value
	 *@exception  IOException  Description of Exception
	 *@since                   May 2, 2002
	 */
	@Override
	public DataRecord getNext(DataRecord record) throws JetelException {
		throw new JetelException("The method is not supported for this parser.");
	}

	@Override
	public DataRecord getNext() throws JetelException {
		try {
			recordResult = xpathContext.getNext();
	        if(exceptionHandler != null ) {  //use handler only if configured
	            while(exceptionHandler.isExceptionThrowed()) {
	                exceptionHandler.handleException();
	        		recordResult = xpathContext.getNext();
	            }
	        }
		} catch (TransformerException e) {
			throw new JetelException("", e);
		}
		return recordResult;
	}

	/**
	 * This method can be removed in a future release.
	 * @param metadata ignored parameter
	 * @throws ComponentNotReadyException
	 */
	@Deprecated
	public void init(DataRecordMetadata metadata) throws ComponentNotReadyException {
		init();
	}

	@Override
	public void init() throws ComponentNotReadyException {
		try {
			// create and init factory
		    SAXParserFactory factory = SAXParserFactory.newInstance();
			initXmlFeatures(factory);

			// create a xml reader
		    reader = factory.newSAXParser().getXMLReader();
		    
		    // error handler
		    if (exceptionHandler instanceof StrictParserExceptionHandler) {
				ErrorListener errorListener = new ErrorListener() {
					@Override
					public void warning(TransformerException exception)
							throws TransformerException {
							throw exception;
					}
					@Override
					public void fatalError(TransformerException exception)
							throws TransformerException {
						exceptionHandler.populateHandler(exception.getMessage(), recordResult, -1, -1, null, new BadDataFormatException(exception.getMessage()));
					}
					@Override
					public void error(TransformerException exception)
							throws TransformerException {
						exceptionHandler.populateHandler(exception.getMessage(), recordResult, -1, -1, null, new BadDataFormatException(exception.getMessage(), exception));
					}
				};
				
				for (XPathEvaluator xPathEvaluator : xPathEvaluators) {
					xPathEvaluator.getConfiguration().setErrorListener(errorListener);
				}
		    }
		} catch (Exception e) {
			throw new ComponentNotReadyException(e);
		}
	}

	/**
	 * Xml features initialization.
	 */
	private void initXmlFeatures(SAXParserFactory factory) throws ComponentNotReadyException, SAXNotRecognizedException, SAXNotSupportedException, ParserConfigurationException {
		if (xmlFeatures == null) return;
		String[] aXmlFeatures = xmlFeatures.split(FEATURES_DELIMETER);
		String[] aOneFeature;
		for (String oneFeature: aXmlFeatures) {
			aOneFeature = oneFeature.split(FEATURES_ASSIGN);
			if (aOneFeature.length != 2) 
				throw new ComponentNotReadyException("The xml feature '" + oneFeature + "' has wrong format");
		    factory.setFeature(aOneFeature[0], Boolean.parseBoolean(aOneFeature[1]));
		}
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#setDataSource(java.lang.Object)
	 */
	@Override
	public void setReleaseDataSource(boolean releaseInputSource)  {
	}

	/**
	 * Sets input data source.
	 * 
	 * Processing is done when getNext is called.
	 * 
	 * @param inputDataSource accepts java.io.InputStream or java.nio.channels.ReadableByteChannel
	 */
	@Override
	public void setDataSource(Object inputDataSource) throws ComponentNotReadyException {
		InputStream input;
		if (inputDataSource instanceof InputStream) {
			input = (InputStream)inputDataSource;
		}else{
			input = Channels.newInputStream((ReadableByteChannel)inputDataSource);
		}
		try {
			xpathContext.init(new SAXSource(reader, new InputSource(input)));
		} catch (Exception e) {
			throw new ComponentNotReadyException(e);
		}
	}

	/**
	 *  Release resources
	 *
	 *@since    May 2, 2002
	 */
	@Override
	public void close() {
	}

	/**
	 * Returns data policy type for this parser
	 * @return Data policy type or null if none was specified
	 */
	@Override
	public PolicyType getPolicyType() {
		if (this.exceptionHandler != null) {
			return this.exceptionHandler.getType();
		} else {
			return null;
		}
	}

    @Override
	public void setExceptionHandler(IParserExceptionHandler handler) {
        this.exceptionHandler = handler;
    }

    @Override
	public IParserExceptionHandler getExceptionHandler() {
        return exceptionHandler;
    }

	/**
	 * Skip records.
	 * @param nRec Number of records to be skipped
	 * @return Number of successfully skipped records.
	 * @throws JetelException
	 */
	@Override
	public int skip(int nRec) throws JetelException {
		int skipped;
		for (skipped = 0; skipped < nRec; skipped++) {
			if (getNext() == null) {  // end of file reached
				break;
			}
		}
		return skipped;
	}
	
	public void setXPath(Document xpathAttribute) {
		this.xpathDocument = xpathAttribute;
	}

	public Document getXPath() {
		return xpathDocument;
	}

	public void assignRecord(DataRecord record, int i) {
		if (isReseted) return;
		xpathContext.assignRecord(record, i);
	}
	
	public List<Integer> getPorts() {
		return ports;
	}
	
	/***
	 * Returns port id where should be record obtained by getNext() send.
	 * 
	 * @return port id
	 */
	public int getActualPort() {
		return xpathContext.getActualPort();
	}

	public void setGraph(TransformationGraph graph) {
		this.graph = graph;
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#reset()
	 */
	@Override
	public void reset() throws ComponentNotReadyException {
		init();
		//xpathContext.reset();
		//isReseted = true;
	}

	@Override
	public Object getPosition() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void movePosition(Object position) {
		// TODO Auto-generated method stub
		
	}

	public void setSkip(int skipRows) {
		this.skipRows = skipRows;
	}

	public void setNumRecords(int numRecords) {
		this.numRecords = numRecords;
	}
	
	public void setXmlFeatures(String xmlFeatures) {
		this.xmlFeatures = xmlFeatures;
	}

	/**
	 * @return the dataModel
	 */
	public SupportedDataModels getDataModel() {
		return dataModel;
	}

	/**
	 * @param dataModel the dataModel to set
	 */
	public void setDataModel(SupportedDataModels dataModel) {
		this.dataModel = dataModel;
	}

	@Override
	public void preExecute() throws ComponentNotReadyException {
		try {
			// parse xml mapping
			xpathContext = parseXPath(xpathDocument);
		} catch (Exception ex) {
			throw new ComponentNotReadyException(ex);
		}
	}

	@Override
	public void postExecute() throws ComponentNotReadyException {
		reset();
	}

	@Override
	public void free() {
		close();
	}

	@Override
	public boolean nextL3Source() {
		return false;
	}
	
}
