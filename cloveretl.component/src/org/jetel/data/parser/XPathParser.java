package org.jetel.data.parser;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.LinkedList;
import java.util.List;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.sax.SAXSource;

import net.sf.saxon.sxpath.XPathEvaluator;
import net.sf.saxon.trans.XPathException;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.sequence.Sequence;
import org.jetel.data.sequence.SequenceFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * This class is parser for Xml XPath reader. 
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 * @created 20.5.2007
 */
public class XPathParser implements Parser {

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

    private TransformationGraph graph;
    private XPathContext xpathContext;
	private Document xpathDocument;
	
	private IParserExceptionHandler exceptionHandler;

	private List<Integer> ports;

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
	    
		return parseXpathContext(node);
	}
	
	private XPathContext parseXpathContext(Node context) throws DOMException, TransformerException, ComponentNotReadyException {
	    // create xpathContext class
	    Node aNodeSet = context.getAttributes().getNamedItem(ATTRIBUTE_XPATH);
	    Node aOutPort = context.getAttributes().getNamedItem(ATTRIBUTE_OUTPORT);
	    Node aOutParentKey = context.getAttributes().getNamedItem(ATTRIBUTE_PARENT_KEY);
	    Node aOutGenKey = context.getAttributes().getNamedItem(ATTRIBUTE_GENERATED_KEY);
	    Node aSeqField = context.getAttributes().getNamedItem(XML_SEQUENCEFIELD);
	    Node aSeqId = context.getAttributes().getNamedItem(XML_SEQUENCEID);

	    XPathContext xpathContext = new XPathContext(aNodeSet.getNodeValue());
	    if (aOutPort != null) {
		    int port = Integer.parseInt(aOutPort.getNodeValue());
		    ports.add(port);
    		xpathContext.setPort(port);
	    }

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
        	/*<Context xpath="/bookstore/bookstore" outPort="0" sequenceField="a4" sequenceId="Sequence0">
    		<Context xpath="book" outPort="1" parentKey="a4;a2" generatedKey="a1;a2">
    			<Context xpath="author" outPort="3">
    				<Mapping xpath="text()" cloverField="a2"/> 
    			</Context>
    			<Context xpath="title" outPort="2" parentKey="a1" generatedKey="a1">
    				<Mapping xpath="@lang" cloverField="a3"/> 
    			</Context>
    			<Mapping xpath="substring(book/author[1]/text(), 1)" cloverField="a4"/> 
    		</Context>
    		<Mapping nodeName="author" cloverField="a1"/> 
    	</Context>*/
        	xpathContext.setSequence(
        			SequenceFactory.createSequence(
                			graph, 
                			"org.jetel.sequence.PrimitiveSequence", 
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
		    	xpathElement = parseXpathElement((Element)node);
		    	if (xpathElement != null) {
			    	xpathContext.assignXPath(xpathElement);
		    	}
		    	continue;
	    	}
	    	if (node.getNodeName().equalsIgnoreCase(ELEMENT_CONTEXT)) {
	    		xpathContext.assignXPathContext(parseXpathContext(node));
	    		continue;
	    	}
	    }
	    return xpathContext;
	}
	
	private XPathElement parseXpathElement(Element element) throws DOMException, TransformerException {
	    Node xpathAttribute = element.getAttributes().getNamedItem(ATTRIBUTE_XPATH);
	    Node nodeNameAttribute = element.getAttributes().getNamedItem(ATTRIBUTE_NODE_NAME);
	    Node cloverFieldAttribute = element.getAttributes().getNamedItem(ATTRIBUTE_CLOVERFIELD);
	    Node trimAttribute = element.getAttributes().getNamedItem(ATTRIBUTE_TRIM);
	    
	    if (xpathAttribute == null && nodeNameAttribute == null)
	    	throw new TransformerException("Attribute '" + ATTRIBUTE_XPATH + "' or '" + ATTRIBUTE_NODE_NAME + "' not found.");

	    if (xpathAttribute != null && nodeNameAttribute != null)
	    	throw new TransformerException("There must be defined just one attribute '" + ATTRIBUTE_XPATH + "' or '" + ATTRIBUTE_NODE_NAME + "'.");
	    
	    if (cloverFieldAttribute == null)
	    	throw new TransformerException("Attribute " + ATTRIBUTE_CLOVERFIELD + " not found.");

	    XPathElement xpathElement;
	    if (xpathAttribute == null) {
	    	xpathElement = new XPathElement(
		    		nodeNameAttribute.getNodeValue(), 
		    		cloverFieldAttribute.getNodeValue());
	    } else {
	    	xpathElement = new XPathElement(
		    		new XPathEvaluator().createExpression(xpathAttribute.getNodeValue()), 
		    		cloverFieldAttribute.getNodeValue());
	    }
	    
	    if (trimAttribute != null)
	    	xpathElement.setTrim(Boolean.parseBoolean(trimAttribute.getNodeValue()));
	    
	    return xpathElement; 
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
	public DataRecord getNext(DataRecord record) throws JetelException {
		throw new JetelException("The method is not supported for this parser.");
	}

	public DataRecord getNext() throws JetelException {
		DataRecord recordResult;
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


	public void init(DataRecordMetadata metadata)
	throws ComponentNotReadyException {		
		try {
			xpathContext = parseXPath(xpathDocument);
		} catch (Exception e) {
			throw new ComponentNotReadyException(e);
		}
	}

	public void setDataSource(Object inputDataSource) throws ComponentNotReadyException {
		InputStream input;
		if (inputDataSource instanceof InputStream) {
			input = (InputStream)inputDataSource;
		}else{
			input = Channels.newInputStream((ReadableByteChannel)inputDataSource);
		}
		try {
			xpathContext.init(new SAXSource(new InputSource(input)));
		} catch (Exception e) {
			throw new ComponentNotReadyException(e);
		}
	}

	/**
	 *  Release resources
	 *
	 *@since    May 2, 2002
	 */
	public void close() {
	}

	/**
	 * Returns data policy type for this parser
	 * @return Data policy type or null if none was specified
	 */
	public PolicyType getPolicyType() {
		if (this.exceptionHandler != null) {
			return this.exceptionHandler.getType();
		} else {
			return null;
		}
	}

    public void setExceptionHandler(IParserExceptionHandler handler) {
        this.exceptionHandler = handler;
    }

    public IParserExceptionHandler getExceptionHandler() {
        return exceptionHandler;
    }

	/**
	 * Skip records.
	 * @param nRec Number of records to be skipped
	 * @return Number of successfully skipped records.
	 * @throws JetelException
	 */
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
		xpathContext.assignRecord(record, i);
	}
	
	public List<Integer> getPorts() {
		return ports;
	}
	
	public int getActualPort() {
		return xpathContext.getActualPort();
	}

	public void setGraph(TransformationGraph graph) {
		this.graph = graph;
	}
}
