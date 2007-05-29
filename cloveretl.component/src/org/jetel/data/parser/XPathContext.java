package org.jetel.data.parser;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.xml.transform.Source;
import javax.xml.transform.TransformerException;

import net.sf.saxon.om.NodeInfo;
import net.sf.saxon.sxpath.XPathEvaluator;
import net.sf.saxon.sxpath.XPathExpression;
import net.sf.saxon.trans.XPathException;

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.sequence.Sequence;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataFieldMetadata;

/**
 * The XPathContext is class that evaluates xpaths and other xpath contexts. 
 * A result is stored into DataRecord.
 * 
 * @author ausperger
 */
public class XPathContext {

	private static final int NO_PORT = -1;

	private String xpath;
	private XPathExpression exp; 
	private List<XPathElement> xpathsList;
	private List<XPathContext> xpathContextList;
	private DataRecord record = null;
	private int port = NO_PORT;	
	private int actualPort;
	private String[] parentKeys;
	private String[] generatedKeys;
	private int[] iParentKeys;
	private int[] iGeneratedKeys;
	private Sequence sequence = null;
	private String sequenceFieldName;
	
	// source xml document
	private Source node;

	// next evaluation context 
	private XPathContext nextXPathContext;
	
	// iterator on this context
	private Iterator contextIterator = null;
	
	// actual node of this context (from contextIterator)
	private NodeInfo actualNode = null;
	
	// indicates last node for port
	private boolean lastPortNode = false;
	//private boolean lastPortNode2 = false;

	// indicates last node for document
	private boolean lastNode = false;
	
	// points to parent node in tree structure
	private XPathContext parentContext = null;
	
	private boolean init = false;

	// points to next port context
	private List<XPathContext> orderedPortList = new LinkedList<XPathContext>();
	
	// points to context where are keys
	private XPathContext parentContext4Keys = null;
	
	// this is used during parentKeys and generatedKeys
	private boolean recordLoaded = false;
	private boolean recordLoadedLast = false;
	
	private DataField sequenceField = null;
	
	private BadDataFormatException bdfe = null;
	
	private StringBuilder stringBuilder = new StringBuilder();
	
	/**
	 * Constructor
	 */
	private XPathContext() {}
	
	/**
	 * Creates new xpath context. 
	 * 
	 * @param xpath is xpath expression that defines context
	 * @throws JetelException
	 * @throws XPathException 
	 */
	public XPathContext(String xpath) throws TransformerException, XPathException {
		if (xpath == null) 
			throw new TransformerException("XPath cannot be null value.");
		exp = new XPathEvaluator().createExpression(xpath);
		this.xpath = xpath;
		xpathsList = new LinkedList<XPathElement>();
		xpathContextList = new LinkedList<XPathContext>();
	}
	
	/**
	 * @return context xpath expression
	 */
	public String getXpath() {
		return xpath;
	}

	/**
	 * @param xpathElement is single xpath for evaluation in context
	 */
	public void assignXPath(XPathElement xpathElement) {
		xpathsList.add(xpathElement);
	}
	
	/**
	 * @return list of xpaths
	 */
	public List<XPathElement> getXPaths() {
		return xpathsList;
	}
	
	/**
	 * @param xpathContext is single xpath context 
	 */
	public void assignXPathContext(XPathContext xpathContext) {
		this.xpathContextList.add(xpathContext);
	}

	/**
	 * @return list of context
	 */
	public List<XPathContext> getXPathContext() {
		return xpathContextList;
	}

	/**
	 * Initialize method
	 * 
	 * @param node may be document, element etc. for context and xpath evaluation. 
	 * @throws TransformerException
	 */
	public void init(Source node) throws TransformerException {
		init = true;
		this.node = node;
	}
	
	/**
	 * Returns evaluated xpaths into DataRecord value.
	 * 
	 * @param record
	 * @return
	 * @throws TransformerException
	 */
	public DataRecord getNext() throws TransformerException {
		if (lastNode) return null;
		if (init) {
			reset(node);
			initTree(this);
			nextXPathContext = null;
			createOrderedPortList(orderedPortList);
			XPathContext portXpathContext;
			for(Iterator<XPathContext> it = orderedPortList.iterator(); it.hasNext();) {
				initParentContext4Keys(portXpathContext = it.next());
				initSequences(portXpathContext);
			}
			init = false;
		}
		DataRecord tmpRecord = null;
		XPathContext portContext;
		for (Iterator<XPathContext> it=orderedPortList.iterator(); it.hasNext(); ) {
			portContext = it.next();
			if (!portContext.recordLoadedLast && portContext.record != null) 
				portContext.record.reset(); // test jeste na null
			
		    try {
				tmpRecord = portContext.getNextInner();
		    } catch(BadDataFormatException bdfe) {
		    	this.bdfe = bdfe;
		    }
			if (tmpRecord != null) {
				portContext.findAndPropagatePort();
				portContext.prepareNextValue(portContext);
				break;
			} else if (portContext.parentContext == null) {
				portContext.prepareNextValue(portContext);
				if (portContext.lastPortNode) break;
				it = orderedPortList.iterator();
			}
		}
		if (lastPortNode && parentContext == null) lastNode = true;
		if (bdfe != null) {
			try {
				throw bdfe;
			} finally {
				bdfe = null;
			}
		}
		return tmpRecord;
	}
	
	private void initSequences(XPathContext portContext) throws TransformerException {
		if (portContext.sequenceFieldName == null || portContext.record == null) return;
		Integer i = (Integer)portContext.record.getMetadata().getFieldNames().get(portContext.sequenceFieldName);
		if (i == null) 
			throw new TransformerException("Clover field name '" + portContext.sequenceFieldName + "' not found in metadata");
		portContext.sequenceField = portContext.record.getField(i);
	}
	
	private void initParentContext4Keys(XPathContext xpathContext) throws TransformerException {
		XPathContext tmp = xpathContext;
		while (tmp.parentContext != null) {
			tmp = tmp.parentContext;
			if (tmp.port != NO_PORT) {
				xpathContext.parentContext4Keys = tmp;
				if (xpathContext.generatedKeys != null) {
					xpathContext.iGeneratedKeys = new int[xpathContext.generatedKeys.length];
					for (int i=0; i<xpathContext.generatedKeys.length; i++) {
						Integer fieldPos = (Integer)xpathContext.record.getMetadata().getFieldNames().get(xpathContext.generatedKeys[i]);
						if (fieldPos == null) 
							throw new TransformerException("Clover field name '" + xpathContext.generatedKeys[i] + "' not found in metadata");
						xpathContext.iGeneratedKeys[i] = fieldPos.intValue();
					}
				}
				if (xpathContext.parentKeys != null) {
					xpathContext.iParentKeys = new int[xpathContext.parentKeys.length];
					for (int i=0; i<xpathContext.parentKeys.length; i++) {
						Integer fieldPos = (Integer)tmp.record.getMetadata().getFieldNames().get(xpathContext.parentKeys[i]);
						if (fieldPos == null) 
							throw new TransformerException("Clover field name '" + xpathContext.parentKeys[i] + "' not found in metadata");
						xpathContext.iParentKeys[i] = fieldPos.intValue();
					}
				}
				break;
			}
		}
	}
	
	private void createOrderedPortList(List<XPathContext> orderedPortList) {
		if (xpathContextList.size() == 0) {
			XPathContext tmp = this;
			while (tmp != null) {
				if (tmp.port != NO_PORT || tmp.parentContext == null) orderedPortList.add(tmp);
				tmp = tmp.nextXPathContext;
			}
		} else {
			xpathContextList.get(xpathContextList.size()-1).createOrderedPortList(orderedPortList);
		}
	}

	/**
	 * Evaluates xpaths.
	 */
	private DataRecord getNextInner() throws TransformerException {
		if (lastPortNode || record == null || actualNode == null || port == NO_PORT) return null;
		if (recordLoaded || recordLoadedLast) {
			recordLoadedLast = false;
			return record;
		}

		for (XPathElement xpath : xpathsList) {
			record = xpath.getValue(record, actualNode);
		}
		if (sequence != null) {
	        if(sequenceField.getType() == DataFieldMetadata.INTEGER_FIELD) {
	            sequenceField.setValue(sequence.nextValueInt());
	        } else if(sequenceField.getType() == DataFieldMetadata.LONG_FIELD
	                || sequenceField.getType() == DataFieldMetadata.DECIMAL_FIELD
	                || sequenceField.getType() == DataFieldMetadata.NUMERIC_FIELD) {
	            sequenceField.setValue(sequence.nextValueLong());
	        } else {
	        	String sdf = sequence.nextValueString();
	            sequenceField.fromString(sdf);
	        }
		}
		
		if (parentKeys != null) {
			if (!recordLoaded) {
				if (parentContext4Keys == null) throw new TransformerException("The parent context hasn't defined outport.");
				parentContext4Keys.getNextInner();
				parentContext4Keys.recordLoaded = true;
				Object value;
				stringBuilder.setLength(0);
				for (int i=0; i<iParentKeys.length; i++) {
					value = parentContext4Keys.record.getField(iParentKeys[i]).getValue();
					if (value != null) {
						if (iGeneratedKeys.length == 1 && iParentKeys.length > 1) {
							stringBuilder.append(value.toString());
							record.getField(iGeneratedKeys[0]).fromString(stringBuilder.toString());
						} else {
							record.getField(iGeneratedKeys[i]).fromString(value.toString());
						}
					}
				}
			}
		}
		
		for (XPathContext context : xpathContextList) {
			if (!context.lastPortNode && context.port == NO_PORT) record = context.getNextInner();
		}
		return record;
	}

	/**
	 * Context tree initialization.
	 */
	private XPathContext initTree(XPathContext previousContext) {
		nextXPathContext = previousContext;
		XPathContext tmp = null;
		for (XPathContext context : xpathContextList) {
			tmp = context.initTree(tmp!=null ? tmp : this);
			context.parentContext = this;
		}
		return tmp!=null ? tmp : this;
	}
	
	/**
	 * Prepares next value for getNext method.
	 */
	private void prepareNextValue(XPathContext borderContext) throws TransformerException {
		if (xpathContextList.size() == 0) {
			prepareValueInner(borderContext);
		} else {
			xpathContextList.get(xpathContextList.size()-1).prepareNextValue(borderContext);
		}
	}

	/**
	 * Inner method for prepareNextValue.
	 */
	private void prepareValueInner(XPathContext borderContext) throws TransformerException {
		if (contextIterator == null || !contextIterator.hasNext()) {
			if (borderContext != this) {
				if (port == NO_PORT)  //TODO: je potreba otestovat
					lastPortNode = false;
				if (nextXPathContext != null) {
					nextXPathContext.prepareValueInner(borderContext);
				} else {
					lastPortNode = true;
				}
				return;
			} else {
				lastPortNode = true;
				if (parentContext4Keys != null) {
					if (parentContext4Keys.recordLoaded) {
						parentContext4Keys.recordLoaded = false;
						parentContext4Keys.recordLoadedLast = true;
					}
				}
				
				// all previous context must indicated last lastPortNode is true
				XPathContext tmp = this;
				while (tmp.parentContext != null) tmp = tmp.parentContext;
				for (XPathContext portContext : tmp.orderedPortList) {
					if (portContext == borderContext) break;
					portContext.lastPortNode = true;
				}
				return;
			}
		}
		actualNode = (NodeInfo) contextIterator.next();
		for (XPathContext context : xpathContextList) {
			context.reset(actualNode);
		}
		if (parentContext != null) {
			boolean reset = false;
			for (XPathContext context : parentContext.xpathContextList) {
				if (reset) {
					if (context.port == NO_PORT)
						context.reset(parentContext.actualNode);
				} else {
					reset = context == this; 
				}
			}
		}
	}
	
	private void findAndPropagatePort() {
		XPathContext tmpContext;
		tmpContext = this;
		// 1) find first port
		while (tmpContext != null) {
			if (tmpContext.port != NO_PORT) {
				tmpContext.actualPort = tmpContext.port;
				break;
			}
			tmpContext = tmpContext.parentContext;
		}
		// 2) propagate port
		while (tmpContext != null) {
			if (tmpContext.parentContext != null)
				tmpContext.parentContext.actualPort = tmpContext.actualPort;
			tmpContext = tmpContext.parentContext;
		}
	}
	
	/**
	 * Resets context subtree for some node.
	 */
	private void reset(Source contextNode) throws TransformerException {
		lastPortNode = false; //TODO: je potreba otestovat
		contextIterator = exp.evaluate(contextNode).iterator();
		// TODO udelat test, kolikrat a jak se zavola evaluate
		if (!contextIterator.hasNext()) {
			actualNode = null;
			return;
		}
		actualNode = (NodeInfo)contextIterator.next();
		if (actualNode != null) {
			for (XPathContext context : xpathContextList) {
				context.reset(actualNode);
			}
		}
	}
	
	public boolean assignRecord(DataRecord record, int i) {
		if (port == i) {
			for (XPathContext context : xpathContextList) {
				assignSubContexts(record, context);
			}			
			if (this.record != null) throw new RuntimeException("Error: there is found duplicated output port '"+i+"' in xpath mapping");
			
			String fieldName;
			boolean allCloverFieldsDoesntMatched;
			for (int j=0; j<record.getNumFields(); j++) {
				fieldName = record.getField(j).getMetadata().getName();
				allCloverFieldsDoesntMatched = true;
				for (XPathElement xpathElement : xpathsList) {
					if (xpathElement.getCloverField().equals(fieldName)) {
						allCloverFieldsDoesntMatched = false;
						break;
					}
				}
				if (allCloverFieldsDoesntMatched && sequenceField != null && sequenceField.getMetadata().getName().equals(fieldName)) {
					allCloverFieldsDoesntMatched = false;
				}
				if (allCloverFieldsDoesntMatched && generatedKeys != null) {
					for (String genName : generatedKeys) {
						if (fieldName.equals(genName)) {
							allCloverFieldsDoesntMatched = false;
							break;
						}
					}
				}
				if (allCloverFieldsDoesntMatched) {
					xpathsList.add(new XPathElement(fieldName, fieldName));
				}
			}
			this.record = record;
			return true;
		}
		for (XPathContext context : xpathContextList) {
			if (context.assignRecord(record, i)) 
				return true;
		}
		return false;
	}
	
	private void assignSubContexts(DataRecord record, XPathContext context) {
		if (context.port != NO_PORT) return;
		context.record = record;		
		for (XPathContext subContext : context.xpathContextList) {
			assignSubContexts(record, subContext);
		}		
	}
	
	public void setPort(int port) {
		this.port = port;
	}
	
	public int getActualPort() {
		return actualPort;
	}

	public void setParentKeys(String[] parentKeys) {
		this.parentKeys = parentKeys;
	}
	
	public void setGeneratedKeys(String[] generatedKeys) {
		this.generatedKeys = generatedKeys;
	}

	public void setSequence(Sequence sequence) {
		this.sequence = sequence;
	}
	
	public void setSequenceField(String sequenceFieldName) {
		this.sequenceFieldName = sequenceFieldName;
	}
	
}
