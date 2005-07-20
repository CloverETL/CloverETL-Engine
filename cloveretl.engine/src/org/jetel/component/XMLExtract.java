/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-05  David Pavlis <david_pavlis@hotmail.com>
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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.w3c.dom.NamedNodeMap;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Provides the logic to parse a xml file and filter to different ports based on
 * a matching element. The element and all children will be turned into a
 * Datarecord and processed by other components. For example: given an xml file
 * of the type: 
 * <a>
 *   <b id="x"> 
 *     <c>hello</c1> 
 *   </b>  
 *   <d name="y"> 
 *     <e>bye</e>
 * 	 </d>
 *   <b id="z"> 
 *     <c>hello again</c>
 *   </b>
 * </a> 
 * 
 * and a mapping of b->port 1 and d->port 2 port 1 will receive: 
 * Column: id c 
 * Row 1: x hello 
 * Row 2: z hello
 * again port 2 will receive: 
 * Column: name d 
 * Row 1: y bye 
 * 
 * Issue: Nested XML implying foreign key relationships are not supported 
 * i.e.
 * <person>
 *   <name>blah</name>
 *   <address><line1>123 Main</line1></address>
 * </person>
 * Where address was another table. (this will flatten it into one table) 
 * 
 * Issue:
 * Enclosing elements having values are not supported 
 * i.e. 
 * <x>
 *   <y>z</y>
 *   xValue
 * </x>
 * there will be no column x with value xValue. 
 * 
 * Issue: Namespaces are not considered 
 * i.e. 
 * <ns1:x>xValue</ns1:x>
 * <ns2:x>xValue2</ns2:x> 
 * will be in the same column x.
 * 
 * @author KKou
 */
public class XMLExtract extends Node {
	// Logger
	private static final Log LOG = LogFactory.getLog(XMLExtract.class);
	
	/**
	 * SAX Handler that will dispatch the elements to the different ports.
	 */
	private class SAXHandler extends DefaultHandler {
		// depth of the element, used to determine when we hit the matching close element
		private int m_activeRecordLevel = 0;
		// flag set if we saw characters, otherwise don't save the column (used to set null values)
		private boolean m_hasCharacters = false;
		// the record being updated
		private DataRecord m_activeRecord = null; 
		// buffer for node value
		private StringBuffer m_characters = new StringBuffer(); 
		
		private String nodeID = null;

		private SAXHandler(String nodeID){
		    this.nodeID=nodeID;
		}
		
		public void startElement(String prefix, String namespace,
				String localName, Attributes attributes) throws SAXException {
			if (m_activeRecord == null) {
				// Not in a matched element, check to see if this element
				// matches any we are interested in.
				if (m_elementPortMap.containsKey(localName)) {
					// We have a match, start converting all child nodes into
					// the DataRecord structure
					Integer portInt = (Integer) m_elementPortMap.get(localName);
					// if port isn't mapped, the activeRecord will be set to null below, so it's ok
					m_activeRecord = (DataRecord) m_records.get(portInt);
					if (m_activeRecord == null) {
						// If it's null that means that there's no edge mapped to the output port
						// remove this mapping so we don't repeat this logic (and logging)
						LOG.warn("XML Extract: " + nodeID + " Element (" + localName + ") does not have an edge mapped to that port.");
						m_elementPortMap.remove(localName);
					}
					// Below code will increment it to 0 if we got an active record
					m_activeRecordLevel = -1;
				}
			}

			if (m_activeRecord != null) {
				// In a matched element (i.e. we are creating a DataRecord)
				// Store all attributes as columns (this hasn't been used/tested)
				m_activeRecordLevel++; // Start element so increase our depth
				for (int i = 0; i < attributes.getLength(); i++) {
					DataField field = m_activeRecord.getField(attributes
							.getLocalName(i));
					field.setValue(attributes.getValue(i));
				}
			}
		}

		public void characters(char[] data, int offset, int length)
				throws SAXException {
			// Save the characters into the buffer, endElement will store it into the field
			if (m_activeRecord != null) {
				m_characters.append(data, offset, length);
				m_hasCharacters = true;
			}
		}

		public void endElement(String prefix, String namespace, String localName)
				throws SAXException {
			if (m_activeRecord != null && m_characters.length() > 0) {
				// Store the characters processed by the characters() call back
				DataField field = m_activeRecord.getField(localName);
				if (field != null) {
					// If field is nullable and there's no character data set it
					// to null
					if (!m_hasCharacters && field.getMetadata().isNullable()) {
						field.setNull(true); // it's null
					} else {
						field.setNull(false); // it's not null
						try {
							field.fromString(m_characters.toString());
						} catch (BadDataFormatException ex) {
							// This is a bit hacky here SOOO let me explain...
							if (field.getType() == 'D') {
								// XML dateTime format is not supported by the
								// DateFormat oject that clover uses... 
								// so timezones are unparsable
								// i.e. XML wants -5:00 but DateFormat wants -500
								// Attempt to munge and retry... (there has to be a better way)
								try {
									// Chop off the ":" in the timezone (it HAS to be at the end)
									String dateTime = m_characters.substring(0,
											m_characters.lastIndexOf(":"))
											+ m_characters.substring(m_characters
													.lastIndexOf(":") + 1);
									DateFormat format =
										new SimpleDateFormat(field.getMetadata().getFormatStr());
									field.setValue(format.parse(dateTime.trim()));
								} catch (Exception ex2) {
									// Oh well we tried, throw the originating exception
									throw new SAXException("Error setting field: "
											+ localName + " data: "
											+ m_characters.toString() + " "
											+ ex.getMessage(), ex);
								}
							} else {
								throw ex;
							}
						}
					}
				}

				// Regardless of whether this was saved, reset the length of the buffer and flag
				m_characters.setLength(0);
				m_hasCharacters = false;
			}

			if (m_activeRecordLevel == 0 && m_activeRecord != null) {
				// This is the closing element of the matched element that triggered the processing
				// That should be the end of this record so send it off to the next Node
				Integer portInt = (Integer) m_elementPortMap.get(localName);
				if (portInt == null) {
					// Mapping not found for this element!
					// This should never happen as the start element will not set the m_activeRecord
					// unless there was an output port for it.
				} else if (runIt) {
					try {
						((OutputPort) outPorts.get(portInt))
								.writeRecord(m_activeRecord);
						m_activeRecord = null;
						// TODO Do we need to clean out the record object? Other
						// components from clover seems to reuse it, keep an 
						// eye on data not cleaned out between runs
					} catch (Exception ex) {
						throw new SAXException(ex);
					}
				} else {
					throw new SAXException("Stop Signaled");
				}
			}
			m_activeRecordLevel--; // ended an element so decrease our depth
		}
	}
	
	// Map of elementName => output port
	private Map m_elementPortMap = new HashMap();

	// Map of elementName => record
	private Map m_records;

	// Where the XML comes from
	private InputSource m_inputSource;

	/**
	 * Constructs an XML Extract node with the given id.
	 */
	public XMLExtract(String id) {
		super(id);
	}

	/**
	 * Perform sanity checks.
	 */
	public void init() throws ComponentNotReadyException {
		// test that we have at least one input port and one output
		// TODO: Flesh out the initialization/add in more checks
		if (outPorts.size() < 1) {
			throw new ComponentNotReadyException(
					"At least one output port has to be defined!");
		}
	}

	/**
	 * Call back from the Clover Engine starting this node.
	 */
	public void run() {
		// Allocate the records and put them into a map so we can retrieve them later
		m_records = new TreeMap();
		Iterator itr = outPorts.entrySet().iterator();
		while (itr.hasNext()) {
			Map.Entry entry = (Map.Entry) itr.next();
			OutputPort outport = (OutputPort) entry.getValue();
			DataRecord record = new DataRecord(outport.getMetadata());
			m_records.put(entry.getKey(), record);

			// Initialize the DataRecord.
			record.init();
		}

		// Parse the XML file
		if (parseXML()) {
			// We have successfully sent out all the data, send out the EOF signal
			broadcastEOF();

			// determine if we successfully sent out everything or we successfully stopped
			// due to a stop signal
			if (runIt)
				resultMsg = "OK";
			else
				resultMsg = "STOPPED";
			resultCode = Node.RESULT_OK;
		} else {
			// If it's false, the result message and codes have been set in the SAX event handler.
			broadcastEOF();
			return;
		}
	}

	/**
	 * Set the input source containing the XML this will parse.
	 */
	public void setInputSource(InputSource inputSource) {
		this.m_inputSource = inputSource;
	}
	
	/**
	 * Accessor to add a mapping programatically.
	 */
	public void addMapping(String localName, Integer portNumber) {
		this.m_elementPortMap.put(localName, portNumber);
	}

	/**
	 * Accessor to remove a mapping programatically.
	 */
	public void removeMapping(String localName) {
		this.m_elementPortMap.remove(localName);
	}

	/**
	 * Returns the mapping. Maybe make this read-only?
	 */
	public Map getMappings() {
		// return Collections.unmodifiableMap(m_elementPortMap); // return a
		// read-only map
		return this.m_elementPortMap;
	}

	/**
	 * Deserializes/constructs an XML Extract node from a given XML configuration.
	 */
	public static Node fromXML(org.w3c.dom.Node nodeXML) {
		NamedNodeMap attribs = nodeXML.getAttributes();
		XMLExtract extract = null;

		// We must have attributes id, type and sourceUri
		if (attribs != null) {
			// Retrieve the required attributes
			org.w3c.dom.Node idNode = attribs.getNamedItem("id");
			org.w3c.dom.Node sourceUriNode = attribs.getNamedItem("sourceUri");
			String id = null;
			String uriString = null;

			if (idNode != null) {
				id = idNode.getNodeValue();
			} else {
				// id is a required attribute... error out
				LOG.error("XML Extract: missing required attribute: id");
				return null;
			}

			if (sourceUriNode != null) {
				uriString = sourceUriNode.getNodeValue();
			} else {
				// source URI is another required attribute, so error out
				LOG.error("XML Extract: " + id + " missing requried attribute: sourceUri");
				return null;
			}

			if (uriString != null && id != null) {
				InputSource inputSource = new InputSource(uriString);
				extract = new XMLExtract(id);
				extract.setInputSource(inputSource);
			} else {
				LOG.error("XML Extract: " + id + " missing required attribute value id and/or sourceUri");
				return null;
			}
		} else {
			LOG.error("XML Extract:  Missing required parameters id and/or sourceUri");
			return null;
		}

		// All of this node's children should consists of mappings
		for (int i = 0; i < nodeXML.getChildNodes().getLength(); i++) {
			org.w3c.dom.Node mapNode = nodeXML.getChildNodes().item(i);
			NamedNodeMap mapAttribs = mapNode.getAttributes();
			if (mapAttribs != null && mapNode.getFirstChild() != null) {
				// There's attributes (at least the element attribute is
				// required)
				// and a child (at least the textNode representing the port is
				// required)
				org.w3c.dom.Node elementAttr = mapAttribs
						.getNamedItem("element");
				String element = elementAttr.getNodeValue();
				String portString = mapNode.getFirstChild().getNodeValue();
				Integer portNumber;
				try {
					portNumber = new Integer(portString);
				} catch (NumberFormatException ex) {
					LOG.warn("XML Extract: " + extract.getID() + " Mapping for element (" + element 
							+ ") is invalid.  The port (" + portString + ") is not a number.");
					continue; // other mappings might be valid so continue
				}

				if (element != null && portNumber != null) {
					// Store this <ElementName> to Port #, mapping
					extract.m_elementPortMap.put(element, portNumber);
				}
			}
		}

		return extract;
	}

	/**
	 * Parses the inputSource.  The SAXHandler defined in this class will handle
	 * the rest of the events.
	 * 
	 * Returns false if there was an exception encountered during processing.
	 */
	private boolean parseXML() {
		SAXParserFactory factory = SAXParserFactory.newInstance();
		SAXParser parser;

		try {
			parser = factory.newSAXParser();
		} catch (Exception ex) {
			resultMsg = ex.getMessage();
			resultCode = Node.RESULT_FATAL_ERROR;
			return false;
		}

		try {
			parser.parse(m_inputSource, new SAXHandler(getID()));
		} catch (SAXException ex) {
			if (!runIt) {
				return true; // we were stopped by a stop signal... probably
			}
			LOG.error("XML Extract: " + getID() + " Parse Exception", ex);
			// ?? LxNode.rollback();
			resultMsg = ex.getMessage();
			resultCode = Node.RESULT_ERROR;
			return false;
		} catch (Exception ex) {
			LOG.error("XML Extract: " + getID() + " Unexpected Exception", ex);
			// ?? LxNode.rollback();
			resultMsg = ex.getMessage();
			resultCode = Node.RESULT_FATAL_ERROR;
			return false;
		}
		return true;
	}

	public String getType() {
		return this.getClass().getName();
	}

	public boolean checkConfig() {
		return true;
	}

	public org.w3c.dom.Node toXML() {
		return null;
	}
}
