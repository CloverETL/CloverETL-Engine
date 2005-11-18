package org.jetel.component;

import java.lang.ref.WeakReference;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.ByteDataField;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DateDataField;
import org.jetel.data.IntegerDataField;
import org.jetel.data.LongDataField;
import org.jetel.data.NumericDataField;
import org.jetel.data.SequenceField;
import org.jetel.data.StringDataField;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ComponentXMLAttributes;
import org.w3c.dom.NodeList;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * <h3>XMLExtract Component</h3>
 *
 * <!-- Provides the logic to parse a xml file and filter to different ports based on
 * a matching element. The element and all children will be turned into a
 * Data record -->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>Concatenate</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Provides the logic to parse a xml file and filter to different ports based on
 * a matching element. The element and all children will be turned into a
 * Data record.</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>0</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>Output port[0] defined/connected. Depends on mapping definition.</td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"XML_EXTRACT"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>sourceUri</b></td><td>location of source XML data to process</td>
 *  </tr>
 *  </table>
 * 
 * Provides the logic to parse a xml file and filter to different ports based on
 * a matching element. The element and all children will be turned into a
 * Datarecord.<br>
 * For example: given an xml file:<br>
 * <code>
 * &lt;myXML&gt; <br>
 * &nbsp;&lt;phrase id="1111"&gt; <br> 
 * &nbsp;&nbsp;&lt;text&gt;hello&lt;/text&gt; <br> 
 * &nbsp;&nbsp;&lt;localization&gt; <br>
 * &nbsp;&nbsp;&nbsp;&lt;chinese&gt;how allo yee dew ying&lt;/chinese&gt; <br> 
 * &nbsp;&nbsp;&nbsp;&lt;german&gt;wie gehts&lt;/german&gt; <br>
 * &nbsp;&nbsp;&lt;/localization&gt; <br>
 * &nbsp;&lt;/phrase&gt; <br>
 * &nbsp;&lt;locations&gt; <br>
 * &nbsp;&nbsp;&lt;location&gt; <br>
 * &nbsp;&nbsp;&nbsp;&lt;name&gt;Stormwind&lt;/name&gt; <br>
 * &nbsp;&nbsp;&nbsp;&lt;description&gt;Beautiful European architecture with a scenic canal system.&lt;/description&gt; <br>
 * &nbsp;&nbsp;&lt;/location&gt; <br>
 * &nbsp;&nbsp;&lt;location&gt; <br> 
 * &nbsp;&nbsp;&nbsp;&lt;name&gt;Ironforge&lt;/name&gt; <br> 
 * &nbsp;&nbsp;&nbsp;&lt;description&gt;Economic capital of the region with a high population density.&lt;/description&gt; <br> 
 * &nbsp;&nbsp;&lt;/location&gt; <br>
 * &nbsp;&lt;/locations&gt; <br>
 * &nbsp;&lt;someUselessElement&gt;...&lt;/someUselessElement&gt; <br>
 * &nbsp;&lt;someOtherUselessElement/&gt; <br>
 * &nbsp;&lt;phrase id="2222"&gt; <br>
 * &nbsp;&nbsp;&lt;text&gt;bye&lt;/text&gt; <br>
 * &nbsp;&nbsp;&lt;localization&gt; <br>
 * &nbsp;&nbsp;&nbsp;&lt;chinese&gt;she yee lai ta&lt;/chinese&gt; <br> 
 * &nbsp;&nbsp;&nbsp;&lt;german&gt;aufweidersehen&lt;/german&gt; <br>
 * &nbsp;&nbsp;&lt;/localization&gt; <br>
 * &nbsp;&lt;/phrase&gt; <br>
 * &lt;/myXML&gt; <br>
 * </code> Suppose we want to pull out "phrase" as one datarecord,
 * "localization" as another datarecord, and "location" as the final datarecord
 * and ignore the useless elements. First we define the metadata for the
 * records. Then create the following mapping in the graph: <br>
 * <code>
 * &lt;node id="myId" type="com.lrn.etl.job.component.XMLExtract"&gt; <br>
 * &nbsp;&lt;mapping element="phrase" outPort="0"&gt;<br> 
 * &nbsp;&nbsp;&lt;mapping element="localization" outPort="1" parentKey="id" generatedKey="parent_id"/&gt;<br> 
 * &nbsp;&lt;/mapping&gt; <br>
 * &nbsp;&lt;mapping element="location" outPort="2"/&gt;<br> 
 * &lt;/node&gt;<br>
 * </code> Port 0 will get the DataRecords:<br>
 * 1) id=1111, text=hello<br>
 * 2) id=2222, text=bye<br>
 * Port 1 will get:<br>
 * 1) parent_id=1111, chinese=how allo yee dew ying, german=wie gehts<br>
 * 2) parent_id=2222, chinese=she yee lai ta, german=aufwiedersehen<br>
 * Port 2 will get:<br>
 * 1) name=Stormwind, description=Beautiful European architecture with a scenic
 * canal system.<br>
 * 2) name=Ironforge, description=Economic capital of the region with a high
 * population density.<br>
 * <hr>
 * Issue: Enclosing elements having values are not supported.<br>
 * i.e. <br>
 * <code>
 *   &lt;x&gt; <br>
 *     &lt;y&gt;z&lt;/y&gt;<br>
 *     xValue<br>
 *   &lt;/x&gt;<br>
 * </code> there will be no column x with value xValue.<br>
 * Issue: Namespaces are not considered.<br>
 * i.e. <br>
 * <code>
 *   &lt;ns1:x&gt;xValue&lt;/ns1:x&gt;<br>
 *   &lt;ns2:x&gt;xValue2&lt;/ns2:x&gt;<br>
 * </code> will be considered the same x.
 * 
 * @author KKou
 */
public class XMLExtract extends Node
{
    public final static String COMPONENT_TYPE = "XML_EXTRACT";
	// Logger
	private static final Log	LOG	= LogFactory.getLog(XMLExtract.class);

	/**
	 * SAX Handler that will dispatch the elements to the different ports.
	 */
	private class SAXHandler extends DefaultHandler
	{
		// depth of the element, used to determine when we hit the matching
		// close element
		private int				m_level			= 0;
		// flag set if we saw characters, otherwise don't save the column (used
		// to set null values)
		private boolean			m_hasCharacters	= false;
		// buffer for node value
		private StringBuffer	m_characters	= new StringBuffer();
		// the active mapping
		private Mapping			m_activeMapping	= null;

		public void startElement(String prefix, String namespace,
				String localName, Attributes attributes) throws SAXException
		{
			m_level++;
			Mapping mapping = null;
			if (m_activeMapping == null)
			{
				mapping = (Mapping) m_elementPortMap.get(localName);
			}
			else
			{
				mapping = (Mapping) m_activeMapping.getChildMapping(localName);
			}
			if (mapping != null)
			{
				// We have a match, start converting all child nodes into
				// the DataRecord structure
				m_activeMapping = mapping;
				m_activeMapping.setLevel(m_level);
				DataRecord record = mapping.getOutRecord();

				if (record == null)
				{
					// If it's null that means that there's no edge mapped to
					// the output port
					// remove this mapping so we don't repeat this logic (and
					// logging)
					LOG.warn("XML Extract: " + getID() + " Element ("
							+ localName
							+ ") does not have an edge mapped to that port.");
					m_activeMapping.getParent().removeChildMapping(
							m_activeMapping);
					return;
				}
			}

			if (m_activeMapping != null)
			{
				// In a matched element (i.e. we are creating a DataRecord)
				// Store all attributes as columns (this hasn't been
				// used/tested)
				for (int i = 0; i < attributes.getLength(); i++)
				{
					DataField field = m_activeMapping.getOutRecord().getField(
							attributes.getQName(i));
					if (field != null)
					{
						field.setNull(false); // Bug: Need to set null to
						// false when there's valid data
						field.fromString(attributes.getValue(i));
					}
				}
			}
		}

		public void characters(char[] data, int offset, int length)
				throws SAXException
		{
			// Save the characters into the buffer, endElement will store it
			// into the field
			if (m_activeMapping != null)
			{
				m_characters.append(data, offset, length);
				m_hasCharacters = true;
			}
		}

		public void endElement(String prefix, String namespace, String localName)
				throws SAXException
		{
			if (m_activeMapping != null)
			{
				// Store the characters processed by the characters() call back
				DataField field = m_activeMapping.getOutRecord().getField(
						localName);
				if (field != null)
				{
					// If field is nullable and there's no character data set it
					// to null
					if (!m_hasCharacters && field.getMetadata().isNullable())
					{
						field.setNull(true); // it's null
					}
					else
					{
						field.setNull(false); // it's not null
						try
						{
							field.fromString(m_characters.toString().trim());
						}
						catch (BadDataFormatException ex)
						{
							// This is a bit hacky here SOOO let me explain...
							if (field.getType() == 'D')
							{
								// XML dateTime format is not supported by the
								// DateFormat oject that clover uses...
								// so timezones are unparsable
								// i.e. XML wants -5:00 but DateFormat wants
								// -500
								// Attempt to munge and retry... (there has to
								// be a better way)
								try
								{
									// Chop off the ":" in the timezone (it HAS
									// to be at the end)
									String dateTime = m_characters.substring(0,
											m_characters.lastIndexOf(":"))
											+ m_characters
													.substring(m_characters
															.lastIndexOf(":") + 1);
									DateFormat format = new SimpleDateFormat(
											field.getMetadata().getFormatStr());
									field.setValue(format
											.parse(dateTime.trim()));
								}
								catch (Exception ex2)
								{
									// Oh well we tried, throw the originating
									// exception
									throw new SAXException(
											"Error setting field: " + localName
													+ " data: "
													+ m_characters.toString()
													+ " " + ex.getMessage(), ex);
								}
							}
							else
							{
								throw ex;
							}
						}
					}
				}

				// Regardless of whether this was saved, reset the length of the
				// buffer and flag
				m_characters.setLength(0);
				m_hasCharacters = false;
			}

			if (m_activeMapping != null
					&& m_level == m_activeMapping.getLevel())
			{
				// This is the closing element of the matched element that
				// triggered the processing
				// That should be the end of this record so send it off to the
				// next Node
				if (runIt)
				{
					try
					{
						OutputPort outPort = getOutputPort(m_activeMapping
								.getOutPort());
						DataRecord outRecord = m_activeMapping.getOutRecord();
						if (m_activeMapping.getParentKey() != null)
						{
							DataField generatedField = outRecord
									.getField(m_activeMapping.getGeneratedKey());
							DataField parentKeyField = m_activeMapping
									.getParent().getOutRecord().getField(
											m_activeMapping.getParentKey());
							if (generatedField == null)
							{
								LOG
										.warn(getID()
												+ ": XML Extract Mapping's generatedKey field was not found. "
												+ m_activeMapping
														.getGeneratedKey());
								m_activeMapping.setGeneratedKey(null);
								m_activeMapping.setParentKey(null);
							}
							else if (parentKeyField == null)
							{
								LOG
										.warn(getID()
												+ ": XML Extract Mapping's parentKey field was not found. "
												+ m_activeMapping
														.getParentKey());
								m_activeMapping.setGeneratedKey(null);
								m_activeMapping.setParentKey(null);
							}
							else
							{
								generatedField.setValue(parentKeyField
										.getValue());
							}
						}
						outPort.writeRecord(outRecord);
						// reset record
						resetRecord(outRecord);

						m_activeMapping = m_activeMapping.getParent();
					}
					catch (Exception ex)
					{
						throw new SAXException(ex);
					}
				}
				else
				{
					throw new SAXException("Stop Signaled");
				}
			}
			m_level--; // ended an element so decrease our depth
		}
	}

	/**
	 * Mapping holds a single mapping.
	 */
	public class Mapping
	{
		String			m_element;
		int				m_outPort;
		DataRecord		m_outRecord;
		String			m_parentKey;
		String			m_generatedKey;
		Map				m_childMap;
		WeakReference	m_parent;
		int				m_level;

		/*
		 * Minimally required information.
		 */
		public Mapping(String element, int outPort)
		{
			m_element = element;
			m_outPort = outPort;
		}

		/**
		 * Gives the optional attributes parentKey and generatedKey.
		 */
		public Mapping(String element, int outPort, String parentKey,
				String generatedKey)
		{
			this(element, outPort);

			m_parentKey = parentKey;
			m_generatedKey = generatedKey;
		}

		public int getLevel()
		{
			return m_level;
		}

		public void setLevel(int level)
		{
			m_level = level;
		}

		public Map getChildMap()
		{
			return m_childMap;
		}

		public Mapping getChildMapping(String element)
		{
			if (m_childMap == null)
			{
				return null;
			}
			return (Mapping) m_childMap.get(element);
		}

		public void addChildMapping(Mapping mapping)
		{
			if (m_childMap == null)
			{
				m_childMap = new HashMap();
			}
			m_childMap.put(mapping.getElement(), mapping);
		}

		public void removeChildMapping(Mapping mapping)
		{
			if (m_childMap == null)
			{
				return;
			}
			m_childMap.remove(mapping.getElement());
		}

		public String getElement()
		{
			return m_element;
		}

		public void setElement(String element)
		{
			m_element = element;
		}

		public String getGeneratedKey()
		{
			return m_generatedKey;
		}

		public void setGeneratedKey(String generatedKey)
		{
			m_generatedKey = generatedKey;
		}

		public int getOutPort()
		{
			return m_outPort;
		}

		public void setOutPort(int outPort)
		{
			m_outPort = outPort;
		}

		public DataRecord getOutRecord()
		{
			if (m_outRecord == null)
			{
				OutputPort outPort = getOutputPort(getOutPort());
				if (outPort != null)
				{
					m_outRecord = new DataRecord(outPort.getMetadata());
					m_outRecord.init();
					resetRecord(m_outRecord);
				}
				else
				{
					m_outRecord = new DataRecord(new DataRecordMetadata(
							"nonexistant"));
					LOG
							.warn(getID()
									+ ": Port "
									+ getOutPort()
									+ " does not have an edge connected.  Please connect the edge or remove the mapping.");
				}
			}
			return m_outRecord;
		}

		public void setOutRecord(DataRecord outRecord)
		{
			m_outRecord = outRecord;
		}

		public String getParentKey()
		{
			return m_parentKey;
		}

		public void setParentKey(String parentKey)
		{
			m_parentKey = parentKey;
		}

		public Mapping getParent()
		{
			if (m_parent != null)
			{
				return (Mapping) m_parent.get();
			}
			else
			{
				return null;
			}
		}

		public void setParent(Mapping parent)
		{
			m_parent = new WeakReference(parent);
		}
	}

	// Map of elementName => output port
	private Map			m_elementPortMap	= new HashMap();

	// Where the XML comes from
	private InputSource	m_inputSource;

	/**
	 * Constructs an XML Extract node with the given id.
	 */
	public XMLExtract(String id)
	{
		super(id);
	}

	// //////////////////////////////////////////////////////////////////////////
	// De-Serialization
	//
	public static Node fromXML(org.w3c.dom.Node nodeXML)
	{
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML);
		XMLExtract extract;

		if (xattribs.exists("id"))
		{
			try
			{
				// Go through this round about method so that sub classes may
				// re-use this logic like:
				// fromXML(nodeXML) {
				// MyXMLExtract = (MyXMLExtract) XMLExtract.fromXML(nodeXML);
				// Do more stuff with MyXMLExtract
				// return MyXMLExtract
				// }
				extract = new XMLExtract(xattribs.getString("id"));

				if (xattribs.exists("sourceUri"))
				{
					extract.setInputSource(new InputSource(xattribs
							.getString("sourceUri")));
				}
				// Process the mappings
				NodeList nodes = nodeXML.getChildNodes();
				for (int i = 0; i < nodes.getLength(); i++)
				{
					org.w3c.dom.Node node = nodes.item(i);
					processMappings(extract, null, node);
				}

				return extract;
			}
			catch (Exception ex)
			{
				System.err.println(COMPONENT_TYPE + ":" + ((xattribs.exists(XML_ID_ATTRIBUTE)) ? xattribs.getString(Node.XML_ID_ATTRIBUTE) : " unknown ID ") + ":" + ex.getMessage());
			}
		}
		return null;
	}

	private static void processMappings(XMLExtract extract,
			Mapping parentMapping, org.w3c.dom.Node nodeXML)
	{
		if ("Mapping".equals(nodeXML.getLocalName()))
		{
			// for a mapping declaration, process all of the attributes
			// element, outPort, parentKeyName, generatedKey
			ComponentXMLAttributes attributes = new ComponentXMLAttributes(
					nodeXML);
			Mapping mapping = null;

			if (attributes.exists("element") && attributes.exists("outPort"))
			{
				mapping = extract.new Mapping(attributes.getString("element"),
						attributes.getInteger("outPort"));
			}
			else
			{
				if (attributes.exists("outPort"))
				{
					LOG
							.warn(extract.getID()
									+ ": XML Extract Mapping for element: "
									+ mapping.getElement()
									+ " missing a required attribute, element for outPort "
									+ attributes.getString("outPort")
									+ ".  Skipping this mapping and all children.");
				}
				else if (attributes.exists("element"))
				{
					LOG
							.warn(extract.getID()
									+ ": XML Extract Mapping for element: "
									+ mapping.getElement()
									+ " missing a required attribute, outPort for element "
									+ attributes.getString("element")
									+ ".  Skipping this mapping and all children.");
				}
				else
				{
					LOG
							.warn(extract.getID()
									+ ": XML Extract Mapping for element: "
									+ mapping.getElement()
									+ " missing required attributes, element and outPort.  Skipping this mapping and all children.");
				}
				return;
			}

			boolean parentKeyPresent = false;
			boolean generatedKeyPresent = false;
			if (attributes.exists("parentKey"))
			{
				mapping.setParentKey(attributes.getString("parentKey"));
				parentKeyPresent = true;
			}

			if (attributes.exists("generatedKey"))
			{
				mapping.setGeneratedKey(attributes.getString("generatedKey"));
				generatedKeyPresent = true;
			}

			if (parentKeyPresent != generatedKeyPresent)
			{
				LOG
						.warn(extract.getID()
								+ ": XML Extract Mapping for element: "
								+ mapping.getElement()
								+ " must either have both parentKey and generatedKey attributes or neither.");
				mapping.setParentKey(null);
				mapping.setGeneratedKey(null);
			}

			// Add this mapping to the parent
			if (parentMapping != null)
			{
				parentMapping.addChildMapping(mapping);
				mapping.setParent(parentMapping);
			}
			else
			{
				extract.addMapping(mapping);
			}

			if (parentKeyPresent && mapping.getParent() == null)
			{
				LOG
						.warn(extract.getID()
								+ ": XML Extact Mapping for element: "
								+ mapping.getElement()
								+ " may only have parentKey or generatedKey attributes if it is a nested mapping");
				mapping.setParentKey(null);
				mapping.setGeneratedKey(null);
			}

			// Process all nested mappings
			NodeList nodes = nodeXML.getChildNodes();
			for (int i = 0; i < nodes.getLength(); i++)
			{
				org.w3c.dom.Node node = nodes.item(i);
				processMappings(extract, mapping, node);
			}
		}
		else if (nodeXML.getNodeType() == org.w3c.dom.Node.TEXT_NODE)
		{
			// Ignore text values inside nodes
		}
		else
		{
			LOG.warn(extract.getID() + ": Unknown element: "
					+ nodeXML.getLocalName()
					+ " ignoring it and all child elements.");
		}
	}

	// //////////////////////////////////////////////////////////
	// Execution Section
	//
	/**
	 * Call back from the Clover Engine starting this node.
	 */
	public void run()
	{
		// Parse the XML file
		if (parseXML())
		{
			// We have successfully sent out all the data, send out the EOF
			// signal
			broadcastEOF();

			// determine if we successfully sent out everything or we
			// successfully stopped due to a stop signal
			if (runIt)
				resultMsg = "OK";
			else
				resultMsg = "STOPPED";
			resultCode = Node.RESULT_OK;
		}
		else
		{
			// If it's false, the result message and codes have been set in the
			// parseXML Method.
			broadcastEOF();
			return;
		}
	}

	/**
	 * Parses the inputSource. The SAXHandler defined in this class will handle
	 * the rest of the events. Returns false if there was an exception
	 * encountered during processing.
	 */
	private boolean parseXML()
	{
		SAXParserFactory factory = SAXParserFactory.newInstance();
		SAXParser parser;

		try
		{
			parser = factory.newSAXParser();
		}
		catch (Exception ex)
		{
			resultMsg = ex.getMessage();
			resultCode = Node.RESULT_FATAL_ERROR;
			return false;
		}

		try
		{
			parser.parse(m_inputSource, new SAXHandler());
		}
		catch (SAXException ex)
		{
			if (!runIt)
			{
				return true; // we were stopped by a stop signal... probably
			}
			LOG.error("XML Extract: " + getID() + " Parse Exception", ex);
			resultMsg = ex.getMessage();
			resultCode = Node.RESULT_ERROR;
			return false;
		}
		catch (Exception ex)
		{
			LOG.error("XML Extract: " + getID() + " Unexpected Exception", ex);
			resultMsg = ex.getMessage();
			resultCode = Node.RESULT_FATAL_ERROR;
			return false;
		}
		return true;
	}

	// //////////////////////////////////////////////////////////////////
	// Clover Call Back Methods
	//
	/**
	 * Perform sanity checks.
	 */
	public void init() throws ComponentNotReadyException
	{
		// test that we have at least one input port and one output
		if (outPorts.size() < 1)
		{
			throw new ComponentNotReadyException(getID()
					+ ": At least one output port has to be defined!");
		}

		if (m_elementPortMap.size() < 1)
		{
			throw new ComponentNotReadyException(
					getID()
							+ ": At least one mapping has to be defined.  <Mapping element=\"elementToMatch\" outPort=\"123\" [parentKey=\"key in parent\" generatedKey=\"new foreign key in target\"]/>");
		}
	}

	public String getType()
	{
		return COMPONENT_TYPE;
	}

	public boolean checkConfig()
	{
		return true;
	}

	public org.w3c.dom.Node toXML()
	{
		return null;
	}

	// //////////////////////////////////////////////////////////////////
	// Accessors
	//
	/**
	 * Set the input source containing the XML this will parse.
	 */
	public void setInputSource(InputSource inputSource)
	{
		m_inputSource = inputSource;
	}

	/**
	 * Accessor to add a mapping programatically.
	 */
	public void addMapping(Mapping mapping)
	{
		m_elementPortMap.put(mapping.getElement(), mapping);
	}

	/**
	 * Returns the mapping. Maybe make this read-only?
	 */
	public Map getMappings()
	{
		// return Collections.unmodifiableMap(m_elementPortMap); // return a
		// read-only map
		return m_elementPortMap;
	}

	private void resetRecord(DataRecord record)
	{
		// reset the record setting the nullable fields to null and default
		// values. Unfortunately init() does not do this, so if you have a field
		// that's nullable and you never set a value to it, it will NOT be null.

		// the reason we need to reset data records is the fact that XML data is
		// not as rigidly
		// structured as csv fields, so column values are regularly "missing"
		// and without a reset
		// the prior row's value will be present.
		for (int i = 0; i < record.getNumFields(); i++)
		{
			DataFieldMetadata fieldMetadata = record.getMetadata().getField(i);
			DataField field = record.getField(i);
			if (fieldMetadata.isNullable())
			{
				// Default all nullables to null
				field.setNull(true);
			}
			else
			{
				// Not nullable so set it to the default value (what init does)
				switch (fieldMetadata.getType())
				{
				case DataFieldMetadata.INTEGER_FIELD:
					((IntegerDataField) field).setValue(0);
					break;

				case DataFieldMetadata.STRING_FIELD:
					((StringDataField) field).setValue("");
					break;

				case DataFieldMetadata.DATE_FIELD:
				case DataFieldMetadata.DATETIME_FIELD:
					((DateDataField) field).setValue(0);
					break;

				case DataFieldMetadata.NUMERIC_FIELD:
					((NumericDataField) field).setValue(0);
					break;

				case DataFieldMetadata.LONG_FIELD:
					((LongDataField) field).setValue(0);
					break;

				case DataFieldMetadata.DECIMAL_FIELD:
					((NumericDataField) field).setValue(0);
					break;

				case DataFieldMetadata.BYTE_FIELD:
					((ByteDataField) field).setValue((byte) 0);
					break;

				case DataFieldMetadata.SEQUENCE_FIELD:
					((SequenceField) field).setValue(0);
					break;

				case DataFieldMetadata.UNKNOWN_FIELD:
				default:
					break;
				}
			}

			if (fieldMetadata.getDefaultValue() != null)
			{
				// Default all default values to their given defaults
				field.fromString(fieldMetadata.getDefaultValue());
			}
		}
	}
}
