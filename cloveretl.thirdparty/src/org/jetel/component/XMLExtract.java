package org.jetel.component;

import java.io.StringReader;
import java.lang.ref.WeakReference;
import java.nio.channels.Channels;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.StringDataField;
import org.jetel.data.sequence.Sequence;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.sequence.PrimitiveSequence;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
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
 * <td>XMLExtract</td></tr>
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
 *  <tr><td><b>useNestedNodes</b></td><td><b>true</b> if nested unmapped XML elements will be used as data source; <b>false</b> if will be ignored</td>
 *  <tr><td><b>mapping</b></td><td>&lt;mapping&gt;</td>
 *  </tr>
 *  </table>
 *
 * Provides the logic to parse a xml file and filter to different ports based on
 * a matching element. The element and all children will be turned into a
 * Data record.<br>
 * Mapping attribute contains mapping hierarchy in XML form. DTD of mapping:<br>
 * <code>
 * &lt;!ELEMENT Mappings (Mapping*)&gt;<br>
 * 
 * &lt;!ELEMENT Mapping (Mapping*)&gt;<br>
 * &lt;!ATTLIST Mapping<br>
 * &nbsp;element NMTOKEN #REQUIRED<br>      
 * &nbsp;&nbsp;//name of binded XML element<br>  
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
 * &nbsp;xmlFields NMTOKEN #IMPLIED<br>     
 * &nbsp;&nbsp;//comma separeted xml element names, which will be mapped on appropriate record fields<br>
 * &nbsp;&nbsp;//defined in cloverFields attribute<br>
 * &nbsp;cloverFields NMTOKEN #IMPLIED<br>  
 * &nbsp;&nbsp;//see xmlFields comment<br>
 * &gt;<br>
 * </code>
 * All nested XML elements will be recognized as record fields and mapped by name
 * (except elements serviced by other nested Mapping elements), if you prefere other mapping
 * xml fields and clover fields than 'by name', use xmlFields and cloveFields attributes
 * to setup custom fields mapping. 'useNestedNodes' component attribute defines
 * if also child of nested xml elements will be mapped on the current clover record.
 * Record from nested Mapping element could be connected via key fields with parent record produced
 * by parent Mapping element (see parentKey and generatedKey attribute notes).
 * In case that fields are unsuitable for key composing, extractor could fill
 * one or more fields with values comming from sequence (see sequenceField and sequenceId attribute). 
 * 
 * For example: given an xml file:<br>
 * <code>
 * &lt;myXML&gt; <br>
 * &nbsp;&lt;phrase&gt; <br>
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
 * &nbsp;&lt;phrase&gt; <br>
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
 * &nbsp;&lt;attr name="mapping"&gt;<br>
 * &nbsp;&nbsp;&lt;Mapping element="phrase" outPort="0" sequenceField="id"&gt;<br>
 * &nbsp;&nbsp;&nbsp;&lt;Mapping element="localization" outPort="1" parentKey="id" generatedKey="parent_id"/&gt;<br>
 * &nbsp;&nbsp;&lt;/Mapping&gt; <br>
 * &nbsp;&nbsp;&lt;Mapping element="location" outPort="2"/&gt;<br>
 * &nbsp;&lt;/attr&gt;<br>
 * &lt;/node&gt;<br>
 * </code> Port 0 will get the DataRecords:<br>
 * 1) id=1, text=hello<br>
 * 2) id=2, text=bye<br>
 * Port 1 will get:<br>
 * 1) parent_id=1, chinese=how allo yee dew ying, german=wie gehts<br>
 * 2) parent_id=2, chinese=she yee lai ta, german=aufwiedersehen<br>
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
public class XMLExtract extends Node {

    // Logger
    private static final Log    LOG = LogFactory.getLog(XMLExtract.class);

    private static final String XML_SOURCEURI_ATTRIBUTE = "sourceUri";
    private static final String XML_USENESTEDNODES_ATTRIBUTE = "useNestedNodes";
    private static final String XML_MAPPING_ATTRIBUTE = "mapping";

    private static final String XML_MAPPING = "Mapping";
    private static final String XML_ELEMENT = "element";
    private static final String XML_OUTPORT = "outPort";
    private static final String XML_PARENTKEY = "parentKey";
    private static final String XML_GENERATEDKEY = "generatedKey";
    private static final String XML_XMLFIELDS = "xmlFields";
    private static final String XML_CLOVERFIELDS = "cloverFields";
    private static final String XML_SEQUENCEFIELD = "sequenceField";
    private static final String XML_SEQUENCEID = "sequenceId";
    private static final String XML_SKIP_ROWS_ATTRIBUTE = "skipRows";
    private static final String XML_NUMRECORDS_ATTRIBUTE = "numRecords";

    public final static String COMPONENT_TYPE = "XML_EXTRACT";

    // Map of elementName => output port
    private Map<String, Mapping> m_elementPortMap = new HashMap<String, Mapping>();
    
    // Where the XML comes from
    private InputSource m_inputSource;
    
    private boolean useNestedNodes = true;
    
    private int skipRows=0; // do not skip rows by default
    private int numRecords = -1;

    /**
     * SAX Handler that will dispatch the elements to the different ports.
     */
    private class SAXHandler extends DefaultHandler {
        
        // depth of the element, used to determine when we hit the matching
        // close element
        private int m_level = 0;
        
        // flag set if we saw characters, otherwise don't save the column (used
        // to set null values)
        private boolean m_hasCharacters = false;
        
        // buffer for node value
        private StringBuffer m_characters = new StringBuffer();
        
        // the active mapping
        private Mapping m_activeMapping = null;
        
        private int globalCounter;
        
        /**
         * @see org.xml.sax.ContentHandler#startElement(java.lang.String, java.lang.String, java.lang.String, org.xml.sax.Attributes)
         */
        public void startElement(String prefix, String namespace, String localName, Attributes attributes) throws SAXException {
            m_level++;
            
            Mapping mapping = null;
            if (m_activeMapping == null) {
                mapping = (Mapping) m_elementPortMap.get(localName);
            } else {
                mapping = (Mapping) m_activeMapping.getChildMapping(localName);
            }
            if (mapping != null) {
                // We have a match, start converting all child nodes into
                // the DataRecord structure
                m_activeMapping = mapping;
                m_activeMapping.setLevel(m_level);
                
                if (mapping.getOutRecord() == null) {
                    // If it's null that means that there's no edge mapped to
                    // the output port
                    // remove this mapping so we don't repeat this logic (and
                    // logging)
                    LOG.warn("XML Extract: " + getId() + " Element ("
                            + localName
                            + ") does not have an edge mapped to that port.");
                    if(m_activeMapping.getParent() != null) {
                        m_activeMapping.getParent().removeChildMapping(m_activeMapping);
                        m_activeMapping = m_activeMapping.getParent();
                    } else {
                        m_elementPortMap.remove(m_activeMapping);
                        m_activeMapping = null;
                    }
                    
                    return;
                }

                //sequence fields initialization
                String sequenceFieldName = m_activeMapping.getSequenceField();
                if(sequenceFieldName != null && m_activeMapping.getOutRecord().hasField(sequenceFieldName)) {
                    Sequence sequence = m_activeMapping.getSequence();
                    DataField sequenceField = m_activeMapping.getOutRecord().getField(sequenceFieldName);
                    if(sequenceField.getType() == DataFieldMetadata.INTEGER_FIELD) {
                        sequenceField.setValue(sequence.nextValueInt());
                    } else if(sequenceField.getType() == DataFieldMetadata.LONG_FIELD
                            || sequenceField.getType() == DataFieldMetadata.DECIMAL_FIELD
                            || sequenceField.getType() == DataFieldMetadata.NUMERIC_FIELD) {
                        sequenceField.setValue(sequence.nextValueLong());
                    } else {
                        sequenceField.fromString(sequence.nextValueString());
                    }
                }
            }
            
            if(m_activeMapping != null //used only if we right now recognize new mapping element or if we want to use nested unmapped nodes as a source of data
                    && (useNestedNodes || mapping != null)) {
                // In a matched element (i.e. we are creating a DataRecord)
                // Store all attributes as columns (this hasn't been
                // used/tested)                
                for (int i = 0; i < attributes.getLength(); i++) {
                    String attrName = attributes.getQName(i);
                    
                    //use fields mapping
                    Map<String, String> xmlCloverMap = m_activeMapping.getXml2CloverFieldsMap();
                    if(xmlCloverMap != null && xmlCloverMap.containsKey(attrName)) {
                       attrName = xmlCloverMap.get(attrName);
                    }
                    
                    if (m_activeMapping.getOutRecord().hasField(attrName)) {
                        m_activeMapping.getOutRecord().getField(attrName).fromString(attributes.getValue(i));
                    }
                }
            }
        }
        
        /**
         * @see org.xml.sax.ContentHandler#characters(char[], int, int)
         */
        public void characters(char[] data, int offset, int length) throws SAXException {
            // Save the characters into the buffer, endElement will store it
            // into the field
            if (m_activeMapping != null) {
                m_characters.append(data, offset, length);
                m_hasCharacters = true;
            }
        }
        
        /**
         * @see org.xml.sax.ContentHandler#endElement(java.lang.String, java.lang.String, java.lang.String)
         */
        public void endElement(String prefix, String namespace, String localName) throws SAXException {
            if (m_activeMapping != null) {
                //use fields mapping
                Map<String, String> xml2clover = m_activeMapping.getXml2CloverFieldsMap();
                if(xml2clover != null && xml2clover.containsKey(localName)) {
                    localName = xml2clover.get(localName);
                }
                // Store the characters processed by the characters() call back
                //only if we have corresponding output field and we are on the right level or we want to use data from nested unmapped nodes
                if (m_activeMapping.getOutRecord().hasField(localName) 
                        && (useNestedNodes || m_level - 1 <= m_activeMapping.getLevel())) {
                    DataField field = m_activeMapping.getOutRecord().getField(localName);
                    // If field is nullable and there's no character data set it
                    // to null
                    if (!m_hasCharacters) {
                        field.setNull(true);
                    } else {
                        try {
                            field.fromString(m_characters.toString().trim());
                        } catch (BadDataFormatException ex) {
                            // This is a bit hacky here SOOO let me explain...
                            if (field.getType() == DataFieldMetadata.DATE_FIELD) {
                                // XML dateTime format is not supported by the
                                // DateFormat oject that clover uses...
                                // so timezones are unparsable
                                // i.e. XML wants -5:00 but DateFormat wants
                                // -500
                                // Attempt to munge and retry... (there has to
                                // be a better way)
                                try {
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
                                } catch (Exception ex2) {
                                    // Oh well we tried, throw the originating
                                    // exception
                                    throw ex;
                                }
                            } else {
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
            
            if (m_activeMapping != null && m_level == m_activeMapping.getLevel()) {
                // This is the closing element of the matched element that
                // triggered the processing
                // That should be the end of this record so send it off to the
                // next Node
                if (runIt) {
                    try {
                        OutputPort outPort = getOutputPort(m_activeMapping.getOutPort());
                        DataRecord outRecord = m_activeMapping.getOutRecord();
                        String[] generatedKey = m_activeMapping.getGeneratedKey();
                        String[] parentKey = m_activeMapping.getParentKey();
                        if (parentKey != null) {
                            //if generatedKey is a single array, all parent keys are concatened into generatedKey field
                            //I know it is ugly code...
                            if(generatedKey.length != parentKey.length && generatedKey.length != 1) {
                                LOG
                                        .warn(getId()
                                        + ": XML Extract Mapping's generatedKey and parentKey attribute has different number of field.");
                                m_activeMapping.setGeneratedKey(null);
                                m_activeMapping.setParentKey(null);
                            } else {
                                for(int i = 0; i < parentKey.length; i++) {
                                    boolean existGeneratedKeyField = generatedKey.length == 1 ? outRecord.hasField(generatedKey[0]) : outRecord.hasField(generatedKey[i]);
                                    boolean existParentKeyField = m_activeMapping.getParent().getOutRecord().hasField(parentKey[i]);
                                    if (!existGeneratedKeyField) {
                                        LOG
                                                .warn(getId()
                                                + ": XML Extract Mapping's generatedKey field was not found. "
                                                + (generatedKey.length == 1 ? generatedKey[0] : generatedKey[i]));
                                        m_activeMapping.setGeneratedKey(null);
                                        m_activeMapping.setParentKey(null);
                                    } else if (!existParentKeyField) {
                                        LOG
                                                .warn(getId()
                                                + ": XML Extract Mapping's parentKey field was not found. "
                                                + parentKey[i]);
                                        m_activeMapping.setGeneratedKey(null);
                                        m_activeMapping.setParentKey(null);
                                    } else {
                                        DataField generatedKeyField = generatedKey.length == 1 ? outRecord.getField(generatedKey[0]) : outRecord.getField(generatedKey[i]);
                                        DataField parentKeyField = m_activeMapping.getParent().getOutRecord().getField(parentKey[i]);
                                        if(generatedKey.length != parentKey.length) {
                                            if(generatedKeyField.getType() != DataFieldMetadata.STRING_FIELD) {
                                                LOG
                                                        .warn(getId()
                                                        + ": XML Extract Mapping's generatedKey field has to be String type (keys are concatened to this field).");
                                                m_activeMapping.setGeneratedKey(null);
                                                m_activeMapping.setParentKey(null);
                                            } else {
                                                ((StringDataField) generatedKeyField).append(parentKeyField.toString());
                                            }
                                        } else {
                                            generatedKeyField.setValue(parentKeyField.getValue());
                                        }
                                    }
                                }
                            }
                        }
                        
                    	if (skipRows > 0) {
                        	skipRows--;
                    	} else {
                            //check for index of last returned record
                            if(!(numRecords >= 0 && numRecords == globalCounter)) {
                                //send off record
                                outPort.writeRecord(outRecord);
                                globalCounter++;
                            }
                    	}
                    	
                        // reset record
                        outRecord.reset();
                        
                        m_activeMapping = m_activeMapping.getParent();
                    } catch (Exception ex) {
                        throw new SAXException(ex);
                    }
                } else {
                    throw new SAXException("Stop Signaled");
                }
            }
            
            //ended an element so decrease our depth
            m_level--; 
        }
    }
    
    /**
     * Mapping holds a single mapping.
     */
    public class Mapping {
        String m_element;
        int m_outPort;
        DataRecord m_outRecord;
        String[] m_parentKey;
        String[] m_generatedKey;
        Map<String, Mapping> m_childMap;
        WeakReference<Mapping> m_parent;
        int m_level;
        String m_sequenceField;
        String m_sequenceId;
        Sequence sequence;
        
        Map<String, String> xml2CloverFieldsMap = new HashMap<String, String>();
        
                /*
                 * Minimally required information.
                 */
        public Mapping(String element, int outPort) {
            m_element = element;
            m_outPort = outPort;
        }
        
        /**
         * Gives the optional attributes parentKey and generatedKey.
         */
        public Mapping(String element, int outPort, String parentKey[],
                String[] generatedKey) {
            this(element, outPort);
            
            m_parentKey = parentKey;
            m_generatedKey = generatedKey;
        }
        
        public int getLevel() {
            return m_level;
        }
        
        public void setLevel(int level) {
            m_level = level;
        }
        
        public Map<String, Mapping> getChildMap() {
            return m_childMap;
        }
        
        public Mapping getChildMapping(String element) {
            if (m_childMap == null) {
                return null;
            }
            return m_childMap.get(element);
        }
        
        public void addChildMapping(Mapping mapping) {
            if (m_childMap == null) {
                m_childMap = new HashMap<String, Mapping>();
            }
            m_childMap.put(mapping.getElement(), mapping);
        }
        
        public void removeChildMapping(Mapping mapping) {
            if (m_childMap == null) {
                return;
            }
            m_childMap.remove(mapping.getElement());
        }
        
        public String getElement() {
            return m_element;
        }
        
        public void setElement(String element) {
            m_element = element;
        }
        
        public String[] getGeneratedKey() {
            return m_generatedKey;
        }
        
        public void setGeneratedKey(String[] generatedKey) {
            m_generatedKey = generatedKey;
        }
        
        public int getOutPort() {
            return m_outPort;
        }
        
        public void setOutPort(int outPort) {
            m_outPort = outPort;
        }
        
        public Map<String, String> getXml2CloverFieldsMap() {
            return xml2CloverFieldsMap;
        }
        
        public void setXml2CloverFieldsMap(Map<String, String> xml2CloverFieldsMap) {
            this.xml2CloverFieldsMap = xml2CloverFieldsMap;
        }
        
        public DataRecord getOutRecord() {
            if (m_outRecord == null) {
                OutputPort outPort = getOutputPort(getOutPort());
                if (outPort != null) {
                    m_outRecord = new DataRecord(outPort.getMetadata());
                    m_outRecord.init();
                    m_outRecord.reset();
                } else {
                    LOG
                            .warn(getId()
                            + ": Port "
                            + getOutPort()
                            + " does not have an edge connected.  Please connect the edge or remove the mapping.");
                }
            }
            return m_outRecord;
        }
        
        public void setOutRecord(DataRecord outRecord) {
            m_outRecord = outRecord;
        }
        
        public String[] getParentKey() {
            return m_parentKey;
        }
        
        public void setParentKey(String[] parentKey) {
            m_parentKey = parentKey;
        }
        
        public Mapping getParent() {
            if (m_parent != null) {
                return m_parent.get();
            } else {
                return null;
            }
        }
        
        public void setParent(Mapping parent) {
            m_parent = new WeakReference<Mapping>(parent);
        }

        public String getSequenceField() {
            return m_sequenceField;
        }

        public void setSequenceField(String field) {
            m_sequenceField = field;
        }

        public String getSequenceId() {
            return m_sequenceId;
        }

        public void setSequenceId(String id) {
            m_sequenceId = id;
        }
        
        public Sequence getSequence() {
            if(sequence == null) {
                if(getSequenceId() == null) {
                    sequence = new PrimitiveSequence(getElement(), getGraph(), getElement());
                } else {
                    sequence = getGraph().getSequence(getSequenceId());
                    if(sequence == null) {
                        LOG
                                .warn(getId()
                                + ": Sequence "
                                + getSequenceId()
                                + " does not exist in transformation graph. Primitive sequence is used instead.");
                        sequence = new PrimitiveSequence(getElement(), getGraph(), getElement());
                    }
                }
            }
            return sequence;
        }
    }
    
    /**
     * Constructs an XML Extract node with the given id.
     */
    public XMLExtract(String id) {
        super(id);
    }
    
    // //////////////////////////////////////////////////////////////////////////
    // De-Serialization
    //
    public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
        ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
        XMLExtract extract;
        
        try {
            // Go through this round about method so that sub classes may
            // re-use this logic like:
            // fromXML(nodeXML) {
            //  MyXMLExtract = (MyXMLExtract) XMLExtract.fromXML(nodeXML);
            //  //Do more stuff with MyXMLExtract
            //  return MyXMLExtract
            // }
            extract = new XMLExtract(xattribs.getString(XML_ID_ATTRIBUTE));
            
            extract.setInputSource(
            		new InputSource(
            				Channels.newInputStream(
            						FileUtils.getReadableChannel(null, xattribs.getString(XML_SOURCEURI_ATTRIBUTE)))
            				));
            
            if(xattribs.exists(XML_USENESTEDNODES_ATTRIBUTE)) {
                extract.setUseNestedNodes(xattribs.getBoolean(XML_USENESTEDNODES_ATTRIBUTE));
            }
            
            // Process the mappings
            NodeList nodes;
            if(xattribs.exists(XML_MAPPING_ATTRIBUTE)) {
                //read mapping from string in attribute 'mapping'
                String mapping = xattribs.getString(XML_MAPPING_ATTRIBUTE);
                Document doc = createDocumentFromString(mapping);
                
                Element rootElement = doc.getDocumentElement();
                nodes = rootElement.getChildNodes();
            } else {
                //old-fashioned version of mapping definition
                //mapping xml elements are child nodes of the component
                nodes = xmlElement.getChildNodes();
            }
            //iterate over 'Mapping' elements
            for (int i = 0; i < nodes.getLength(); i++) {
                org.w3c.dom.Node node = nodes.item(i);
                processMappings(graph, extract, null, node);
            }
            if (xattribs.exists(XML_SKIP_ROWS_ATTRIBUTE)){
            	extract.setSkipRows(xattribs.getInteger(XML_SKIP_ROWS_ATTRIBUTE));
            }
            if (xattribs.exists(XML_NUMRECORDS_ATTRIBUTE)){
            	extract.setNumRecords(xattribs.getInteger(XML_NUMRECORDS_ATTRIBUTE));
            }
            
            return extract;
        } catch (Exception ex) {
            throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
        }
    }
    
    /**
     * Creates org.w3c.dom.Document object from the given String.
     * 
     * @param inString
     * @return
     * @throws XMLConfigurationException
     */
    private static Document createDocumentFromString(String inString) throws XMLConfigurationException {
        InputSource is = new InputSource(new StringReader(inString));
        
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setCoalescing(true);

        Document doc;
        
        try {
            doc = dbf.newDocumentBuilder().parse(is);
        } catch (Exception e) {
            throw new XMLConfigurationException("Mapping parameter parse error occur.", e);
        }
        
        return doc;
    }
    
    private static void processMappings(TransformationGraph graph, XMLExtract extract, Mapping parentMapping, org.w3c.dom.Node nodeXML) {
        if (XML_MAPPING.equals(nodeXML.getNodeName())) {
            // for a mapping declaration, process all of the attributes
            // element, outPort, parentKeyName, generatedKey
            ComponentXMLAttributes attributes = new ComponentXMLAttributes((Element)nodeXML, graph);
            Mapping mapping = null;
            
            try {
                mapping = extract.new Mapping(attributes.getString(XML_ELEMENT),
                        attributes.getInteger(XML_OUTPORT));
            } catch(AttributeNotFoundException ex) {
                if (attributes.exists(XML_OUTPORT)) {
                    LOG
                            .warn(extract.getId()
                            + ": XML Extract : Mapping missing a required attribute, element for outPort "
                            + attributes.getString(XML_OUTPORT, null)
                            + ".  Skipping this mapping and all children.");
                } else if (attributes.exists("element")) {
                    LOG
                            .warn(extract.getId()
                            + ": XML Extract : Mapping missing a required attribute, outPort for element "
                            + attributes.getString(XML_ELEMENT, null)
                            + ".  Skipping this mapping and all children.");
                } else {
                    LOG
                            .warn(extract.getId()
                            + ": XML Extract Mapping missing required attributes, element and outPort.  Skipping this mapping and all children.");
                }
                return;
            }
            
            // Add this mapping to the parent
            if (parentMapping != null) {
                parentMapping.addChildMapping(mapping);
                mapping.setParent(parentMapping);
            } else {
                extract.addMapping(mapping);
            }

            boolean parentKeyPresent = false;
            boolean generatedKeyPresent = false;
            if (attributes.exists(XML_PARENTKEY)) {
                mapping.setParentKey(attributes.getString(XML_PARENTKEY, null).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
                parentKeyPresent = true;
            }
            
            if (attributes.exists(XML_GENERATEDKEY)) {
                mapping.setGeneratedKey(attributes.getString(XML_GENERATEDKEY, null).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
                generatedKeyPresent = true;
            }
            
            if (parentKeyPresent != generatedKeyPresent) {
                LOG
                        .warn(extract.getId()
                        + ": XML Extract Mapping for element: "
                        + mapping.getElement()
                        + " must either have both parentKey and generatedKey attributes or neither.");
                mapping.setParentKey(null);
                mapping.setGeneratedKey(null);
            }

            if (parentKeyPresent && mapping.getParent() == null) {
                LOG
                        .warn(extract.getId()
                        + ": XML Extact Mapping for element: "
                        + mapping.getElement()
                        + " may only have parentKey or generatedKey attributes if it is a nested mapping.");
                mapping.setParentKey(null);
                mapping.setGeneratedKey(null);
            }

            //mapping between xml fields and clover fields initialization
            if (attributes.exists(XML_XMLFIELDS) && attributes.exists(XML_CLOVERFIELDS)) {
                String[] xmlFields = attributes.getString(XML_XMLFIELDS, null).split(Defaults.Component.KEY_FIELDS_DELIMITER);
                String[] cloverFields = attributes.getString(XML_CLOVERFIELDS, null).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);

                if(xmlFields.length == cloverFields.length){
                    Map<String, String> xmlCloverMap = new HashMap<String, String>();
                    for (int i = 0; i < xmlFields.length; i++) {
                        xmlCloverMap.put(xmlFields[i], cloverFields[i]);
                    }
                    mapping.setXml2CloverFieldsMap(xmlCloverMap);
                } else {
                    LOG
                    .warn(extract.getId()
                    + ": XML Extact Mapping for element: "
                    + mapping.getElement()
                    + " must have same number of the xml fields and the clover fields attribute.");
                }
            }
            
            //sequence field
            if (attributes.exists(XML_SEQUENCEFIELD)) {
                mapping.setSequenceField(attributes.getString(XML_SEQUENCEFIELD, null));
                mapping.setSequenceId(attributes.getString(XML_SEQUENCEID, null));
            }
            
            // Process all nested mappings
            NodeList nodes = nodeXML.getChildNodes();
            for (int i = 0; i < nodes.getLength(); i++) {
                org.w3c.dom.Node node = nodes.item(i);
                processMappings(graph, extract, mapping, node);
            }
        } else if (nodeXML.getNodeType() == org.w3c.dom.Node.TEXT_NODE) {
            // Ignore text values inside nodes
        } else {
            LOG.warn(extract.getId() + ": Unknown element: "
                    + nodeXML.getLocalName()
                    + " ignoring it and all child elements.");
        }
    }
    
    @Override
    public Result execute() throws Exception {
    	Result result;
    	if (parseXML()) {
    		result = runIt ? Result.FINISHED_OK : Result.ABORTED;
    	}else{
    		result = runIt ? Result.ERROR : Result.ABORTED;
    	}
    	broadcastEOF();
		return result;
    }
    
     /**
     * Parses the inputSource. The SAXHandler defined in this class will handle
     * the rest of the events. Returns false if there was an exception
     * encountered during processing.
     */
    private boolean parseXML() throws JetelException{
        SAXParserFactory factory = SAXParserFactory.newInstance();
        SAXParser parser;
        
        try {
            parser = factory.newSAXParser();
        } catch (Exception ex) {
        	throw new JetelException(ex.getMessage(), ex);
        }
        
        try {
            parser.parse(m_inputSource, new SAXHandler());
        } catch (SAXException ex) {
            if (!runIt) {
                return true; // we were stopped by a stop signal... probably
            }
            LOG.error("XML Extract: " + getId() + " Parse Exception", ex);
            throw new JetelException("XML Extract: " + getId() + " Parse Exception", ex);
        } catch (Exception ex) {
            LOG.error("XML Extract: " + getId() + " Unexpected Exception", ex);
            throw new JetelException("XML Extract: " + getId() + " Unexpected Exception", ex);
        }
        return true;
    }
    
    // //////////////////////////////////////////////////////////////////
    // Clover Call Back Methods
    //
    /**
     * Perform sanity checks.
     */
    public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
        // test that we have at least one input port and one output
        if (outPorts.size() < 1) {
            throw new ComponentNotReadyException(getId()
            + ": At least one output port has to be defined!");
        }
        
        if (m_elementPortMap.size() < 1) {
            throw new ComponentNotReadyException(
                    getId()
                    + ": At least one mapping has to be defined.  <Mapping element=\"elementToMatch\" outPort=\"123\" [parentKey=\"key in parent\" generatedKey=\"new foreign key in target\"]/>");
        }
    }
    
    public String getType() {
        return COMPONENT_TYPE;
    }
    
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        //TODO
        return status;
    }
    
    public org.w3c.dom.Node toXML() {
        return null;
    }
    
    // //////////////////////////////////////////////////////////////////
    // Accessors
    //
    /**
     * Set the input source containing the XML this will parse.
     */
    public void setInputSource(InputSource inputSource) {
        m_inputSource = inputSource;
    }
    
    public void setUseNestedNodes(boolean useNestedNodes) {
        this.useNestedNodes = useNestedNodes;
    }
    
    /**
     * Accessor to add a mapping programatically.
     */
    public void addMapping(Mapping mapping) {
        m_elementPortMap.put(mapping.getElement(), mapping);
    }
    
    /**
     * Returns the mapping. Maybe make this read-only?
     */
    public Map getMappings() {
        // return Collections.unmodifiableMap(m_elementPortMap); // return a
        // read-only map
        return m_elementPortMap;
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

//    private void resetRecord(DataRecord record) {
//        // reset the record setting the nullable fields to null and default
//        // values. Unfortunately init() does not do this, so if you have a field
//        // that's nullable and you never set a value to it, it will NOT be null.
//        
//        // the reason we need to reset data records is the fact that XML data is
//        // not as rigidly
//        // structured as csv fields, so column values are regularly "missing"
//        // and without a reset
//        // the prior row's value will be present.
//        for (int i = 0; i < record.getNumFields(); i++) {
//            DataFieldMetadata fieldMetadata = record.getMetadata().getField(i);
//            DataField field = record.getField(i);
//            if (fieldMetadata.isNullable()) {
//                // Default all nullables to null
//                field.setNull(true);
//            } else if(fieldMetadata.isDefaultValue()) {
//                //Default all default values to their given defaults
//                field.setToDefaultValue();
//            } else {
//                // Not nullable so set it to the default value (what init does)
//                switch (fieldMetadata.getType()) {
//                    case DataFieldMetadata.INTEGER_FIELD:
//                        ((IntegerDataField) field).setValue(0);
//                        break;
//                        
//                    case DataFieldMetadata.STRING_FIELD:
//                        ((StringDataField) field).setValue("");
//                        break;
//                        
//                    case DataFieldMetadata.DATE_FIELD:
//                    case DataFieldMetadata.DATETIME_FIELD:
//                        ((DateDataField) field).setValue(0);
//                        break;
//                        
//                    case DataFieldMetadata.NUMERIC_FIELD:
//                        ((NumericDataField) field).setValue(0);
//                        break;
//                        
//                    case DataFieldMetadata.LONG_FIELD:
//                        ((LongDataField) field).setValue(0);
//                        break;
//                        
//                    case DataFieldMetadata.DECIMAL_FIELD:
//                        ((NumericDataField) field).setValue(0);
//                        break;
//                        
//                    case DataFieldMetadata.BYTE_FIELD:
//                        ((ByteDataField) field).setValue((byte) 0);
//                        break;
//                        
//                    case DataFieldMetadata.UNKNOWN_FIELD:
//                    default:
//                        break;
//                }
//            }
//        }
//    }
}

