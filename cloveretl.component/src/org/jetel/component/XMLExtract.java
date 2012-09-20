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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.lang.ref.WeakReference;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dom4j.io.SAXContentHandler;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.StringDataField;
import org.jetel.data.sequence.Sequence;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.sequence.PrimitiveSequence;
import org.jetel.util.AutoFilling;
import org.jetel.util.ReadableChannelIterator;
import org.jetel.util.file.FileURLParser;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.PropertyRefResolver;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;
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
 * &nbsp;outPort NMTOKEN #IMPLIED<br>      
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

/*
 * Invariant:
 *  Mappings are preprocessed so that all element and attribute references stored in internal structures
 *  use universal names :
 *  	element="mov:movies" -> element="{http://www.javlin.eu}movies"
 *  	xmlFields="mov:att1" -> xmlFields="{http://www.javlin.eu}att1"
 *  
 *  Default namespace in mapping -> NOT POSSIBLE
 *  We are not able to implement default namespace functionality.
 *  Default namespace (declared without prefix e.g. xmlns="http://www.javlin.eu/default-ns") does
 *  NOT apply to attributes!!! (see www.w3.org/TR/REC-xml-names/#defaulting)
 *  Since we currently do not know which xmlFields 
 *  are attributes and which are elements -> so we do not know which should be expanded by default namespace.
 *  User therefore has to declare explicit namespace prefix in the Mapping, even for
 *  attributes and elements falling into the default namespace in the processed XML document.
 *  
 */
public class XMLExtract extends Node {

    // Logger
    private static final Log LOG = LogFactory.getLog(XMLExtract.class);

    // xml attributes
    public static final String XML_SOURCEURI_ATTRIBUTE = "sourceUri";
    private static final String XML_USENESTEDNODES_ATTRIBUTE = "useNestedNodes";
    private static final String XML_MAPPING_ATTRIBUTE = "mapping";
    private static final String XML_CHARSET_ATTRIBUTE = "charset";

    // mapping attributes
    private static final String XML_MAPPING = "Mapping";
	private final static String XML_MAPPING_URL_ATTRIBUTE = "mappingURL";
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
	private static final String XML_TRIM_ATTRIBUTE = "trim";
    private static final String XML_VALIDATE_ATTRIBUTE = "validate";
    private static final String XML_XML_FEATURES_ATTRIBUTE = "xmlFeatures";
    private static final String XML_NAMESPACE_BINDINGS_ATTRIBUTE = "namespaceBindings";
    
    /** MiSho Experimental Templates */
    private static final String XML_TEMPLATE_ID = "templateId";
    private static final String XML_TEMPLATE_REF = "templateRef";
    private static final String XML_TEMPLATE_DEPTH = "nestedDepth";
    
    private static final String FEATURES_DELIMETER = ";";
    private static final String FEATURES_ASSIGN = ":=";

    // component name
    public final static String COMPONENT_TYPE = "XML_EXTRACT";
    
    // from which input port to read
	private final static int INPUT_PORT = 0;

	public static final String PARENT_MAPPING_REFERENCE_PREFIX = "..";
	public static final String PARENT_MAPPING_REFERENCE_SEPARATOR = "/";
	public static final String PARENT_MAPPING_REFERENCE_PREFIX_WITHSEPARATOR = PARENT_MAPPING_REFERENCE_PREFIX + PARENT_MAPPING_REFERENCE_SEPARATOR;
	public static final String ELEMENT_VALUE_REFERENCE = "{}.";
	
    // Map of elementName => output port
    private Map<String, Mapping> m_elementPortMap = new HashMap<String, Mapping>();
    
    // Where the XML comes from
    private InputSource m_inputSource;

    // input file
    private String inputFile;
	private ReadableChannelIterator readableChannelIterator;

	// can I use nested nodes for mapping processing?
    private boolean useNestedNodes = true;

    // global skip and numRecords
    private int skipRows=0; // do not skip rows by default
    private int numRecords = -1;

    // autofilling support
    private AutoFilling autoFilling = new AutoFilling();

	private String xmlFeatures;
	
	private boolean validate;

	private String charset = Defaults.DataParser.DEFAULT_CHARSET_DECODER;

	private boolean trim = true;

	private String mappingURL;

	private String mapping;

	private NodeList mappingNodes;

	private TreeMap<String, Mapping> declaredTemplates = new TreeMap<String, Mapping>();
	
	/**
	 * Namespace bindings relate namespace prefix used in Mapping specification
	 * and the namespace URI used by the namespace declaration in processed XML document
	 */
	private HashMap<String,String> namespaceBindings = new HashMap<String,String>();
	
    /**
     * SAX Handler that will dispatch the elements to the different ports.
     */
    private class SAXHandler extends SAXContentHandler {
        
        // depth of the element, used to determine when we hit the matching
        // close element
        private int m_level = 0;
        
        // flag set if we saw characters, otherwise don't save the column (used
        // to set null values)
        private boolean m_hasCharacters = false;
        //flag to skip text value immediately after end xml tag, for instance
        //<root>
        //	<subtag>text</subtag>
        //	another text
        //</root>
        //"another text" will be ignored
        private boolean m_grabCharacters = true;
        
        // buffer for node value
        private StringBuilder m_characters = new StringBuilder();
        
        // the active mapping
        private Mapping m_activeMapping = null;
        
        private Set<String> cloverAttributes;
        
        /**
		 * @param cloverAttributes
		 */
		public SAXHandler(Set<String> cloverAttributes) {
			super();
			this.cloverAttributes = cloverAttributes;
		}

		/**
         * @see org.xml.sax.ContentHandler#startElement(java.lang.String, java.lang.String, java.lang.String, org.xml.sax.Attributes)
         */
        public void startElement(String namespaceURI, String localName, String qualifiedName, Attributes attributes) throws SAXException {
            m_level++;
            m_grabCharacters = true;
            
    		// store value of parent of currently starting element (if appropriate)
        	if (m_activeMapping != null && m_hasCharacters && m_level == m_activeMapping.getLevel() + 1) {
                if (m_activeMapping.descendantReferences.containsKey(ELEMENT_VALUE_REFERENCE)) {
               		m_activeMapping.descendantReferences.put(ELEMENT_VALUE_REFERENCE, trim ? m_characters.toString().trim() : m_characters.toString());
                }
        		processCharacters(null,null, true);
        	}
        	
            // Regardless of starting element type, reset the length of the buffer and flag
            m_characters.setLength(0);
            m_hasCharacters = false;
            
            final String universalName = augmentURI(namespaceURI) + localName;
            Mapping mapping = null;
            if (m_activeMapping == null) {
                mapping = (Mapping) m_elementPortMap.get(universalName);
                
                // CL-2053 - backward compatibility (part 1/2)
                if (mapping == null) {
                	mapping = (Mapping) m_elementPortMap.get("{}" + localName);
                }
            } else if (useNestedNodes || m_activeMapping.getLevel() == m_level - 1) {
                mapping = (Mapping) m_activeMapping.getChildMapping(universalName);
                
                // CL-2053 - backward compatibility (part 2/2)
                if (mapping == null) {
                	mapping = (Mapping) m_activeMapping.getChildMapping("{}" + localName);
                }
            }
            if (mapping != null) {
                // We have a match, start converting all child nodes into
                // the DataRecord structure
                m_activeMapping = mapping;
                m_activeMapping.setLevel(m_level);
                // clear cached values of xml fields referenced by descendants (there may be values from previously read element of this m_activemapping)
                for (Entry<String, String> e : m_activeMapping.descendantReferences.entrySet()) {
					e.setValue(null);
				}
                
                if (mapping.getOutRecord() != null) {
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
	               	m_activeMapping.prepareDoMap();
	               	m_activeMapping.incCurrentRecord4Mapping();
	                
	                // This is the closing element of the matched element that
	                // triggered the processing
	                // That should be the end of this record so send it off to the
	                // next Node
	                if (runIt) {
	                    try {
	                        DataRecord outRecord = m_activeMapping.getOutRecord();
	                        String[] generatedKey = m_activeMapping.getGeneratedKey();
	                        String[] parentKey = m_activeMapping.getParentKey();
	                        if (parentKey != null) {
	                            //if generatedKey is a single array, all parent keys are concatenated into generatedKey field
	                            //I know it is ugly code...
	                            if(generatedKey.length != parentKey.length && generatedKey.length != 1) {
	                                LOG.warn(getId() + ": XML Extract Mapping's generatedKey and parentKey attribute has different number of field.");
	                                m_activeMapping.setGeneratedKey(null);
	                                m_activeMapping.setParentKey(null);
	                            } else {
	                                for(int i = 0; i < parentKey.length; i++) {
	                                    boolean existGeneratedKeyField = (outRecord != null) 
	                                    			&& (generatedKey.length == 1 ? outRecord.hasField(generatedKey[0]) : outRecord.hasField(generatedKey[i]));
	                                    boolean existParentKeyField = m_activeMapping.getParent().getOutRecord() != null 
	                                    					&& m_activeMapping.getParent().getOutRecord().hasField(parentKey[i]);
	                                    if (!existGeneratedKeyField) {
	                                        LOG.warn(getId() + ": XML Extract Mapping's generatedKey field was not found. generatedKey: "
	                                                + (generatedKey.length == 1 ? generatedKey[0] : generatedKey[i]) + " of element " + m_activeMapping.m_element + ", outPort: " + m_activeMapping.m_outPort);
	                                        m_activeMapping.setGeneratedKey(null);
	                                        m_activeMapping.setParentKey(null);
	                                    } else if (!existParentKeyField) {
	                                        LOG.warn(getId() + ": XML Extract Mapping's parentKey field was not found. parentKey: " + parentKey[i] + " of element " + m_activeMapping.m_element + ", outPort: " + m_activeMapping.m_outPort);
	                                        m_activeMapping.setGeneratedKey(null);
	                                        m_activeMapping.setParentKey(null);
	                                    } else {
	                                    	// both outRecord and m_activeMapping.getParrent().getOutRecord are not null
	                                    	// here, because of if-else if-else chain
	                                        DataField generatedKeyField = generatedKey.length == 1 ? outRecord.getField(generatedKey[0]) : outRecord.getField(generatedKey[i]);
	                                        DataField parentKeyField = m_activeMapping.getParent().getOutRecord().getField(parentKey[i]);
	                                        if(generatedKey.length != parentKey.length) {
	                                            if(generatedKeyField.getType() != DataFieldMetadata.STRING_FIELD) {
	                                                LOG.warn(getId() + ": XML Extract Mapping's generatedKey field has to be String type (keys are concatened to this field).");
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
	                    } catch (Exception ex) {
	                        throw new SAXException(" for output port number '" + m_activeMapping.getOutPort() + "'. Check also parent mapping. ", ex);
	                    }
	                    
	                    // Fill fields from parent record (if any mapped)
	                    if (m_activeMapping.hasFieldsFromAncestor()) {
	                    	for (AncestorFieldMapping afm : m_activeMapping.getFieldsFromAncestor()) {
	                    		if (m_activeMapping.getOutRecord().hasField(afm.currentField) && afm.ancestor != null) {
	                    			m_activeMapping.getOutRecord().getField(afm.currentField).fromString(afm.ancestor.descendantReferences.get(afm.ancestorField));
	                    		}
	                    	}
	                    }
	                } else {
	                    throw new SAXException("Stop Signaled");
	                }
                }
            }
            
            if(m_activeMapping != null //used only if we right now recognize new mapping element or if we want to use nested unmapped nodes as a source of data
                    && (useNestedNodes || mapping != null)) {
                // In a matched element (i.e. we are creating a DataRecord)
                // Store all attributes as columns (this hasn't been
                // used/tested)                
                for (int i = 0; i < attributes.getLength(); i++) {
                	/*
                	 * Note: XML namespaces handling
                	 * Default namespace declared via "xmlns" attribute without prefix specification (e.g. xmlns="http://www.javlin.eu/movies)
                	 * does NOT apply on attributes (see http://www.w3.org/TR/REC-xml-names/#defaulting)
                	 */
                	final String attributeLocalName = attributes.getLocalName(i);
                    String attrName = augmentURI(attributes.getURI(i)) + attributeLocalName;
                    
                    if (m_activeMapping.descendantReferences.containsKey(attrName)) {
                    	String val = attributes.getValue(i);
                    	m_activeMapping.descendantReferences.put(attrName, trim ? val.trim() : val);
                    }
                    
                    //use fields mapping
                    final Map<String, String> xmlCloverMap = m_activeMapping.getXml2CloverFieldsMap();
                    String fieldName = null;
                    if (xmlCloverMap != null) {
                    	if (xmlCloverMap.containsKey(attrName)) {
                    		fieldName = xmlCloverMap.get(attrName);
                    	} else if (m_activeMapping.explicitCloverFields.contains(attrName)) {
                    		continue; // don't do implicit mapping if clover field is used in an explicit mapping 
                    	}
                    }
                    
                    if (fieldName == null) {
                    	// we could not find mapping using the universal name -> try implicit mapping using local name
                    	fieldName = attributeLocalName;
                    }

                    // TODO Labels replace:
                    if (m_activeMapping.getOutRecord() != null && m_activeMapping.getOutRecord().hasField(fieldName)) {
                    	String val = attributes.getValue(i);
                        m_activeMapping.getOutRecord().getField(fieldName).fromString(trim ? val.trim() : val);
                    }
                    // TODO Labels end replace

					// TODO Labels replace with:
                    //DataRecord outRecord = m_activeMapping.getOutRecord();
                    //DataField field = null;
                    //
                    //if (outRecord != null) {
                    //	if (outRecord.hasLabeledField(fieldName)) {
                    //		field = outRecord.getFieldByLabel(fieldName);
                    //	}
                    //}
                    //
                    //if (field != null) {
                    //	String val = attributes.getValue(i);
                    //	field.fromString(trim ? val.trim() : val);
                    //}
                }
            }
            
        }
        
        /**
         * @see org.xml.sax.ContentHandler#characters(char[], int, int)
         */
        public void characters(char[] data, int offset, int length) throws SAXException {
            // Save the characters into the buffer, endElement will store it into the field
            if (m_activeMapping != null && m_grabCharacters) {
                m_characters.append(data, offset, length);
                m_hasCharacters = true;
            }
        }
        
        /**
         * @see org.xml.sax.ContentHandler#endElement(java.lang.String, java.lang.String, java.lang.String)
         */
        public void endElement(String namespaceURI, String localName, String qualifiedName) throws SAXException {
            if (m_activeMapping != null) {
            	String fullName = "{" + namespaceURI + "}" + localName;
            	
            	// cache characters value if the xml field is referenced by descendant
                if (m_level - 1 <= m_activeMapping.getLevel() && m_activeMapping.descendantReferences.containsKey(fullName)) {
               		m_activeMapping.descendantReferences.put(fullName, trim ? m_characters.toString().trim() : m_characters.toString());
                }
               	processCharacters(namespaceURI, localName, m_level == m_activeMapping.getLevel());
                
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
                        
                        if (outPort != null) {
                            // we just ignore creating output, if port is empty (without metadata) or not specified
	                        DataRecord outRecord = m_activeMapping.getOutRecord();
	                        
	                        // skip or process row
	                    	if (skipRows > 0) {
	                    		if (m_activeMapping.getParent() == null) skipRows--;
	                    	} else {
	                            //check for index of last returned record
	                            if(!(numRecords >= 0 && numRecords == autoFilling.getGlobalCounter())) {
	                            	// set autofilling
	                                autoFilling.setAutoFillingFields(outRecord);
	                                
	                                // can I do the map? it depends on skip and numRecords.
	                                if (m_activeMapping.doMap()) {
		                                //send off record
	                                	outPort.writeRecord(outRecord);
	                                }
//	                                if (m_activeMapping.getParent() == null) autoFilling.incGlobalCounter();
	                            }
	                    	}
	                    	
	                    	// resets all child's mappings for skip and numRecords 
                           	m_activeMapping.resetCurrentRecord4ChildMapping();

                        	// reset record
	                        outRecord.reset();
                        }
                       
                        m_activeMapping = m_activeMapping.getParent();
                    } catch (Exception ex) {
                        throw new SAXException(ex);
                    }
                } else {
                    throw new SAXException("Stop Signaled");
                }
            }
            
            //text value immediately after end tag element should not be stored
            m_grabCharacters = false;
            
            //ended an element so decrease our depth
            m_level--; 
        }

		/**
		 * Store the characters processed by the characters() call back only if we have corresponding 
		 * output field and we are on the right level or we want to use data from nested unmapped nodes
		 */
		private void processCharacters(String namespaceURI, String localName, boolean elementValue) {
        	// Create universal name
			String universalName = null;
			if (localName != null) {
				universalName = augmentURI(namespaceURI) + localName;
			}
			
			String fieldName = null;
			//use fields mapping
            Map<String, String> xml2clover = m_activeMapping.getXml2CloverFieldsMap();
            if (xml2clover != null) {
           		if (elementValue && xml2clover.containsKey(ELEMENT_VALUE_REFERENCE)) {
            		fieldName = xml2clover.get(ELEMENT_VALUE_REFERENCE);
        		} else if (xml2clover.containsKey(universalName)) {
            		fieldName = xml2clover.get(universalName);
        		} else if (m_activeMapping.explicitCloverFields.contains(localName) ) {
        			// XXX: this is nonsense code ... the names stored here are field names and the code used XML element names
        			return; // don't do implicit mapping if clover field is used in an explicit mapping
        		}

           		if (fieldName == null) {
           			/*
           			 * As we could not find match using qualified name
           			 * try mapping the xml element/attribute without the namespace prefix
           			 */
           			fieldName = localName;
           		}
            }
            
			// TODO Labels replace:
			if (m_activeMapping.getOutRecord() != null && m_activeMapping.getOutRecord().hasField(fieldName) 
			        && (useNestedNodes || m_level - 1 <= m_activeMapping.getLevel())) {
			    DataField field = m_activeMapping.getOutRecord().getField(fieldName);
			// TODO Labels replace end
			
			// TODO Labels replace with:
            //DataRecord outRecord = m_activeMapping.getOutRecord();
            //DataField field = null;
            //
			//if ((outRecord != null) && (useNestedNodes || m_level - 1 <= m_activeMapping.getLevel())) {
            //	if (outRecord.hasLabeledField(fieldName)) {
            //		field = outRecord.getFieldByLabel(fieldName);
            //	}
			//}
			//
			//if (field != null) {
			// TODO Labels replace with end
			    // If field is nullable and there's no character data set it to null
			    if (m_hasCharacters) {
			        try {
			    	if (field.getValue() != null && cloverAttributes.contains(fieldName)) {
			    		field.fromString(trim ? field.getValue().toString().trim() : field.getValue().toString());
			    	} else {
			    		field.fromString(trim ? m_characters.toString().trim() : m_characters.toString());
			    	}
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
			                    DateFormat format = new SimpleDateFormat(field.getMetadata().getFormatStr());
			                    field.setValue(format.parse(trim ? dateTime.trim() : dateTime));
			                } catch (Exception ex2) {
			                    // Oh well we tried, throw the originating
			                    // exception
			                    throw ex;
			                }
			            } else {
			                throw ex;
			            }
			        }
			    } else if (field.getType() == DataFieldMetadata.STRING_FIELD 
			    		// and value wasn't already stored (from characters)
			    		&& (field.getValue() == null || field.getValue().equals(field.getMetadata().getDefaultValueStr()))) {
			    	field.setValue("");
			    }
			}
		}
    }
    
    /**
     * Mapping holds a single mapping.
     */
    public class Mapping {
        String m_element;								// name of an element for this mapping
        int m_outPort;									// output port number
        DataRecord m_outRecord;							// output record
        String[] m_parentKey;							// parent keys
        String[] m_generatedKey;						// generated keys
        Map<String, Mapping> m_childMap;				// direct children for this mapping 
        WeakReference<Mapping> m_parent;				// direct parent mapping
        int m_level;									// original xml tree level (a depth of this element) 
        String m_sequenceField;							// sequence field
        String m_sequenceId;							// sequence ID
        Sequence sequence;								// sequence (Simple, Db,..)
        
        /** Mapping - xml name -> clover field name */
        Map<String, String> xml2CloverFieldsMap = new HashMap<String, String>();

        /** List of clover fields (among else) which will be filled from ancestor */
        List<AncestorFieldMapping> fieldsFromAncestor; 

        /** Mapping - xml name -> clover field name; these xml fields are referenced by descendant mappings */
        Map<String, String> descendantReferences = new HashMap<String, String>();
        
        /** Set of Clover fields which are mapped explicitly (using xmlFields & cloverFields attributes).
         *  It is union of xml2CloverFieldsMap.values() and Clover fields from fieldsFromAncestor list. Its purpose: quick lookup
         */
        Set<String> explicitCloverFields = new HashSet<String>(); 
        
        // for skip and number a record attribute for this mapping
		int skipRecords4Mapping;						// skip records
		int numRecords4Mapping = Integer.MAX_VALUE;		// number records
//		int skipSourceRecords4Mapping;					// skip records
//		int numSourceRecords4Mapping = -1;              // number records
		int currentRecord4Mapping;						// record counter for this mapping
		boolean processSkipOrNumRecords;				// what xml element can be skiped
		boolean bDoMap = true;							// should I skip an xml element? depends on processSkipOrNumRecords
		boolean bReset4CurrentRecord4Mapping;			// should I reset submappings?
        
		/**
		 * Copy constructor - created a deep copy of all attributes and children elements
		 */
		public Mapping(Mapping otherMapping, Mapping parent) {
			this.m_element = otherMapping.m_element;
			this.m_outPort = otherMapping.m_outPort;
			this.m_parentKey = otherMapping.m_parentKey == null ? null : Arrays.copyOf(otherMapping.m_parentKey,otherMapping.m_parentKey.length);
			this.m_generatedKey = otherMapping.m_generatedKey == null ? null : Arrays.copyOf(otherMapping.m_generatedKey, otherMapping.m_generatedKey.length);
			this.m_sequenceField = otherMapping.m_sequenceField;
			this.m_sequenceId = otherMapping.m_sequenceId;
			this.skipRecords4Mapping = otherMapping.skipRecords4Mapping;
			this.numRecords4Mapping = otherMapping.numRecords4Mapping;
			xml2CloverFieldsMap = new HashMap<String, String>(otherMapping.xml2CloverFieldsMap);
			
			// Create deep copy of children elements 
			if (otherMapping.m_childMap != null) {
				this.m_childMap = new HashMap<String,Mapping>();
				for (String key : otherMapping.m_childMap.keySet()) {
					final Mapping child = new Mapping(otherMapping.m_childMap.get(key), this);
					this.m_childMap.put(key, child);
				}
			}

			if (parent != null) {
				setParent(parent);
				parent.addChildMapping(this);
			}
			
			if (otherMapping.hasFieldsFromAncestor()) {
				for (AncestorFieldMapping m : otherMapping.getFieldsFromAncestor()) {
					addAncestorFieldMapping(m.originalFieldReference, m.currentField);
				}
			}

		}
		
        /**
         * Minimally required information.
         */
        public Mapping(String element, int outPort, Mapping parent) {
            m_element = element;
            m_outPort = outPort;
            m_parent = new WeakReference<Mapping>(parent);
            if (parent != null) {
            	parent.addChildMapping(this);
            }
        }
        
		/**
         * Gives the optional attributes parentKey and generatedKey.
         */
        public Mapping(String element, int outPort, String parentKey[],
                String[] generatedKey, Mapping parent) {
            this(element, outPort, parent);
            
            m_parentKey = parentKey;
            m_generatedKey = generatedKey;
        }
        
        /**
         * Gets original xml tree level (a deep of this element)
         * @return
         */
        public int getLevel() {
            return m_level;
        }
        
        /**
         * Sets original xml tree level (a deep of this element)
         * @param level
         */
        public void setLevel(int level) {
            m_level = level;
        }
        
        /**
         * Sets direct children for this mapping. 
         * @return
         */
        public Map<String, Mapping> getChildMap() {
            return m_childMap;
        }
        
        /**
         * Gets direct children for this mapping. 
         * @param element
         * @return
         */
        public Mapping getChildMapping(String element) {
            if (m_childMap == null) {
                return null;
            }
            return m_childMap.get(element);
        }
        
        /**
         * Adds a direct child for this mapping.
         * @param mapping
         */
        public void addChildMapping(Mapping mapping) {
            if (m_childMap == null) {
                m_childMap = new HashMap<String, Mapping>();
            }
            m_childMap.put(mapping.getElement(), mapping);
        }
        
        /**
         * Removes a direct child for this mapping.
         * @param mapping
         */
        public void removeChildMapping(Mapping mapping) {
            if (m_childMap == null) {
                return;
            }
            m_childMap.remove(mapping.getElement());
        }
        
        /**
         * Gets an element name for this mapping.
         * @return
         */
        public String getElement() {
            return m_element;
        }
        
        /**
         * Sets an element name for this mapping.
         * @param element
         */
        public void setElement(String element) {
            m_element = element;
        }
        
        /**
         * Gets generated keys of for this mapping.
         * @return
         */
        public String[] getGeneratedKey() {
            return m_generatedKey;
        }
        
        /**
         * Sets generated keys of for this mapping.
         * @param generatedKey
         */
        public void setGeneratedKey(String[] generatedKey) {
            m_generatedKey = generatedKey;
        }
        
        /**
         * Gets an output port.
         * @return
         */
        public int getOutPort() {
            return m_outPort;
        }
        
        /**
         * Sets an output port.
         * @param outPort
         */
        public void setOutPort(int outPort) {
            m_outPort = outPort;
        }
        
        /**
         * Gets mapping - xml name -> clover field name
         * WARNING: values of this map must be kept in synch with explicitCloverFields; prefer {@link #putXml2CloverFieldMap()} 
         */
        public Map<String, String> getXml2CloverFieldsMap() {
            return xml2CloverFieldsMap;
        }
        
        public void putXml2CloverFieldMap(String xmlField, String cloverField) {
        	xml2CloverFieldsMap.put(createQualifiedName(xmlField), cloverField);
        	explicitCloverFields.add(cloverField);
        }
        
        /**
         * Gets an output record.
         * @return
         */
        public DataRecord getOutRecord() {
            if (m_outRecord == null) {
                OutputPort outPort = getOutputPort(getOutPort());
                if (outPort != null) {
                	DataRecordMetadata dataRecordMetadata = outPort.getMetadata();
                	autoFilling.addAutoFillingFields(dataRecordMetadata);
                    m_outRecord = new DataRecord(dataRecordMetadata);
                    m_outRecord.init();
                    m_outRecord.reset();
                } // Original code is commented, it is valid to have null port now
                /* else {
                    LOG
                            .warn(getId()
                            + ": Port "
                            + getOutPort()
                            + " does not have an edge connected.  Please connect the edge or remove the mapping.");
                }*/ 
            }
            return m_outRecord;
        }
        
        /**
         * Sets an output record.
         * @param outRecord
         */
        public void setOutRecord(DataRecord outRecord) {
            m_outRecord = outRecord;
        }
        
        /**
         * Gets parent key.
         * @return
         */
        public String[] getParentKey() {
            return m_parentKey;
        }
        
        /**
         * Sets parent key.
         * @param parentKey
         */
        public void setParentKey(String[] parentKey) {
            m_parentKey = parentKey;
        }
        
        /**
         * Gets a parent mapping.
         * @return
         */
        public Mapping getParent() {
            if (m_parent != null) {
                return m_parent.get();
            } else {
                return null;
            }
        }
        
        /**
         * Sets a parent mapping.
         * @param parent
         */
        public void setParent(Mapping parent) {
            m_parent = new WeakReference<Mapping>(parent);
        }

        /**
         * Gets a sequence name.
         * @return
         */
        public String getSequenceField() {
            return m_sequenceField;
        }

        /**
         * Sets a sequence name.
         * @param field
         */
        public void setSequenceField(String field) {
            m_sequenceField = field;
        }

        /**
         * Gets a sequence ID.
         * @return
         */
        public String getSequenceId() {
            return m_sequenceId;
        }

        /**
         * Sets a sequence ID.
         * @param id
         */
        public void setSequenceId(String id) {
            m_sequenceId = id;
        }
        
        /**
         * Gets a Sequence (simple sequence, db sequence, ...).
         * @return
         */
        public Sequence getSequence() {
            if(sequence == null) {
                String element = StringUtils.normalizeName(StringUtils.trimXmlNamespace(getElement()));

                if(getSequenceId() == null) {
                    sequence = new PrimitiveSequence(element, getGraph(), element);
                } else {
                    sequence = getGraph().getSequence(getSequenceId());

                    if(sequence == null) {
                        LOG.warn(getId() + ": Sequence " + getSequenceId() + " does not exist in "
                                + "transformation graph. Primitive sequence is used instead.");
                        sequence = new PrimitiveSequence(element, getGraph(), element);
                    }
                }
            }

            return sequence;
        }
        
        /**
         * processSkipOrNumRecords is true - mapping can be skipped
         */
		public boolean getProcessSkipOrNumRecords() {
			if (processSkipOrNumRecords) return true;
			Mapping parent = getParent();
			if (parent == null) {
				return processSkipOrNumRecords;
			}
			return parent.getProcessSkipOrNumRecords();
		}
		
		/**
		 * Sets inner variables for processSkipOrNumRecords.
		 */
		public void prepareProcessSkipOrNumRecords() {
			Mapping parentMapping = getParent();
			processSkipOrNumRecords = parentMapping != null && parentMapping.getProcessSkipOrNumRecords() ||
				(skipRecords4Mapping > 0 || numRecords4Mapping < Integer.MAX_VALUE);
		}
		
		/**
		 * Sets inner variables for bReset4CurrentRecord4Mapping.
		 */
		public void prepareReset4CurrentRecord4Mapping() {
			bReset4CurrentRecord4Mapping = processSkipOrNumRecords;
        	if (m_childMap != null) {
        		Mapping mapping;
        		for (Iterator<Entry<String, Mapping>> it=m_childMap.entrySet().iterator(); it.hasNext();) {
        			mapping = it.next().getValue();
        			if (mapping.processSkipOrNumRecords) {
        				bReset4CurrentRecord4Mapping = true;
        				break;
        			}
        		}
        	}
		}
		
		/**
		 * skipRecords for this mapping.
		 * @param skipRecords4Mapping
		 */
        public void setSkipRecords4Mapping(int skipRecords4Mapping) {
        	this.skipRecords4Mapping = skipRecords4Mapping;
        }
        
        /**
         * numRecords for this mapping.
         * @param numRecords4Mapping
         */
        public void setNumRecords4Mapping(int numRecords4Mapping) {
        	this.numRecords4Mapping = numRecords4Mapping;
        }
        
//		/**
//		 * skipRecords for this mapping.
//		 * @param skipRecords4Mapping
//		 */
//        public void setSkipSourceRecords4Mapping(int skipSourceRecords4Mapping) {
//        	this.skipSourceRecords4Mapping = skipSourceRecords4Mapping;
//        }
//        
//        /**
//         * numRecords for this mapping.
//         * @param numRecords4Mapping
//         */
//        public void setNumSourceRecords4Mapping(int numSourceRecords4Mapping) {
//        	this.numSourceRecords4Mapping = numSourceRecords4Mapping;
//        }
//
        /**
         * Counter for this mapping.
         */
        public void incCurrentRecord4Mapping() {
        	currentRecord4Mapping++;
		}
        
        /**
         * Resets submappings.
         */
        public void resetCurrentRecord4ChildMapping() {
        	if (!bReset4CurrentRecord4Mapping) return;
        	if (m_childMap != null) {
        		Mapping mapping;
        		for (Iterator<Entry<String, Mapping>> it=m_childMap.entrySet().iterator(); it.hasNext();) {
        			mapping = it.next().getValue();
        			mapping.currentRecord4Mapping = 0;
        			mapping.resetCurrentRecord4ChildMapping();
        		}
        	}
		}

        /**
         * Sets if this and child mapping should be skipped.
         */
		public void prepareDoMap() {
			if (!processSkipOrNumRecords) return;
			Mapping parent = getParent();
        	bDoMap = (parent == null || parent.doMap()) && 
        		currentRecord4Mapping >= skipRecords4Mapping && currentRecord4Mapping-skipRecords4Mapping < numRecords4Mapping;
        	if (m_childMap != null) {
        		Mapping mapping;
        		for (Iterator<Entry<String, Mapping>> it=m_childMap.entrySet().iterator(); it.hasNext();) {
        			mapping = it.next().getValue();
        			mapping.prepareDoMap();
        		}
        	}
		}
		
		/**
		 * Can process this mapping? It depends on currentRecord4Mapping, skipRecords4Mapping and numRecords4Mapping
		 * for this and parent mappings.
		 * @return
		 */
        public boolean doMap() {
        	return !processSkipOrNumRecords || (processSkipOrNumRecords && bDoMap);
        }
        
        public void addAncestorField(AncestorFieldMapping ancestorFieldReference) {
        	if (fieldsFromAncestor == null) {
        		fieldsFromAncestor = new LinkedList<AncestorFieldMapping>();
        	}
        	fieldsFromAncestor.add(ancestorFieldReference);
        	if (ancestorFieldReference.ancestor != null) {
        		ancestorFieldReference.ancestor.descendantReferences.put(ancestorFieldReference.ancestorField, null);
        	}
        	explicitCloverFields.add(ancestorFieldReference.currentField);
        }
        
		public List<AncestorFieldMapping> getFieldsFromAncestor() {
			return fieldsFromAncestor;
		}
		
		public boolean hasFieldsFromAncestor() {
			return fieldsFromAncestor != null && !fieldsFromAncestor.isEmpty(); 
		}
		
		private void addAncestorFieldMapping(String ancestorFieldRef, String currentField) {
			String ancestorField = ancestorFieldRef;
			ancestorField = normalizeAncestorValueRef(ancestorField);
			Mapping ancestor = this;
			while (ancestorField.startsWith(PARENT_MAPPING_REFERENCE_PREFIX_WITHSEPARATOR)) {
				ancestor = ancestor.getParent();
				if (ancestor == null) {
					// User may want this in template declaration
					LOG.debug("Invalid ancestor XML field reference " + ancestorFieldRef + " in mapping of element <" + this.getElement() + ">"); 
					break;
				}
				ancestorField = ancestorField.substring(PARENT_MAPPING_REFERENCE_PREFIX_WITHSEPARATOR.length());
			}
			
			// After the ancestor prefix has been stripped, process the namespace
			ancestorField = createQualifiedName(ancestorField);
			if (ancestor != null) {
				addAncestorField(new AncestorFieldMapping(ancestor, ancestorField, currentField, ancestorFieldRef));
			} else {
				// This AncestorFieldMapping makes sense in templates - invalid ancestor reference may become valid in template reference
				addAncestorField(new AncestorFieldMapping(null, null, currentField, ancestorFieldRef));
			}
		}

		/**
		 * If <code>ancestorField</code> is reference to ancestor element value, returns its normalized
		 * version, otherwise returns unchanged original parameter.
		 * Normalized ancestor field reference always ends with "../.": suffix.
		 * Valid unnormalized ancestor element value references are i.e.: ".." or "../"
		 */
		private String normalizeAncestorValueRef(String ancestorField) {
			if (PARENT_MAPPING_REFERENCE_PREFIX.equals(ancestorField)) {
				return PARENT_MAPPING_REFERENCE_PREFIX_WITHSEPARATOR + ELEMENT_VALUE_REFERENCE;
			}
			
			if (ancestorField.startsWith(PARENT_MAPPING_REFERENCE_PREFIX_WITHSEPARATOR)) {
				if (ancestorField.endsWith(PARENT_MAPPING_REFERENCE_PREFIX)) {
					ancestorField += PARENT_MAPPING_REFERENCE_SEPARATOR + ELEMENT_VALUE_REFERENCE;
				} else if (ancestorField.endsWith(PARENT_MAPPING_REFERENCE_PREFIX_WITHSEPARATOR)) {
					ancestorField += ELEMENT_VALUE_REFERENCE;
				}
			}
			return ancestorField;
		}
		
    }
    
	public static class AncestorFieldMapping {
		final Mapping ancestor;
		final String ancestorField;
		final String currentField;
		final String originalFieldReference;

		public AncestorFieldMapping(Mapping ancestor, String ancestorField, String currentField, String originalFieldReference) {
			this.ancestor = ancestor;
			this.ancestorField = ancestorField;
			this.currentField = currentField;
			this.originalFieldReference = originalFieldReference;
		}
	}
	
	
    /**
     * Constructs an XML Extract node with the given id.
     */
    public XMLExtract(String id) {
        super(id);
    }
    

    /**
     * Creates an inctence of this class from a xml node.
     * @param graph
     * @param xmlElement
     * @return
     * @throws XMLConfigurationException
     */
    public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
        ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
        XMLExtract extract;
        
        try {
        	// constructor
            extract = new XMLExtract(xattribs.getString(XML_ID_ATTRIBUTE));
            
            // set input file
            extract.setInputFile(xattribs.getStringEx(XML_SOURCEURI_ATTRIBUTE,RefResFlag.SPEC_CHARACTERS_OFF));
            
            // set dtd schema
//            if (xattribs.exists(XML_SCHEMA_ATTRIBUTE)) {
//            	extract.setSchemaFile(xattribs.getString(XML_SCHEMA_ATTRIBUTE));
//            }
            
            // if can use nested nodes.
            if(xattribs.exists(XML_USENESTEDNODES_ATTRIBUTE)) {
                extract.setUseNestedNodes(xattribs.getBoolean(XML_USENESTEDNODES_ATTRIBUTE));
            }
            
            // set mapping
            String mappingURL = xattribs.getStringEx(XML_MAPPING_URL_ATTRIBUTE, null,RefResFlag.SPEC_CHARACTERS_OFF);
            String mapping = xattribs.getString(XML_MAPPING_ATTRIBUTE, null);
            NodeList nodes = xmlElement.getChildNodes();
            if (mappingURL != null) extract.setMappingURL(mappingURL);
            else if (mapping != null) extract.setMapping(mapping);
            else if (nodes != null && nodes.getLength() > 0){
                //old-fashioned version of mapping definition
                //mapping xml elements are child nodes of the component
            	extract.setNodes(nodes);
            } else {
            	xattribs.getStringEx(XML_MAPPING_URL_ATTRIBUTE,RefResFlag.SPEC_CHARACTERS_OFF); // throw configuration exception
            }

            // set namespace bindings attribute
            Properties props = null;
			if (xattribs.exists(XML_NAMESPACE_BINDINGS_ATTRIBUTE)) {
				try {
					props = new Properties();
					final String content = xattribs.getString(
							XML_NAMESPACE_BINDINGS_ATTRIBUTE, null);
					if (content != null) {
						props.load(new ByteArrayInputStream(content.getBytes()));
					}
				} catch (IOException e) {
					throw new XMLConfigurationException("Unable to initialize namespace bindings",e);
				}
				
				final HashMap<String,String> namespaceBindings = new HashMap<String,String>();
				for (String name : props.stringPropertyNames()) {
					namespaceBindings.put(name, props.getProperty(name));
				}
				
				extract.setNamespaceBindings(namespaceBindings);
				
			}
            
            // set a skip row attribute
            if (xattribs.exists(XML_SKIP_ROWS_ATTRIBUTE)){
            	extract.setSkipRows(xattribs.getInteger(XML_SKIP_ROWS_ATTRIBUTE));
            }
            
            // set a numRecord attribute
            if (xattribs.exists(XML_NUMRECORDS_ATTRIBUTE)){
            	extract.setNumRecords(xattribs.getInteger(XML_NUMRECORDS_ATTRIBUTE));
            }
            
            if (xattribs.exists(XML_XML_FEATURES_ATTRIBUTE)){
            	extract.setXmlFeatures(xattribs.getString(XML_XML_FEATURES_ATTRIBUTE));
            }
            if (xattribs.exists(XML_VALIDATE_ATTRIBUTE)){
            	extract.setValidate(xattribs.getBoolean(XML_VALIDATE_ATTRIBUTE));
            }
            if (xattribs.exists(XML_CHARSET_ATTRIBUTE)){
            	extract.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE));
            }
            
			if (xattribs.exists(XML_TRIM_ATTRIBUTE)){
				extract.setTrim(xattribs.getBoolean(XML_TRIM_ATTRIBUTE));
			}
            return extract;
        } catch (Exception ex) {
            throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
        }
    }
    
	@Deprecated
    private void setNodes(NodeList nodes) {
    	this.mappingNodes = nodes;
	}


	public void setMappingURL(String mappingURL) {
    	this.mappingURL = mappingURL;
	}


	public void setMapping(String mapping) {
		this.mapping = mapping;
	}


	/**
     * Sets the trim indicator.
     * @param trim
     */
	public void setTrim(boolean trim) {
		this.trim = trim;
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
    
    /**
     * Creates org.w3c.dom.Document object from the given ReadableByteChannel.
     * 
     * @param readableByteChannel
     * @return
     * @throws XMLConfigurationException
     */
    public static Document createDocumentFromChannel(ReadableByteChannel readableByteChannel) throws XMLConfigurationException {
    	DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    	dbf.setCoalescing(true);
        Document doc;
        try {
            doc = dbf.newDocumentBuilder().parse(Channels.newInputStream(readableByteChannel));
        } catch (Exception e) {
            throw new XMLConfigurationException("Mapping parameter parse error occur.", e);
        }
        return doc;
    }
    
    /**
     * Creates mappings.
     * 
     * @param graph
     * @param extract
     * @param parentMapping
     * @param nodeXML
     */
	private List<String> processMappings(TransformationGraph graph, Mapping parentMapping, org.w3c.dom.Node nodeXML) {
		List<String> errors = new LinkedList<String>();
		if (XML_MAPPING.equals(nodeXML.getNodeName())) {
			// for a mapping declaration, process all of the attributes
			// element, outPort, parentKeyName, generatedKey
			ComponentXMLAttributes attributes = new ComponentXMLAttributes((Element) nodeXML, graph);
			Mapping mapping = null;

			if (attributes.exists(XML_TEMPLATE_REF)) {
				// template mapping reference
				String templateId = null;
				try {
					templateId = attributes.getString(XML_TEMPLATE_REF);
				} catch (AttributeNotFoundException e) {
					// this cannot happen (see if above)
					errors.add("Attribute 'templateId' is missing");
					return errors;
				}

				if (!declaredTemplates.containsKey(templateId)) {
					errors.add("Template '" + templateId + "' has not been declared");
					return errors;
				}
				
				mapping = new Mapping(declaredTemplates.get(templateId), parentMapping);
			}

        	// standard mapping declaration
            try {
            	int outputPort = -1;
            	if (attributes.exists(XML_OUTPORT)) {
            		outputPort = attributes.getInteger(XML_OUTPORT);
            	}

            	
            	
            	if (mapping == null) {
            		mapping = new Mapping(
            				createQualifiedName(attributes.getString(XML_ELEMENT)), 
            				outputPort, 
            				parentMapping);
            	} else {
            		if (outputPort != -1) {
	            		mapping.setOutPort(outputPort);
	            		if (attributes.exists(XML_ELEMENT)) {
	            			mapping.setElement(
	            					createQualifiedName(attributes.getString(XML_ELEMENT)));
	            		}
            		}
            	}
            } catch(AttributeNotFoundException ex) {
            	errors.add("Required attribute 'element' missing. Skipping this mapping and all children.");
                return errors;
            }
            
            // Add new root mapping
            if (parentMapping == null) {
                addMapping(mapping);
            }

            boolean parentKeyPresent = false;
            boolean generatedKeyPresent = false;
            if (attributes.exists(XML_PARENTKEY)) {
            	final String[] parentKey = attributes.getString(XML_PARENTKEY, null).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
                mapping.setParentKey(parentKey);
                parentKeyPresent = true;
            }
            
            if (attributes.exists(XML_GENERATEDKEY)) {
            	final String[] generatedKey = attributes.getString(XML_GENERATEDKEY, null).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX); 
                mapping.setGeneratedKey(generatedKey);
                generatedKeyPresent = true;
            }
            
            if (parentKeyPresent != generatedKeyPresent) {
            	errors.add("Mapping for element: " + mapping.getElement() 
                		+ " must either have both 'parentKey' and 'generatedKey' attributes or neither.");
                mapping.setParentKey(null);
                mapping.setGeneratedKey(null);
            }

            if (parentKeyPresent && mapping.getParent() == null) {
            	errors.add("Mapping for element: " + mapping.getElement() 
                		+ " may only have 'parentKey' or 'generatedKey' attributes if it is a nested mapping.");
                mapping.setParentKey(null);
                mapping.setGeneratedKey(null);
            }

            //mapping between xml fields and clover fields initialization
            if (attributes.exists(XML_XMLFIELDS) && attributes.exists(XML_CLOVERFIELDS)) {
                String[] xmlFields = attributes.getString(XML_XMLFIELDS, null).split(Defaults.Component.KEY_FIELDS_DELIMITER);
                String[] cloverFields = attributes.getString(XML_CLOVERFIELDS, null).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
                // TODO add existence check for Clover fields, if possible

                if(xmlFields.length == cloverFields.length){
                    for (int i = 0; i < xmlFields.length; i++) {
                    	if (xmlFields[i].startsWith(PARENT_MAPPING_REFERENCE_PREFIX_WITHSEPARATOR) || xmlFields[i].equals(PARENT_MAPPING_REFERENCE_PREFIX)) {
                    		mapping.addAncestorFieldMapping(xmlFields[i], cloverFields[i]);
                    	} else {
                    		mapping.putXml2CloverFieldMap(xmlFields[i], cloverFields[i]);
                    	}
                    }
                } else {
                	errors.add("Mapping for element: " + mapping.getElement() 
                    		+ " must have same number of the xml fields and the clover fields attribute.");
                }
            }
            
            //sequence field
            if (attributes.exists(XML_SEQUENCEFIELD)) {
                mapping.setSequenceField(attributes.getString(XML_SEQUENCEFIELD, null));
                mapping.setSequenceId(attributes.getString(XML_SEQUENCEID, null));
            }
            
            //skip rows field
            if (attributes.exists(XML_SKIP_ROWS_ATTRIBUTE)) {
                mapping.setSkipRecords4Mapping(attributes.getInteger(XML_SKIP_ROWS_ATTRIBUTE, 0));
            }
            
            //number records field
            if (attributes.exists(XML_NUMRECORDS_ATTRIBUTE)) {
                mapping.setNumRecords4Mapping(attributes.getInteger(XML_NUMRECORDS_ATTRIBUTE, Integer.MAX_VALUE));
            }
            
            // template declaration
            if (attributes.exists(XML_TEMPLATE_ID)) {
            	final String templateId = attributes.getString(XML_TEMPLATE_ID, null);
            	if (declaredTemplates.containsKey(templateId)) {
            		errors.add("Template '" + templateId + "' has duplicate declaration");
            	}
            	declaredTemplates.put(templateId, mapping);
            }
            
            // prepare variables for skip and numRecords for this mapping
        	mapping.prepareProcessSkipOrNumRecords();

        	// multiple nested references of a template
        	if (attributes.exists(XML_TEMPLATE_REF) && attributes.exists(XML_TEMPLATE_DEPTH)) {
				int depth = attributes.getInteger(XML_TEMPLATE_DEPTH, 1) - 1;
				Mapping currentMapping = mapping;
				while (depth > 0) {
					currentMapping = new Mapping(currentMapping, currentMapping);
					currentMapping.prepareProcessSkipOrNumRecords();
					depth--;
				}
				while (currentMapping != mapping) {
					currentMapping.prepareReset4CurrentRecord4Mapping();
					currentMapping = currentMapping.getParent();
				}
        	}
        	
            // Process all nested mappings
            NodeList nodes = nodeXML.getChildNodes();
            for (int i = 0; i < nodes.getLength(); i++) {
                org.w3c.dom.Node node = nodes.item(i);
                errors.addAll(processMappings(graph, mapping, node));
            }
            
            // prepare variable reset of skip and numRecords' attributes
            mapping.prepareReset4CurrentRecord4Mapping();
            
        } else if (nodeXML.getNodeType() == org.w3c.dom.Node.ELEMENT_NODE) {
        	errors.add("Unknown element '" + nodeXML.getNodeName() + "' is ignored with all it's child elements.");
        } // Ignore every other xml element (text values, comments...)
		return errors;
    }


	@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();

		if (firstRun()) {
			// sets input file to readableChannelIterator and sets its settings (directory, charset, input port,...)
	        if (inputFile != null) {
	        	createReadableChannelIterator();
	        	this.readableChannelIterator.init();
	        }
		} else {
			autoFilling.reset();
			this.readableChannelIterator.reset();
		}
		
        if (!readableChannelIterator.isGraphDependentSource()) prepareNextSource();
	}	
	

    
    @Override
    public Result execute() throws Exception {
    	Result result;
    	
    	// parse xml from input file(s).
    	if (parseXML()) {
    		// finished successfully
    		result = runIt ? Result.FINISHED_OK : Result.ABORTED;
    		
    	} else {
    		// an error occurred 
    		result = runIt ? Result.ERROR : Result.ABORTED;
    	}

    	broadcastEOF();
		return result;
    }
    
	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
		//no input channel is closed here - this could be changed in future
	}

    
     /**
     * Parses the inputSource. The SAXHandler defined in this class will handle
     * the rest of the events. Returns false if there was an exception
     * encountered during processing.
     */
    private boolean parseXML() throws JetelException{
    	// create new sax factory
        SAXParserFactory factory = SAXParserFactory.newInstance();
		factory.setValidating(validate);
		factory.setNamespaceAware(true);
		initXmlFeatures(factory);
        SAXParser parser;
        Set<String> xmlAttributes = getXMLMappingValues();
        
        try {
        	// create new sax parser
            parser = factory.newSAXParser();
        } catch (Exception ex) {
        	throw new JetelException(ex.getMessage(), ex);
        }
        
        try {
        	// prepare next source
            if (readableChannelIterator.isGraphDependentSource()) {
                try {
                    if(!nextSource()) return true;
                } catch (JetelException e) {
                    throw new ComponentNotReadyException(e.getMessage()/*"FileURL attribute (" + inputFile + ") doesn't contain valid file url."*/, e);
                }
            }
    		do {
    			if (m_inputSource != null) {
    				// parse the input source
    				parser.parse(m_inputSource, new SAXHandler(xmlAttributes));
    			}
                
                // get a next source
    		} while (nextSource());
    		
        } catch (SAXException ex) {
        	// process error
            if (!runIt) {
                return true; // we were stopped by a stop signal... probably
            }
            LOG.error("XML Extract: " + getId() + " Parse Exception" + ex.getMessage(), ex);
            throw new JetelException("XML Extract: " + getId() + " Parse Exception", ex);
        } catch (Exception ex) {
            LOG.error("XML Extract: " + getId() + " Unexpected Exception", ex);
            throw new JetelException("XML Extract: " + getId() + " Unexpected Exception", ex);
        }
        return true;
    }
    
	private Set<String> getXMLMappingValues() {
		try {
			SAXParser saxParser = SAXParserFactory.newInstance().newSAXParser();
			DefaultHandler handler = new MyHandler();
			InputStream is = null;
			if (this.mappingURL != null) {
				String filePath = FileUtils.getFile(getGraph().getRuntimeContext().getContextURL(), mappingURL);
				is = new FileInputStream(new File(filePath));
			} else if (this.mapping != null) {
				is = new ByteArrayInputStream(mapping.getBytes(charset));
			}
			if (is != null) {
				saxParser.parse(is, handler);
				return ((MyHandler) handler).getCloverAttributes();
			}
		} catch (Exception e) {
			return new HashSet<String>();
		}
		return new HashSet<String>();
	}


	/**
	 * Xml features initialization.
	 * @throws JetelException 
	 */
	private void initXmlFeatures(SAXParserFactory factory) throws JetelException {
		if (xmlFeatures == null) return;
		String[] aXmlFeatures = xmlFeatures.split(FEATURES_DELIMETER);
		String[] aOneFeature;
	    try {
			for (String oneFeature: aXmlFeatures) {
				aOneFeature = oneFeature.split(FEATURES_ASSIGN);
				if (aOneFeature.length != 2) 
					throw new JetelException("The xml feature '" + oneFeature + "' has wrong format");
					factory.setFeature(aOneFeature[0], Boolean.parseBoolean(aOneFeature[1]));
			}
		} catch (Exception e) {
			throw new JetelException(e.getMessage(), e);
		}
	}

    /**
     * Perform sanity checks.
     */
    public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();

		augmentNamespaceURIs();
		
    	TransformationGraph graph = getGraph();
    	URL projectURL = graph != null ? graph.getRuntimeContext().getContextURL() : null;
    	
		// prepare mapping
		if (mappingURL != null) {
			try {
				ReadableByteChannel ch = FileUtils.getReadableChannel(projectURL, mappingURL);
				Document doc = createDocumentFromChannel(ch);
                Element rootElement = doc.getDocumentElement();
                mappingNodes = rootElement.getChildNodes();
			} catch (Exception e) {
				throw new ComponentNotReadyException(e);
			}
		} else if (mapping != null) {
			Document doc;
			try {
				doc = createDocumentFromString(mapping);
			} catch (XMLConfigurationException e) {
				throw new ComponentNotReadyException(e);
			}
			Element rootElement = doc.getDocumentElement();
			mappingNodes = rootElement.getChildNodes();
		}
        //iterate over 'Mapping' elements
		declaredTemplates.clear();
		String errorPrefix = getId() + ": Mapping error - "; 
        for (int i = 0; i < mappingNodes.getLength(); i++) {
            org.w3c.dom.Node node = mappingNodes.item(i);
            List<String> errors = processMappings(graph, null, node);
            for (String error : errors) {
            	LOG.warn(errorPrefix + error);
			}
        }
		
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
    
    /**
     * Augments the namespaceURIs with curly brackets to allow easy creation of qualified names
     * E.g. xmlns:mov="http://www.javlin.eu/mov"
     * URI = "http://www.javlin.eu";
     * Augmented URI = "{http://www.javlin.eu}";
     */
    private void augmentNamespaceURIs() {
    	for (String prefix : namespaceBindings.keySet()) {
    		String uri = namespaceBindings.get(prefix);
    		namespaceBindings.put(prefix, augmentURI(uri));
    	}
    }
	
    private String augmentURI(String uri) {
    	if (uri == null) {
    		return null;
    	}
    	
    	return "{" + uri + "}";
    }
    
    private void createReadableChannelIterator() throws ComponentNotReadyException {
    	TransformationGraph graph = getGraph();
    	URL projectURL = graph != null ? graph.getRuntimeContext().getContextURL() : null;
    	
    	this.readableChannelIterator = new ReadableChannelIterator(
    			getInputPort(INPUT_PORT), 
    			projectURL,
    			inputFile);
    	this.readableChannelIterator.setCharset(charset);
    	this.readableChannelIterator.setPropertyRefResolver(new PropertyRefResolver(graph.getGraphProperties()));
    	this.readableChannelIterator.setDictionary(graph.getDictionary());
    }
	
	/**
	 * Prepares a next source.
	 * @throws ComponentNotReadyException
	 */
	private void prepareNextSource() throws ComponentNotReadyException {
        try {
            if(!nextSource()) {
                //throw new ComponentNotReadyException("FileURL attribute (" + inputFile + ") doesn't contain valid file url.");
            }
        } catch (JetelException e) {
            throw new ComponentNotReadyException(e.getMessage()/*"FileURL attribute (" + inputFile + ") doesn't contain valid file url."*/, e);
        }
	}

	/**
     * Switch to the next source file.
	 * @return
	 * @throws JetelException 
	 */
	private boolean nextSource() throws JetelException {
		ReadableByteChannel stream = null; 
		while (readableChannelIterator.hasNext()) {
			autoFilling.resetSourceCounter();
			autoFilling.resetGlobalSourceCounter();
			stream = readableChannelIterator.next();
			if (stream == null) continue; // if record no record found
			autoFilling.setFilename(readableChannelIterator.getCurrentFileName());
			long fileSize = 0;
			Date fileTimestamp = null;
			if (FileUtils.isLocalFile(autoFilling.getFilename()) && !readableChannelIterator.isGraphDependentSource()) {
				File tmpFile = new File(autoFilling.getFilename());
				long timestamp = tmpFile.lastModified();
				fileTimestamp = timestamp == 0 ? null : new Date(timestamp);
				fileSize = tmpFile.length();
			}
			autoFilling.setFileSize(fileSize);
			autoFilling.setFileTimestamp(fileTimestamp);				
			m_inputSource = new InputSource(Channels.newInputStream(stream));
			return true;
		}
        readableChannelIterator.blankRead();
		return false;
	}

	/**
	 * Expands a prefixed element or attribute name to a universal name.
	 * I.e. the namespace prefix is replaced by augmented URI.
	 * The URIs are taken from the {@link XMLExtract#namespaceBindings} 
	 * 
	 * @param prefixedName XML element or attribute name e.g. <code>mov:movies</code>
	 * 
	 * @return Universal XML name in the form: <code>{http://www.javlin.eu/movies}title</code>
	 */
	private String createQualifiedName(String prefixedName) {
		if (prefixedName == null || prefixedName.isEmpty()) {
			return prefixedName;
		}
		
		// check if universal XML name exists
		int indexOfOpenBracket = prefixedName.indexOf("{");
		if (-1<indexOfOpenBracket && indexOfOpenBracket<prefixedName.indexOf("}")) {
			return prefixedName;
		}
		
		final String[] parsed = prefixedName.split(":");
		
		if (parsed.length < 2) {
			return "{}" + parsed[0];
		}
		
		/*
		 * Prefixed element:
		 * Get the URI (already in Clark's notation) and use it to create qualified name
		 */
		String namespaceURI = namespaceBindings.get(parsed[0]);
		namespaceURI = namespaceURI == null ? "{}" : namespaceURI;
		return namespaceURI + parsed[1];
	}
	
	private String[] createQualifiedName(String[] prefixedNames) {
		final String[] result = new String[prefixedNames.length];
		for (int i = 0; i<prefixedNames.length; i++) {
			result[i] = createQualifiedName(prefixedNames[i]);
		}
		return result;
	}
	
    public String getType() {
        return COMPONENT_TYPE;
    }
    
    private void checkUniqueness(ConfigurationStatus status, Mapping mapping) {
    	if (mapping.getOutRecord() == null) {
    		return;
    	}
		new UniqueLabelsValidator(status, this).validateMetadata(mapping.getOutRecord().getMetadata());
		if (mapping.getChildMap() != null) {
			for (Mapping child: mapping.getChildMap().values()) {
				checkUniqueness(status, child);
			}
		}
    }
    
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {

    	if (charset != null && !Charset.isSupported(charset)) {
        	status.add(new ConfigurationProblem(
            		"Charset "+charset+" not supported!", 
            		ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
        }

    	TransformationGraph graph = getGraph();
    	//Check whether XML mapping schema is valid
		try {
			SAXParserFactory factory = SAXParserFactory.newInstance();
			SAXParser saxParser = factory.newSAXParser();
			DefaultHandler handler = new MyHandler();
			InputSource is = null;
			Document doc = null;
			if (this.mappingURL != null) {
				String filePath = FileUtils.getFile(graph.getRuntimeContext().getContextURL(), mappingURL);
				is = new InputSource(new FileInputStream(new File(filePath)));
				ReadableByteChannel ch = FileUtils.getReadableChannel(
						graph != null ? graph.getRuntimeContext().getContextURL() : null, mappingURL);
				doc = createDocumentFromChannel(ch);
			} else if (this.mapping != null) {
				// inlined mapping
				// don't use the charset of the component's input files, but the charset of the .grf file
		        is = new InputSource(new StringReader(mapping));
				doc = createDocumentFromString(mapping);
	        }
			if (is != null) {
				saxParser.parse(is, handler);
				Set<String> attributesNames = ((MyHandler) handler).getAttributeNames();
				for (String attributeName : attributesNames) {
					if (!isXMLAttribute(attributeName)) {
						status.add(new ConfigurationProblem("Can't resolve XML attribute: " + attributeName, Severity.WARNING, this, Priority.NORMAL));
					}
				}
			}
			if (doc != null) {
				Element rootElement = doc.getDocumentElement();
				mappingNodes = rootElement.getChildNodes();
				 
		        for (int i = 0; i < mappingNodes.getLength(); i++) {
		            org.w3c.dom.Node node = mappingNodes.item(i);
		            List<String> errors = processMappings(graph, null, node);
		            ConfigurationProblem problem;
		            for (String error : errors) {
		            	problem = new ConfigurationProblem("Mapping error - " + error, Severity.WARNING, this, Priority.NORMAL);
		                status.add(problem);
					}
		        }
			}
		} catch (Exception e) {
			status.add(new ConfigurationProblem("Can't parse XML mapping schema. Reason: " + e.getMessage(), Severity.ERROR, this, Priority.NORMAL));
		} finally {
			declaredTemplates.clear();
		}
		
		// TODO Labels:
		//for (Mapping mapping: getMappings().values()) {
		//	checkUniqueness(status, mapping);
		//}
		// TODO Labels end
		
        try { 
            // check inputs
        	if (inputFile != null) {
        		createReadableChannelIterator();
        		this.readableChannelIterator.checkConfig();
        		
            	URL contextURL = graph != null ? graph.getRuntimeContext().getContextURL() : null;
        		String fName = null; 
        		Iterator<String> fit = readableChannelIterator.getFileIterator();
        		while (fit.hasNext()) {
        			try {
        				fName = fit.next();
        				if (fName.equals("-")) continue;
        				if (fName.startsWith("dict:")) continue; //this test has to be here, since an involuntary warning is caused
        				String mostInnerFile = FileURLParser.getMostInnerAddress(fName);
        				URL url = FileUtils.getFileURL(contextURL, mostInnerFile);
        				if (FileUtils.isServerURL(url)) {
        					//FileUtils.checkServer(url); //this is very long operation
        					continue;
        				}
        				if (FileURLParser.isArchiveURL(fName)) {
        					// test if the archive file exists
        					// getReadableChannel is too long for archives
        					String path = url.getRef() != null ? url.getFile() + "#" + url.getRef() : url.getFile();
        					if (new File(path).exists()) continue;
        					throw new ComponentNotReadyException("File is unreachable: " + fName);
        				}
        				FileUtils.getReadableChannel(contextURL, fName).close();
        			} catch (IOException e) {
        				throw new ComponentNotReadyException("File is unreachable: " + fName, e);
        			} catch (ComponentNotReadyException e) {
        				throw new ComponentNotReadyException("File is unreachable: " + fName, e);
        			}
        		}
        	}
		} catch (ComponentNotReadyException e) {
            ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.WARNING, this, ConfigurationStatus.Priority.NORMAL);
            if(!StringUtils.isEmpty(e.getAttributeName())) {
                problem.setAttributeName(e.getAttributeName());
            }
            status.add(problem);
        } finally {
        	free();
        }
    	
        //TODO
        return status;
    }
    
    private static class MyHandler extends DefaultHandler { 
		//Handler used at checkConfig to parse XML mapping and retrieve attributes names
		private Set<String> attributeNames = new HashSet<String>();
		private Set<String> cloverAttributes = new HashSet<String>();
		
		public void startElement(String namespaceURI, String localName, String qName, Attributes atts) { 
			int length = atts.getLength(); 
			for (int i=0; i<length; i++) {
				String xmlField = atts.getQName(i);
				attributeNames.add(xmlField);
				if (xmlField.equals("cloverFields")) {
					cloverAttributes.add(atts.getValue(i));
				}
			}
		}
		
		public Set<String> getAttributeNames() {
			return attributeNames;
		}
		
		public Set<String> getCloverAttributes() {
			return cloverAttributes;
		}
	}
	
	private boolean isXMLAttribute(String attribute) {
		//returns true if given attribute is known XML attribute
		if (attribute.equals(XML_ELEMENT) ||
				attribute.equals(XML_OUTPORT) ||
				attribute.equals(XML_PARENTKEY) ||
				attribute.equals(XML_GENERATEDKEY) ||
				attribute.equals(XML_XMLFIELDS) ||
				attribute.equals(XML_CLOVERFIELDS) ||
				attribute.equals(XML_SEQUENCEFIELD) ||
				attribute.equals(XML_SEQUENCEID) ||
				attribute.equals(XML_SKIP_ROWS_ATTRIBUTE) ||
				attribute.equals(XML_NUMRECORDS_ATTRIBUTE) ||
				attribute.equals(XML_TRIM_ATTRIBUTE) ||
				attribute.equals(XML_VALIDATE_ATTRIBUTE) ||
				attribute.equals(XML_XML_FEATURES_ATTRIBUTE) ||
				attribute.equals(XML_TEMPLATE_ID) ||
				attribute.equals(XML_TEMPLATE_REF) ||
				attribute.equals(XML_TEMPLATE_DEPTH)) {
			return true;
		}
		
		return false;
	}
    
    public org.w3c.dom.Node toXML() {
        return null;
    }
    
    /**
     * Set the input source containing the XML this will parse.
     */
    public void setInputSource(InputSource inputSource) {
        m_inputSource = inputSource;
    }
    
    /**
     * Sets an input file.
     * @param inputFile
     */
    public void setInputFile(String inputFile) {
    	this.inputFile = inputFile;
    }
    
    /**
     * 
     * @param useNestedNodes
     */
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
    public Map<String,Mapping> getMappings() {
        // return Collections.unmodifiableMap(m_elementPortMap); // return a
        // read-only map
        return m_elementPortMap;
    }

    /**
     * Sets skipRows - how many elements to skip.
     * @param skipRows
     */
    public void setSkipRows(int skipRows) {
        this.skipRows = skipRows;
    }
    
    /**
     * Sets numRecords - how many elements to process.
     * @param numRecords
     */
    public void setNumRecords(int numRecords) {
        this.numRecords = Math.max(numRecords, 0);
    }

    /**
     * Sets the xml feature.
     * @param xmlFeatures
     */
    public void setXmlFeatures(String xmlFeatures) {
    	this.xmlFeatures = xmlFeatures;
	}

    /**
     * Sets validation option.
     * @param validate
     */
    public void setValidate(boolean validate) {
    	this.validate = validate;
	}
    
    /**
     * Sets charset for dictionary and input port reading.
     * @param string
     */
    public void setCharset(String charset) {
    	this.charset = charset;
	}
    
    /**
     * Sets namespace bindings to allow processing that relate namespace prefix used in Mapping
     * and namespace URI used in processed XML document
	 * @param namespaceBindings the namespaceBindings to set
	 */
	public void setNamespaceBindings(HashMap<String, String> namespaceBindings) {
		this.namespaceBindings = namespaceBindings;
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
