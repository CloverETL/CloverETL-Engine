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
package org.jetel.component.xml;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dom4j.io.SAXContentHandler;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.StringDataField;
import org.jetel.data.sequence.Sequence;
import org.jetel.exception.BadDataFormatException;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.metadata.DataFieldMetadata;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

/**
 * SAX Handler that will dispatch the elements to the different ports.
 * 
 * NOTE: almost hard copy from XMLExtract's inner class - has to be re-developed!!!
 */
public class SAXHandler extends SAXContentHandler {
    
    private static final Log logger = LogFactory.getLog(SAXHandler.class);

	private Node parentNode;
	
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
    private StringBuffer m_characters = new StringBuffer();

    //whole mapping
    private Map<String, ParseMapping> mElementPortMap;
    
    // the active sub-mapping
    private ParseMapping m_activeMapping = null;
    
	// can I use nested nodes for mapping processing?
    private boolean useNestedNodes = true;

	private boolean trim = true;

	public SAXHandler(Node parentNode, Map<String, ParseMapping> mElementPortMap, boolean useNestedNodes, boolean trim) {
		this.parentNode = parentNode;
		this.mElementPortMap = mElementPortMap;
		this.useNestedNodes = useNestedNodes;
		this.trim = trim;
	}
    
    /**
     * @see org.xml.sax.ContentHandler#startElement(java.lang.String, java.lang.String, java.lang.String, org.xml.sax.Attributes)
     */
    @Override
	public void startElement(String prefix, String namespace, String localName, Attributes attributes) throws SAXException {
        m_level++;
        m_grabCharacters = true;
        
        ParseMapping mapping = null;
        if (m_activeMapping == null) {
            mapping = mElementPortMap.get(localName);
        } else {
            mapping = m_activeMapping.getChildMapping(localName);
        }
        if (mapping != null) {
            // We have a match, start converting all child nodes into
            // the DataRecord structure
            m_activeMapping = mapping;
            m_activeMapping.setLevel(m_level);
            
            if (mapping.getOutRecord() == null) {
            	// Former comment was reading:
                	// If it's null that means that there's no edge mapped to
                	// the output port
                	// remove this mapping so we don't repeat this logic (and
                	// logging)
            	// Improved behaviour: (jlehotsky)
            	    // If it's null that means either that there's no edge mapped
            	    // to the output port, or output port is not specified.
            	    // This is OK, we simply ignore the fact and continue.
            	    // Thus the original code is commented out
                /*LOG.warn("XML Extract: " + getId() + " Element ("
                        + localName
                        + ") does not have an edge mapped to that port.");
                if(m_activeMapping.getParent() != null) {
                    m_activeMapping.getParent().removeChildMapping(m_activeMapping);
                    m_activeMapping = m_activeMapping.getParent();
                } else {
                    m_elementPortMap.remove(m_activeMapping);
                    m_activeMapping = null;
                }*/
                
                return;
            }

            //sequence fields initialization
            String sequenceFieldName = m_activeMapping.getSequenceField();
            if(sequenceFieldName != null && m_activeMapping.getOutRecord().hasField(sequenceFieldName)) {
                Sequence sequence = m_activeMapping.getSequence(parentNode.getGraph());
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
            if (parentNode.runIt()) {
                try {
                    DataRecord outRecord = m_activeMapping.getOutRecord();
                    String[] generatedKey = m_activeMapping.getGeneratedKey();
                    String[] parentKey = m_activeMapping.getParentKey();
                    if (parentKey != null) {
                        //if generatedKey is a single array, all parent keys are concatenated into generatedKey field
                        //I know it is ugly code...
                        if(generatedKey.length != parentKey.length && generatedKey.length != 1) {
                            logger.warn("XML Extract Mapping's generatedKey and parentKey attribute has different number of field.");
                            m_activeMapping.setGeneratedKey(null);
                            m_activeMapping.setParentKey(null);
                        } else {
                            for(int i = 0; i < parentKey.length; i++) {
                                boolean existGeneratedKeyField = (outRecord != null) 
                                			&& (generatedKey.length == 1 ? outRecord.hasField(generatedKey[0]) : outRecord.hasField(generatedKey[i]));
                                boolean existParentKeyField = m_activeMapping.getParent().getOutRecord() != null 
                                					&& m_activeMapping.getParent().getOutRecord().hasField(parentKey[i]);
                                if (!existGeneratedKeyField) {
                                    logger.warn("XML Extract Mapping's generatedKey field was not found. "
                                            + (generatedKey.length == 1 ? generatedKey[0] : generatedKey[i]));
                                    m_activeMapping.setGeneratedKey(null);
                                    m_activeMapping.setParentKey(null);
                                } else if (!existParentKeyField) {
                                    logger.warn("XML Extract Mapping's parentKey field was not found. " + parentKey[i]);
                                    m_activeMapping.setGeneratedKey(null);
                                    m_activeMapping.setParentKey(null);
                                } else {
                                	// both outRecord and m_activeMapping.getParrent().getOutRecord are not null
                                	// here, because of if-else if-else chain
                                    DataField generatedKeyField = generatedKey.length == 1 ? outRecord.getField(generatedKey[0]) : outRecord.getField(generatedKey[i]);
                                    DataField parentKeyField = m_activeMapping.getParent().getOutRecord().getField(parentKey[i]);
                                    if(generatedKey.length != parentKey.length) {
                                        if(generatedKeyField.getType() != DataFieldMetadata.STRING_FIELD) {
                                            logger.warn("XML Extract Mapping's generatedKey field has to be String type (keys are concatened to this field).");
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
            } else {
                throw new SAXException("Stop Signaled");
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
                
                if (m_activeMapping.getOutRecord() != null && m_activeMapping.getOutRecord().hasField(attrName)) {
                    m_activeMapping.getOutRecord().getField(attrName).fromString(attributes.getValue(i));
                }
            }
        }
        
        // Regardless of starting element type, reset the length of the buffer and flag
        m_characters.setLength(0);
        m_hasCharacters = false;
    }
    
    /**
     * @see org.xml.sax.ContentHandler#characters(char[], int, int)
     */
    @Override
	public void characters(char[] data, int offset, int length) throws SAXException {
        // Save the characters into the buffer, endElement will store it
        // into the field
        if (m_activeMapping != null && m_grabCharacters) {
            m_characters.append(data, offset, length);
            m_hasCharacters = true;
        }
    }
    
    /**
     * @see org.xml.sax.ContentHandler#endElement(java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
	public void endElement(String prefix, String namespace, String localName) throws SAXException {
        if (m_activeMapping != null) {
            //use fields mapping
            Map<String, String> xml2clover = m_activeMapping.getXml2CloverFieldsMap();
            if(xml2clover != null && xml2clover.containsKey(localName)) {
                localName = xml2clover.get(localName);
            }
            // Store the characters processed by the characters() call back
            //only if we have corresponding output field and we are on the right level or we want to use data from nested unmapped nodes
            if (m_activeMapping.getOutRecord() != null && m_activeMapping.getOutRecord().hasField(localName) 
                    && (useNestedNodes || m_level - 1 <= m_activeMapping.getLevel())) {
                DataField field = m_activeMapping.getOutRecord().getField(localName);
                // If field is nullable and there's no character data set it
                // to null
                if (m_hasCharacters) {
                    try {
                        field.fromString(trim ? m_characters.toString().trim() : m_characters.toString());
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
            if (parentNode.runIt()) {
                try {
                    OutputPort outPort = parentNode.getOutputPort(m_activeMapping.getOutPort());
                    
                    if (outPort != null) {
                        // we just ignore creating output, if port is empty (without metadata) or not specified
                        DataRecord outRecord = m_activeMapping.getOutRecord();
                        
                        // can I do the map? it depends on skip and numRecords.
                        if (m_activeMapping.doMap()) {
                            //send off record
                        	outPort.writeRecord(outRecord);
                        }
//	                                if (m_activeMapping.getParent() == null) autoFilling.incGlobalCounter();
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
    
}
