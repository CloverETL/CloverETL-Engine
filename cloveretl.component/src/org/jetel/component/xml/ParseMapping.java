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

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.sequence.Sequence;
import org.jetel.graph.TransformationGraph;
import org.jetel.sequence.PrimitiveSequence;
import org.jetel.util.string.StringUtils;

/**
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 10.11.2009
 *
 * NOTE: almost hard copy from XMLExtract - has to be re-developed!!!
 */
public class ParseMapping {
	
    private static final Log logger = LogFactory.getLog(ParseMapping.class);

    private String m_element;								// name of an element for this mapping
    private int m_outPort;									// output port number
    private DataRecord m_outRecord;							// output record
    private String[] m_parentKey;							// parent keys
    private String[] m_generatedKey;						// generated keys
    private Map<String, ParseMapping> m_childMap;				// direct children for this mapping 
    private WeakReference<ParseMapping> m_parent;				// direct parent mapping
    private int m_level;									// original xml tree level (a deep of this element) 
    private String m_sequenceField;							// sequence field
    private String m_sequenceId;							// sequence ID
    private Sequence sequence;								// sequence (Simple, Db,..)
    
    // mapping - xml name -> clover field name
    private Map<String, String> xml2CloverFieldsMap = new HashMap<String, String>();
    
    // for skip and number a record attribute for this mapping
    private int skipRecords4Mapping;						// skip records
    private int numRecords4Mapping = Integer.MAX_VALUE;		// number records
    private int currentRecord4Mapping;						// record counter for this mapping
    private boolean processSkipOrNumRecords;				// what xml element can be skiped
    private boolean bDoMap = true;							// should I skip an xml element? depends on processSkipOrNumRecords
    private boolean bReset4CurrentRecord4Mapping;			// should I reset submappings?
    
    /*
     * Minimally required information.
     */
    public ParseMapping(String element, int outPort) {
        m_element = element;
        m_outPort = outPort;
    }
    
	/**
     * Gives the optional attributes parentKey and generatedKey.
     */
    public ParseMapping(String element, int outPort, String parentKey[], String[] generatedKey) {
        this(element, outPort);
        
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
    public Map<String, ParseMapping> getChildMap() {
        return m_childMap;
    }
    
    /**
     * Gets direct children for this mapping. 
     * @param element
     * @return
     */
    public ParseMapping getChildMapping(String element) {
        if (m_childMap == null) {
            return null;
        }
        return m_childMap.get(element);
    }
    
    /**
     * Adds a direct child for this mapping.
     * @param mapping
     */
    public void addChildMapping(ParseMapping mapping) {
        if (m_childMap == null) {
            m_childMap = new HashMap<String, ParseMapping>();
        }
        m_childMap.put(mapping.getElement(), mapping);
    }
    
    /**
     * Removes a direct child for this mapping.
     * @param mapping
     */
    public void removeChildMapping(ParseMapping mapping) {
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
     * @return
     */
    public Map<String, String> getXml2CloverFieldsMap() {
        return xml2CloverFieldsMap;
    }
    
    /**
     * Sets mapping - xml name -> clover field name
     * @param xml2CloverFieldsMap
     */
    public void setXml2CloverFieldsMap(Map<String, String> xml2CloverFieldsMap) {
        this.xml2CloverFieldsMap = xml2CloverFieldsMap;
    }
    
    /**
     * Gets an output record.
     * @return
     */
    public DataRecord getOutRecord() {
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
    public ParseMapping getParent() {
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
    public void setParent(ParseMapping parent) {
        m_parent = new WeakReference<ParseMapping>(parent);
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
    public Sequence getSequence(TransformationGraph graph) {
        if(sequence == null) {
            String element = StringUtils.trimXmlNamespace(getElement());

            if(getSequenceId() == null) {
                sequence = new PrimitiveSequence(element, graph, element);
            } else {
                sequence = graph.getSequence(getSequenceId());

                if(sequence == null) {
                    logger.warn("Sequence " + getSequenceId() + " does not exist in " + "transformation graph. Primitive sequence is used instead.");
                    sequence = new PrimitiveSequence(element, graph, element);
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
		ParseMapping parent = getParent();
		if (parent == null) {
			return processSkipOrNumRecords;
		}
		return parent.getProcessSkipOrNumRecords();
	}
	
	/**
	 * Sets inner variables for processSkipOrNumRecords.
	 */
	public void prepareProcessSkipOrNumRecords() {
		ParseMapping parentMapping = getParent();
		processSkipOrNumRecords = parentMapping != null && parentMapping.getProcessSkipOrNumRecords() ||
			(skipRecords4Mapping > 0 || numRecords4Mapping < Integer.MAX_VALUE);
	}
	
	/**
	 * Sets inner variables for bReset4CurrentRecord4Mapping.
	 */
	public void prepareReset4CurrentRecord4Mapping() {
		bReset4CurrentRecord4Mapping = processSkipOrNumRecords;
    	if (m_childMap != null) {
    		ParseMapping mapping;
    		for (Iterator<Entry<String, ParseMapping>> it=m_childMap.entrySet().iterator(); it.hasNext();) {
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
    
//	/**
//	 * skipRecords for this mapping.
//	 * @param skipRecords4Mapping
//	 */
//    public void setSkipSourceRecords4Mapping(int skipSourceRecords4Mapping) {
//    	this.skipSourceRecords4Mapping = skipSourceRecords4Mapping;
//    }
//    
//    /**
//     * numRecords for this mapping.
//     * @param numRecords4Mapping
//     */
//    public void setNumSourceRecords4Mapping(int numSourceRecords4Mapping) {
//    	this.numSourceRecords4Mapping = numSourceRecords4Mapping;
//    }
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
    		ParseMapping mapping;
    		for (Iterator<Entry<String, ParseMapping>> it=m_childMap.entrySet().iterator(); it.hasNext();) {
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
		ParseMapping parent = getParent();
    	bDoMap = (parent == null || parent.doMap()) && 
    		currentRecord4Mapping >= skipRecords4Mapping && currentRecord4Mapping-skipRecords4Mapping < numRecords4Mapping;
    	if (m_childMap != null) {
    		ParseMapping mapping;
    		for (Iterator<Entry<String, ParseMapping>> it=m_childMap.entrySet().iterator(); it.hasNext();) {
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
}
