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
package org.jetel.graph.extension;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.HashKey;
import org.jetel.data.RecordKey;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Description of input port mapping and record's relations with records from another ports.
 * Also wrapper for read data records.
 *
 * @author Martin Varecha <martin.varecha@javlinconsulting.cz>
 * (c) JavlinConsulting s.r.o.
 * www.javlinconsulting.cz
 * @created Dec 20, 2007
 */
public class PortDefinition {

    private static final Log logger = LogFactory.getLog(PortDefinition.class);

    public int portIndex = 0;
    /** Comma separated list of columns names, which identify records from this port. Can be null. */
    public String keysAttr = null;
    /** List of columns names, which identify records from this port. Can be null. */
    public String[] keys = null;
    public String keysDeprecatedAttr = null;
    /** Comma separated list of columns from this record which identify parent record. */
    public String keysToParentDeprecatedAttr = null;
    /** Comma separated list of columns from parent record which identify this record. */
    public String keysFromParentDeprecatedAttr = null;
    /** Comma separated list of columns names, which identify records from parent port. Can be null. */
    public String parentKeysAttr = null;
    /** List of columns names, which identify rekeyscords from parent port. Can be null. */
    public String[] parentKeys = null;
    public List<String> relationKeysStrings = new ArrayList<String>();
    public List<String[]> relationKeysArrays = new ArrayList<String[]>();
    /** List of children definitions. */
    public List<PortDefinition> children;
    /** Parent port definition. It's null for root definition. */
    public PortDefinition parent;

    // Map of keyName => recordsMap
    // records may be stored by more different keys
    Map<String, Map<HashKey, TreeRecord>> dataMapsByRelationKeys = new HashMap<String, Map<HashKey, TreeRecord>>();
    public DataRecordMetadata metadata;

    public List<DataRecord> dataRecords = new LinkedList<DataRecord>();
    /** Flag which indicates, that fields will be written as attributes in output XML. */
    public boolean fieldsAsAttributes;
    /** Comma separated list of fields which are exception for flag fieldsAsAttributes.
     *  (if flag is true, fields from this list will be written as elements) */
    public List<String> fieldsAsExcept;
    /** Set of fields which will be kicked out of XML output. */
    public List<String> fieldsIgnore;
    /** Name of element of record in out XMl. May be null, default is "record". */
    public String element = null;
    /** lazy initialized list to simplify processing of XML output. */
    public Integer[] fieldsAsAttributesIndexes;
    /** lazy initialized list to simplify processing of XML output. */
    public Integer[] fieldsAsElementsIndexes;
    /** Pairs of prefix-uri for namespaces */
    public Map<String, String> namespaces;
    /** Optional prefix for attributes or elements. */
    public String fieldsNamespacePrefix;
    /** DefaultNamespace */
    public String defaultNamespace;

    public PortDefinition() {
    }

    @Override
	public String toString() {
        return "PortDefinition#" + portIndex + " key:" + keysAttr + " parentKey:" + parentKeysAttr + " relationKeysStrings:" + relationKeysStrings;
    }

    /** Resets this instance for next execution without graph init. */
    public void reset() {
        dataMapsByRelationKeys = new HashMap<String, Map<HashKey, TreeRecord>>();
    }

    public void addDataRecord(String relationKeysString, String[] relationKeysArray, DataRecord record) {
        RecordKey recKey = new RecordKey(relationKeysArray, metadata);
        HashKey key = new HashKey(recKey, record);
        TreeRecord tr = getTreeRecord(relationKeysString, key);
        if (tr == null) {
            tr = new TreeRecord();
        }
        tr.records.add(record);
        Map<HashKey, TreeRecord> map = dataMapsByRelationKeys.get(relationKeysString);
        if (map == null) {
            map = new HashMap<HashKey, TreeRecord>();
            dataMapsByRelationKeys.put(relationKeysString, map);
        }
        map.put(key, tr);
    }

    public TreeRecord getTreeRecord(String relationKeysString, HashKey key) {
        TreeRecord tr = null;
        Map<HashKey, TreeRecord> map = dataMapsByRelationKeys.get(relationKeysString);
        if (map != null) {
            tr = map.get(key);
        }
        return tr;
    }

    public void setMetadata(DataRecordMetadata metadata) {
        this.metadata = metadata;
    }

    public boolean hasParent() {
        return (parent != null);
    }

    /**
	 * Simple wrapper for single record (unique key) or set of records (not unique key).
	 * @author Martin Varecha <martin.varecha@javlinconsulting.cz>
	 * (c) JavlinConsulting s.r.o.
	 * www.javlinconsulting.cz
	 * @created Dec 11, 2007
	 */
	public static class TreeRecord {

		/** this attribute is set when TreeRecord instance is stored in collection with unique key */
		//public DataRecord record;
		/** this attribute is set when TreeRecord instance is stored in collection with not unique key */
		public List<DataRecord> records = new ArrayList<DataRecord>();
	}
}
