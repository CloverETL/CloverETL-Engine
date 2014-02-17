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
package org.jetel.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jetel.exception.JetelRuntimeException;
import org.jetel.plugin.Extension;
import org.jetel.plugin.Plugins;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * This is repository of statically defined metadata via plugin extension point "metadata".
 * These metadata are referenced by component's descriptors. For example UniversalDataReader
 * has statically defined metadata for second error output port.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 1. 10. 2013
 */
public class MetadataRepository {

    public final static String EXTENSION_POINT_ID = "metadata";

	public static Map<String, DataRecordMetadata> metadataRepository = new HashMap<String, DataRecordMetadata>();
	
	public static void init() {
        //ask plugin framework for metadata
        List<Extension> extensions = Plugins.getExtensions(EXTENSION_POINT_ID);
        //register all metadata
        for (Extension extension : extensions) {
        	NodeList xmlContent = extension.getXMLDefinition().getElementsByTagName("Metadata");
        	if (xmlContent.getLength() == 1) {
	        	DataRecordMetadata metadata = DataRecordMetadataXMLReaderWriter.read((Element) xmlContent.item(0));
	            registerMetadata(metadata);
        	} else {
        		throw new JetelRuntimeException("Invalid metadata extension point definition. Missing 'Metadata' element.");
        	}
        }
	}
	
	public static void registerMetadata(DataRecordMetadata metadata) {
		if (!StringUtils.isEmpty(metadata.getId())) {
			if (!metadataRepository.containsKey(metadata.getId())) {
				metadataRepository.put(metadata.getId(), metadata);
			} else {
				throw new JetelRuntimeException("Metadata with ID '" + metadata.getId() + "' are already registered in repository."); 
			}
		} else {
			throw new JetelRuntimeException("Metadata with no ID cannot be added into repository.");
		}
	}

	public static boolean contains(String metadataId) {
		return metadataRepository.containsKey(metadataId);
	}

	/**
	 * @param metadataId
	 * @return
	 */
	public static DataRecordMetadata getMetadata(String metadataId) {
		return metadataRepository.get(metadataId);
	}
	
	public static List<DataRecordMetadata> getAllMetadata() {
		return new ArrayList<DataRecordMetadata>(metadataRepository.values());
	}
	
}
