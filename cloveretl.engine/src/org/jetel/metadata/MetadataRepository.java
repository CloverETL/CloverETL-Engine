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
import org.jetel.graph.Node;
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

	/**
	 * This map contains metadata registered in all plugin.xml 
	 */
	private static Map<String, DataRecordMetadata> registeredMetadata = new HashMap<String, DataRecordMetadata>();

	/**
	 * This map contains provided metadata by this repository, where registered metadata are duplicated
	 * with updated identifier according requested component type, see {@link #getMetadata(String, String)}.
	 */
	private static Map<String, DataRecordMetadata> metadataCache = new HashMap<String, DataRecordMetadata>();
	
	/**
	 * Loads all metadata from all plugin.xml
	 */
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
	
	private static void registerMetadata(DataRecordMetadata metadata) {
		if (!StringUtils.isEmpty(metadata.getId())) {
			if (!registeredMetadata.containsKey(metadata.getId())) {
				registeredMetadata.put(metadata.getId(), metadata);
			} else {
				throw new JetelRuntimeException("Metadata with ID '" + metadata.getId() + "' are already registered in repository."); 
			}
		} else {
			throw new JetelRuntimeException("Metadata with no ID cannot be added into repository.");
		}
	}

	private static DataRecordMetadata getRegisteredMetadata(String registeredMetadataId) {
		return registeredMetadata.get(registeredMetadataId);
	}
	
	/**
	 * @param metadataId identifier of registered metadata
	 * @param component associated component
	 * @return metadata instance associated with given id and component type.
	 */
	public static DataRecordMetadata getMetadata(String metadataId, Node component) {
		String componentType = component.getType();
		String componentName = component.getDescriptor().getDescription() != null ? component.getDescriptor().getDescription().getName() : componentType;
		String resultedMetadataId = getMetadataId(metadataId, componentType);
		if (metadataCache.containsKey(resultedMetadataId)) {
			return metadataCache.get(resultedMetadataId);
		} else {
			DataRecordMetadata registeredMetadata = getRegisteredMetadata(metadataId);
			if (registeredMetadata != null) {
				DataRecordMetadata metadata = registeredMetadata.duplicate();
				metadata.setId(resultedMetadataId);
				metadata.setName(getMetadataName(metadata.getName(), componentName));
				metadataCache.put(resultedMetadataId, metadata);
				return metadata;
			} else {
				return null;
			}
		}
	}
	
	private static String getMetadataId(String registeredMetadataId, String componentType) {
		return "__static_metadata_" + componentType + "_" + registeredMetadataId;
	}
	
	private static String getMetadataName(String rawMetadataId, String componentType) {
		return componentType + "_" + rawMetadataId;
	}
	
	/**
	 * @return all registered metadata
	 */
	public static List<DataRecordMetadata> getAllRegisteredMetadata() {
		return new ArrayList<DataRecordMetadata>(registeredMetadata.values());
	}
	
}
