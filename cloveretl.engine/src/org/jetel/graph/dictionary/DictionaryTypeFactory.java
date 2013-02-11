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
package org.jetel.graph.dictionary;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.plugin.Extension;
import org.jetel.plugin.PluginDescriptor;
import org.jetel.plugin.Plugins;

/**
 * Factory class for dictionary entry types.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 10.3.2008
 */
public class DictionaryTypeFactory {

    private static Log logger = LogFactory.getLog(DictionaryTypeFactory.class);

	private final static Map<String, DictionaryTypeDescription> dictionaryTypeDescriptionMap = new HashMap<String, DictionaryTypeDescription>();
	
	public static void init() {
        //ask plugin framework for dictionary types
        List<Extension> dictionaryTypesExtensions = Plugins.getExtensions(DictionaryTypeDescription.EXTENSION_POINT_ID);
        
        //register all dictionary types
        for(Extension extension : dictionaryTypesExtensions) {
            try {
            	DictionaryTypeDescription description = new DictionaryTypeDescription(extension);
            	description.init();
                registerDictionaryEntry(description);
            } catch(Exception e) {
                logger.error("Cannot create dictionary type description, extension in plugin manifest is not valid.\n"
                        + "pluginId = " + extension.getPlugin().getId() + "\n" + extension + "\nReason: " + e.getMessage());
            }
        }
	}
	
    public final static void registerDictionaryType(DictionaryTypeDescription[] dictionaryTypes) {
        for(int i = 0; i < dictionaryTypes.length; i++) {
        	registerDictionaryEntry(dictionaryTypes[i]);
        }
    }
	
	public final static void registerDictionaryEntry(DictionaryTypeDescription dictionaryType){
		dictionaryTypeDescriptionMap.put(dictionaryType.getType(), dictionaryType);
	}
	
    /**
     * @return class from the given dictionary entry type
     */
    private final static Class<?> getDictionaryTypeClass(String dictionaryType) {
        String className = null;
        DictionaryTypeDescription dictionaryTypeDescription = dictionaryTypeDescriptionMap.get(dictionaryType);
        
        try {
            if(dictionaryTypeDescription == null) { 
                //unknown dictionary entry type, we suppose dictionaryEntryType as full class name classification
                className = dictionaryType;
                //find class of dictionary entry
                return Class.forName(dictionaryType); 
            } else {
                className = dictionaryTypeDescription.getClassName();

                PluginDescriptor pluginDescriptor = dictionaryTypeDescription.getPluginDescriptor();
                
                //find class of dictionary entry
                return Class.forName(className, true, pluginDescriptor.getClassLoader());
            }
        } catch(ClassNotFoundException ex) {
            throw new RuntimeException("Unknown dictionary entry type: " + dictionaryType + " class: " + className, ex);
        } catch(Exception ex) {
            throw new RuntimeException("Unknown dictionary entry type: " + dictionaryType, ex);
        }
    }
    
	/**
	 *  Method for creating various types of dictionary entries.<br>
	 *  If dictionary entry type is not registered, it tries to use dictionaryEntryType parameter directly as a class name.
	 *  
	 * @param  dictionaryEntryType  Type of the dictionary entry
	 * @return                		requested dictionary entry provider object or null if creation failed 
	 */
	public final static IDictionaryType getDictionaryType(String dictionaryType) {
		//provider with the given type wasn't already instantiated
		Class<?> tClass = getDictionaryTypeClass(dictionaryType);
        
		//create class of dictionary entry provider
		Object entryProviderObject;
		try {
			entryProviderObject = tClass.newInstance();
		} catch (Exception ex) {
			throw new RuntimeException("Can't create dictionary entry type " + dictionaryType, ex);
		}
		if(!(entryProviderObject instanceof IDictionaryType)) {
            throw new RuntimeException("Can't create dictionary entry type " + dictionaryType + " with reason: '" + tClass.getCanonicalName() + "' is not instance of the DictionaryEntryProvider interface.");
		}
		return (IDictionaryType) entryProviderObject;
	}
    
	final static public Map<String, DictionaryTypeDescription> getDictionaryTypes() {
		return Collections.unmodifiableMap(dictionaryTypeDescriptionMap);
	}
}


