/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
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

package org.jetel.graph.dictionary;

//import org.w3c.dom.Node;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.plugin.Extension;
import org.jetel.plugin.PluginDescriptor;
import org.jetel.plugin.Plugins;

/**
 * Factory class for dictionary entry providers.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 10.3.2008
 */
public class DictionaryEntryFactory {

    private static Log logger = LogFactory.getLog(DictionaryEntryFactory.class);

	private final static Map<String, DictionaryEntryDescription> dictionaryEntryMap = new HashMap<String, DictionaryEntryDescription>();
	private final static Map<String, DictionaryEntryProvider> dictionaryEntryProviderMap = new HashMap<String, DictionaryEntryProvider>();
	
	public static void init() {
        //ask plugin framework for components
        List<Extension> dictionaryEntryExtensions = Plugins.getExtensions(DictionaryEntryDescription.EXTENSION_POINT_ID);
        
        //register all components
        for(Extension extension : dictionaryEntryExtensions) {
            try {
                registerDictionaryEntry(new DictionaryEntryDescription(extension));
            } catch(Exception e) {
                logger.error("Cannot create component description, extension in plugin manifest is not valid.\n"
                        + "pluginId = " + extension.getPlugin().getId() + "\n" + extension + "\nReason: " + e.getMessage());
            }
        }
        
	}
	
    public final static void registerDictionaryEntry(DictionaryEntryDescription[] dictionaryEntries) {
        for(int i = 0; i < dictionaryEntries.length; i++) {
        	registerDictionaryEntry(dictionaryEntries[i]);
        }
    }
	
	public final static void registerDictionaryEntry(DictionaryEntryDescription dictionaryEntry){
		dictionaryEntryMap.put(dictionaryEntry.getType(), dictionaryEntry);
	}
	
    
    /**
     * @param dictionaryEntryType
     * @return class from the given dictionary entry type
     */
    private final static Class<?> getDictionaryEntryClass(String dictionaryEntryType) {
        String className = null;
        DictionaryEntryDescription dictionaryEntryDescription = dictionaryEntryMap.get(dictionaryEntryType);
        
        try {
            if(dictionaryEntryDescription == null) { 
                //unknown dictionary entry type, we suppose dictionaryEntryType as full class name classification
                className = dictionaryEntryType;
                //find class of dictionary entry
                return Class.forName(dictionaryEntryType); 
            } else {
                className = dictionaryEntryDescription.getClassName();

                PluginDescriptor pluginDescriptor = dictionaryEntryDescription.getPluginDescriptor();
                
                //find class of dictionary entry
                return Class.forName(className, true, pluginDescriptor.getClassLoader());
            }
        } catch(ClassNotFoundException ex) {
            logger.error("Unknown dictionary entry type: " + dictionaryEntryType + " class: " + className);
            throw new RuntimeException("Unknown dictionary entry type: " + dictionaryEntryType + " class: " + className);
        } catch(Exception ex) {
            logger.error("Unknown dictionary entry type: " + dictionaryEntryType);
            throw new RuntimeException("Unknown dictionary entry type: " + dictionaryEntryType);
        }

    }
    
	/**
	 *  Method for creating various types of dictionary entries.<br>
	 *  If dictionary entry type is not registered, it tries to use dictionaryEntryType parameter directly as a class name.
	 *  
	 * @param  dictionaryEntryType  Type of the dictionary entry
	 * @return                		requested dictionary entry provider object or null if creation failed 
	 */
	public final static DictionaryEntryProvider getDictionaryEntryProvider(String dictionaryEntryType) {
		DictionaryEntryProvider entryProvider = dictionaryEntryProviderMap.get(dictionaryEntryType);
		if(entryProvider != null) {
			return entryProvider;
		}
		//provider with the given type wasn't already instantiated
		Class<?> tClass = getDictionaryEntryClass(dictionaryEntryType);
        
		//create class of dictionary entry provider
		Object entryProviderObject;
		try {
			entryProviderObject = tClass.newInstance();
		} catch (InstantiationException e) {
			logger.error("Can't create object of type " + dictionaryEntryType + " with reason: " + e.getMessage());
			throw new RuntimeException("Can't create object of type " + dictionaryEntryType + " with reason: " + e.getMessage());
		} catch (IllegalAccessException e) {
			logger.error("Can't create object of type " + dictionaryEntryType + " with reason: " + e.getMessage());
          	throw new RuntimeException("Can't create object of type " + dictionaryEntryType + " with reason: " + e.getMessage());
		} catch(Exception ex) {
			logger.error("Can't create object of : " + dictionaryEntryType + " exception: " + ex);
			throw new RuntimeException("Can't create object of : " + dictionaryEntryType + " exception: " + ex);
		}
		if(!(entryProviderObject instanceof DictionaryEntryProvider)) {
            logger.error("Can't create object of type " + dictionaryEntryType + " with reason: '" + tClass.getCanonicalName() + "' is not instance of the DictionaryEntryProvider interface.");
            throw new RuntimeException("Can't create object of type " + dictionaryEntryType + " with reason: '" + tClass.getCanonicalName() + "' is not instance of the DictionaryEntryProvider interface.");
		}
		dictionaryEntryProviderMap.put(dictionaryEntryType, (DictionaryEntryProvider) entryProviderObject);
		return (DictionaryEntryProvider) entryProviderObject;
	}
    
}


