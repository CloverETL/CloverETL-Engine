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
package org.jetel.ctl.extensions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.plugin.Extension;
import org.jetel.plugin.Plugins;

/**
 * This class provides access to TL functions stored in engine plugins.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 30.5.2007
 */
public class TLFunctionPluginRepository {

    private static Log logger = LogFactory.getLog(TLFunctionPluginRepository.class);

    private static List<TLFunctionLibraryDescription> functionLibraries = new ArrayList<TLFunctionLibraryDescription>();

    /** Consolidated map of functions where overloaded functions from under different libraries are kept under the same key */
    private static Map<String,List<TLFunctionDescriptor>> consolidatedFunctions = new TreeMap<String, List<TLFunctionDescriptor>>();
    
    private TLFunctionPluginRepository() {
        //private constructor - this class is not intended to instantiate
    }
    
    public static void init() {
        //ask plugin framework for all libraries extensions
        List<Extension> tlfunctionExtensions = Plugins.getExtensions(TLFunctionLibraryDescription.EXTENSION_POINT_ID);
      
        //register all function libraries
        for(Extension extension : tlfunctionExtensions) {
            try {
            	TLFunctionLibraryDescription description = new TLFunctionLibraryDescription(extension);
            	description.init();
                registerFunctionLibrary(description);
            } catch(Exception e) {
                logger.error("Cannot create TL function description, extension in plugin manifest is not valid.\n"
                        + "pluginId = " + extension.getPlugin().getId() + "\n" + extension, e);
            }
        }

    }
  
	public static void registerFunctionLibrary(TLFunctionLibraryDescription functionLibrary) {
		functionLibrary.getFunctionLibrary().init();
		functionLibraries.add(functionLibrary);

		Map<String, List<TLFunctionDescriptor>> contents = functionLibrary.getAllFunctions();
		for (String name : contents.keySet()) {
			// we want to have own lists because we will merge functions from all libraries
			List<TLFunctionDescriptor> value = consolidatedFunctions.get(name);
			if (value == null) {
				value = new LinkedList<TLFunctionDescriptor>();
				consolidatedFunctions.put(name, value);
			}
			value.addAll(contents.get(name));
		}
	}

    /**
     * Method to retrieve all functions declared within all accessible libraries.
     * Consolidates the functions i.e. if a functions is overloaded within several 
     * libraries, all variants (from all libraries) are stored under the function name key.
     * 
     * @return	consolidated map of function to the descriptor
     */
	public static Map<String,List<TLFunctionDescriptor>> getAllFunctions() {
		return Collections.unmodifiableMap(consolidatedFunctions);
	}

}
