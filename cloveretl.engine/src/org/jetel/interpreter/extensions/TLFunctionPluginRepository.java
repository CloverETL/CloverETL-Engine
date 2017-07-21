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
package org.jetel.interpreter.extensions;

import java.util.ArrayList;
import java.util.List;

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
                        + "pluginId = " + extension.getPlugin().getId() + "\n" + extension + "\nReason: " + e.getMessage());
            }
        }

    }
  
    public static void registerFunctionLibrary(TLFunctionLibraryDescription functionLibrary){
        functionLibraries.add(functionLibrary);
    }

    public static boolean exists(String functionName) {
        for(TLFunctionLibraryDescription libraryDescription : functionLibraries) {
            if(libraryDescription.hasFunction(functionName)) {
                return true;
            }
        }
        return false;
    }

    public static TLFunctionPrototype getFunction(String functionName) {
        for(TLFunctionLibraryDescription libraryDescription : functionLibraries) {
            if(libraryDescription.hasFunction(functionName)) {
                TLFunctionPrototype function = libraryDescription.getFunction(functionName);
                if(function != null) {
                    return function;
                }
            }
        }
        return null;
    }

	public static List<TLFunctionPrototype> getAllFunctions() {
		List<TLFunctionPrototype> ret = new ArrayList<TLFunctionPrototype>();
		
		for(TLFunctionLibraryDescription libraryDescription : functionLibraries) {
			for (TLFunctionPrototype function : libraryDescription.getAllFunctions()) {
				ret.add(function);
			}
		}
		
		return ret;
	}

}
