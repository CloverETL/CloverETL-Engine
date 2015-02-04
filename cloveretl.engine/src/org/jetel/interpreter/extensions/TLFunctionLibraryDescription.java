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
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.PluginableItemDescriptionImpl;
import org.jetel.plugin.Extension;
import org.jetel.plugin.PluginDescriptor;

/**
 * This class describes transformation function library from a plugin.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 29.5.2007
 */
public class TLFunctionLibraryDescription extends PluginableItemDescriptionImpl {

    private static Log logger = LogFactory.getLog(TLFunctionLibraryDescription.class);

    public final static String EXTENSION_POINT_ID = "tlfunction";

    private final static String LIBRARY_NAME = "libraryName";
    private final static String CLASS = "className";
    private final static String FUNCTION = "function";

    private String libraryName;
    
    private String className;
    
    private List<String> functionNames;
    
    private PluginDescriptor pluginDescriptor;
    
    private ITLFunctionLibrary functionLibrary;
    
    public TLFunctionLibraryDescription(Extension extensionPoint) {
    	super(extensionPoint);
    	
        this.libraryName = extensionPoint.getParameter(LIBRARY_NAME).getString();
        this.className = extensionPoint.getParameter(CLASS).getString();
        this.functionNames = extensionPoint.getParameter(FUNCTION).getValues();
        this.pluginDescriptor = extensionPoint.getPlugin();
    }
    
    public boolean hasFunction(String functionName) {
        return functionNames.contains(functionName) || 
        		(functionName.startsWith(libraryName) && 
        		functionNames.contains(functionName.substring(libraryName.length() + 1)));
    }

    public TLFunctionPrototype getFunction(String functionName) {
        return getFunctionLibrary().getFunction(functionName);
    }

    private ITLFunctionLibrary getFunctionLibrary() {
        if(functionLibrary == null) {
            Class libraryClass;
            
            try {
                //find class of function library
                libraryClass = Class.forName(className, true, pluginDescriptor.getClassLoader());
            } catch(ClassNotFoundException ex) {
                logger.error("Library class " + className + " does not exist in plugin " + pluginDescriptor.getId() + ".");
                throw new RuntimeException("Library class " + className + " does not exist in plugin " + pluginDescriptor.getId() + ".");
            }
            
            if(!ITLFunctionLibrary.class.isAssignableFrom(libraryClass)) {
                logger.error("Library class " + className + " does not implement " + ITLFunctionLibrary.class.getName() + " interface.");
                throw new RuntimeException("Library class " + className + " does not implement " + ITLFunctionLibrary.class.getName() + " interface.");
            }
            
            try {
                functionLibrary = (ITLFunctionLibrary) libraryClass.newInstance();
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("Library class " + className + " does not have accessible nullary constructor.");
                throw new RuntimeException("Library class " + className + " does not have accessible nullary constructor.");
            }
        }
        
        return functionLibrary;
    }

	public Collection<TLFunctionPrototype> getAllFunctions() {
		
		ITLFunctionLibrary a = getFunctionLibrary();

		return a.getAllFunctions();
	}

	@Override
	protected List<String> getClassNames() {
		List<String> result = new ArrayList<String>();
		result.add(className);
		return result;
	}
	
}
