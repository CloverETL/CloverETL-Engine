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
package org.jetel.plugin.generalobject;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.PluginableItemDescriptionImpl;
import org.jetel.plugin.Extension;
import org.jetel.plugin.PluginDescriptor;

/**
 * This class describes a general object from a plugin. This object is registered by 'generalObject'
 * extension point.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 27 Feb 2012
 */
public class GeneralObjectDescription extends PluginableItemDescriptionImpl {

    private static Log logger = LogFactory.getLog(GeneralObjectDescription.class);

    public final static String EXTENSION_POINT_ID = "generalObject";

    private final static String ID = "id";
    private final static String CLASS = "className";

    private String id;
    
    private String className;
    
    private PluginDescriptor pluginDescriptor;
    
    private Object generalObject;
    
    public GeneralObjectDescription(Extension extensionPoint) {
    	super(extensionPoint);
    	
        this.id = extensionPoint.getParameter(ID).getString();
        this.className = extensionPoint.getParameter(CLASS).getString();
        this.pluginDescriptor = extensionPoint.getPlugin();
    }
    
    /**
     * @return string identifier under which the general object was registered
     */
    public String getId() {
    	return id;
    }
    
    /**
     * Lazy created instance of class registered by 'generalObject' extension point.
     * Non-parametric constructor of class is expected.
     * @return instance of general object class 
     */
    public Object getGeneralObject() {
        if (generalObject == null) {
            Class<?> generalObjectClass;
            
            try {
                //find class of general object
                generalObjectClass = Class.forName(className, true, pluginDescriptor.getClassLoader());
            } catch (ClassNotFoundException ex) {
                logger.error("General object class " + className + " does not exist in plugin " + pluginDescriptor.getId() + ".");
                throw new RuntimeException("General object class " + className + " does not exist in plugin " + pluginDescriptor.getId() + ".", ex);
            }
            
            try {
                generalObject = generalObjectClass.newInstance();
            } catch (Exception e) {
                logger.error("Library class " + className + " does not have accessible nullary constructor.");
                throw new RuntimeException("Library class " + className + " does not have accessible nullary constructor.", e);
            }
        }
        
        return generalObject;
    }

	@Override
	protected List<String> getClassNames() {
		List<String> result = new ArrayList<String>();
		result.add(className);
		return result;
	}
	
}
