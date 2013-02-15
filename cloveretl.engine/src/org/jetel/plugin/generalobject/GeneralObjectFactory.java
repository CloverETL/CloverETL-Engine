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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.plugin.Extension;
import org.jetel.plugin.Plugins;

/**
 * This factory provides instances of class registered via "generalObject" extension point.
 * This extension point is the most general way how to provide an instance of a class from a plugin.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 27 Feb 2012
 */
public class GeneralObjectFactory {

    private static Log logger = LogFactory.getLog(GeneralObjectFactory.class);

	private final static Map<String, GeneralObjectDescription> generalObjectsDescriptionMap = new HashMap<String, GeneralObjectDescription>();
	
	public static void init() {
        //ask plugin framework for all general objects
        List<Extension> generalObjectExtensions = Plugins.getExtensions(GeneralObjectDescription.EXTENSION_POINT_ID);
        
        //register all dictionary types
        for(Extension extension : generalObjectExtensions) {
            try {
            	GeneralObjectDescription description = new GeneralObjectDescription(extension);
            	description.init();
                registerGeneralObject(description);
            } catch(Exception e) {
                logger.error("Cannot create general object description, extension in plugin manifest is not valid.\n"
                        + "pluginId = " + extension.getPlugin().getId() + "\n" + extension, e);
            }
        }
	}
	
    public final static void registerGeneralObjects(GeneralObjectDescription[] generalObjects) {
        for (int i = 0; i < generalObjects.length; i++) {
        	registerGeneralObject(generalObjects[i]);
        }
    }
	
	public final static void registerGeneralObject(GeneralObjectDescription generalObject){
		generalObjectsDescriptionMap.put(generalObject.getId(), generalObject);
	}
	
	/**
	 * Basic way how general objects are provided.
	 * @see GeneralObjectDescription
	 */
	public final static Object getGeneralObject(String id) {
		GeneralObjectDescription desc = generalObjectsDescriptionMap.get(id);
        
		if (desc == null) {
			throw new RuntimeException("Unknown general object identifier: " + id);
		}

		return desc.getGeneralObject();
	}
    
}
