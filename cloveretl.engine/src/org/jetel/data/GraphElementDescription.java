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
package org.jetel.data;

import java.util.ArrayList;
import java.util.List;

import org.jetel.plugin.Extension;
import org.jetel.plugin.PluginDescriptor;

/**
 * Ascendant of all plugin elements description like ComponentDescription, SequenceDescription,
 * ConnectionDescription etc. This class describes relation between engine and plugin, represents
 * extension point of plugin.
 *  
 * @author Martin Zatopek
 *
 */
public class GraphElementDescription extends PluginableItemDescription {

    private final static String TYPE = "type";
    private final static String CLASS = "className";

    private String type;
    
    private String className;
    
    private PluginDescriptor pluginDescriptor;
    
    public GraphElementDescription(String extensionPointId, Extension extension) {
    	super(extension);
    	
        if(!extension.getPointId().equals(extensionPointId)) {
            throw new IllegalArgumentException("Invalid extension point id (unexpected exception).");
        }
        this.type = extension.getParameter(TYPE).getString();
        this.className = extension.getParameter(CLASS).getString();
        this.pluginDescriptor = extension.getPlugin();
        if(type == null || className == null) {
            throw new IllegalArgumentException("Extension hasn't type or className parameter defined.");
        }
        
    }

    public String getClassName() {
        return className;
    }

    public String getType() {
        return type;
    }
    
    public PluginDescriptor getPluginDescriptor() {
        return pluginDescriptor;
    }

	@Override
	protected List<String> getClassNames() {
		List<String> result = new ArrayList<String>();
		result.add(getClassName());
		return result;
	}

}
