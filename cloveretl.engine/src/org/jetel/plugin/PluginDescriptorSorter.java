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
package org.jetel.plugin;

import java.util.LinkedHashMap;

import org.jetel.exception.JetelRuntimeException;

/**
 * This class sorts given map of {@link PluginDescriptor}s with respect of plugin prerequisities.
 * Resulted map is sorted so each plugin is behind all its prerequisities.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 8. 7. 2015
 */
public class PluginDescriptorSorter {

    public static void sort(LinkedHashMap<String, PluginDescriptor> pluginDescriptors) {
    	new PluginDescriptorSorter(pluginDescriptors).sort();
    }
    
    private LinkedHashMap<String, PluginDescriptor> pluginDescriptors;
    
	public PluginDescriptorSorter(LinkedHashMap<String, PluginDescriptor> pluginDescriptors) {
		this.pluginDescriptors = pluginDescriptors;
	}
    
    public void sort() {
    	LinkedHashMap<String, PluginDescriptor> result = new LinkedHashMap<String, PluginDescriptor>();
    	
    	while (!pluginDescriptors.isEmpty()) {
    		boolean progress = false;
    		for (PluginDescriptor pluginDescritor : pluginDescriptors.values()) {
    			boolean minimalPluginDescriptor = true;
    			for (PluginPrerequisite pluginPrerequisite : pluginDescritor.getPrerequisites()) {
    				if (pluginDescriptors.containsKey(pluginPrerequisite.getPluginId())) {
    					minimalPluginDescriptor = false;
    					break;
    				}
    			}
    			if (minimalPluginDescriptor) {
    				pluginDescriptors.remove(pluginDescritor.getId());
    				result.put(pluginDescritor.getId(), pluginDescritor);
    				progress = true;
    				break;
    			}
    		}
    		if (!progress) {
    			throw new JetelRuntimeException("Cyclic plugin prerequisities detected: " + pluginDescriptors.keySet());
    		}
    	}
    	
    	pluginDescriptors.clear();
    	pluginDescriptors.putAll(result);
    }
    
}
