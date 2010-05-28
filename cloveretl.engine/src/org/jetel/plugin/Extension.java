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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.jetel.util.string.StringUtils;

/**
 * This class represents extension point of engine.
 * @author Martin Zatopek
 *
 */
public class Extension {

    private final String pointId;
    
    private final Map<String, ExtensionParameter> parameters;

    private final PluginDescriptor plugin;
    
    public Extension(String pointId, PluginDescriptor plugin) {
        this.pointId = pointId;
        this.parameters = new HashMap<String, ExtensionParameter>();
        this.plugin = plugin;
    }

    public void addParameter(String key, ExtensionParameter parameter) {
        parameters.put(key, parameter);
    }
    
    public Map<String, ExtensionParameter> getParameters() {
        return new HashMap<String, ExtensionParameter>(parameters);
    }

    public Map<String, ExtensionParameter> getParameters(String[] excludeParameters) {
        if(excludeParameters == null) {
            return getParameters();
        }
        
        List<String> excludeList = Arrays.asList(excludeParameters);
        Map<String, ExtensionParameter> ret = new HashMap<String, ExtensionParameter>();
        for(Entry<String, ExtensionParameter> entry : parameters.entrySet()) {
            if(!excludeList.contains(entry.getKey())) {
                ret.put(entry.getKey(), entry.getValue());
            }
        }
        
        return ret;
    }

    public Map<String, ExtensionParameter> getParametersStartWith(String prefix) {
        if(StringUtils.isEmpty(prefix)) {
            return getParameters();
        }
        
        Map<String, ExtensionParameter> ret = new HashMap<String, ExtensionParameter>();
        for(Entry<String, ExtensionParameter> entry : parameters.entrySet()) {
            if(entry.getKey().startsWith(prefix)) {
                ret.put(entry.getKey(), entry.getValue());
            }
        }
        
        return ret;
    }

    public ExtensionParameter getParameter(String key) {
        return parameters.get(key);
    }
    
    public ExtensionParameter getParameter(String key, String defaultValue) {
        ExtensionParameter ret = parameters.get(key);
        return ret != null ? ret : new ExtensionParameter(defaultValue);
    }

    public boolean hasParameter(String key) {
        return parameters.containsKey(key);
    }
    
    public PluginDescriptor getPlugin() {
        return plugin;
    }

    public String getPointId() {
        return pointId;
    }

    public String toString() {
        StringBuilder ret = new StringBuilder(getPointId() + " { ");
        
        for(String key : parameters.keySet()) {
            final ExtensionParameter parameter = parameters.get(key); 
            ret.append(key + " = ");
            ret.append(parameter);
            ret.append("; ");
        }
        
        ret.append(" }");
        return ret.toString();
    }
}
