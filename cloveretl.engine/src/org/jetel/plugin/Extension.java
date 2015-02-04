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
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * This class represents extension point of engine.
 * @author Martin Zatopek
 *
 */
public class Extension {

    private final String pointId;
    
    private final Map<String, ExtensionParameter> parameters;

    private final PluginDescriptor plugin;
    
    /** Complete XML of extension point. */
    private final Element xmlElement;
    
    public Extension(String pointId, Element xmlElement, PluginDescriptor plugin) {
        this.pointId = pointId;
        this.parameters = new HashMap<String, ExtensionParameter>();
        this.plugin = plugin;
        this.xmlElement = xmlElement;
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

    /**
     * @return complete XML of extension point
     */
    public Element getXMLDefinition() {
    	return xmlElement;
    }
    
    @Override
	public String toString() {
        StringBuilder ret = new StringBuilder(getPointId() + " { ");
        
        //print out all 'parameters'
        for(String key : parameters.keySet()) {
            final ExtensionParameter parameter = parameters.get(key); 
            ret.append(key + " = " + parameter + "; ");
        }
        
        //this complicated code just print out important attributes of the root XML element of this extension description
        //this is used for 'component' extension point, where 'paramters' are not used at all
        if (parameters.isEmpty() && xmlElement.hasChildNodes()) {
        	NodeList childNodes = xmlElement.getChildNodes();
        	for (int j = 0; j < childNodes.getLength(); j++) {
        		if (childNodes.item(j) instanceof Element) {
		        	NamedNodeMap attributes = childNodes.item(j).getAttributes();
		        	if (attributes != null) {
			        	for (int i = 0; i < attributes.getLength(); i++) {
			        		Node attribute = attributes.item(i);
			        		String attrName = attribute.getNodeName();
			        		if (isPrintedAttribute(attrName)) {
				        		String attrValue = attribute.getNodeValue();
				        		ret.append(attrName + " = " + attrValue + "; ");
			        		}
			        	}
		        	}
		        	break;
        		}
        	}
        }
        ret.append("}");
        return ret.toString();
    }
    
    private static final List<String> PRINTED_ATTRIBUTES = Arrays.asList("id", "name", "type", "className");
    /**
     * Filter out only important attributes which should be printed out to console.
     */
    private static boolean isPrintedAttribute(String attributeName) {
    	return PRINTED_ATTRIBUTES.contains(attributeName);
    }
    
}
