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

import javax.naming.directory.InvalidAttributesException;

import org.jetel.util.string.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * XML builder for plugin descriptor. Parse input xml document and fill all fields in plugin descriptor.
 *  
 * @author Martin Zatopek
 *
 */
public class PluginDescriptionBuilder {

    private static final String PLUGIN_ELEMENT = "plugin";

    private static final String ID_ATTR = "id";

    private static final String VERSION_ATTR = "version";

    private static final String PROVIDER_NAME = "provider-name";

    private static final String CLASS_ATTR = "class";

    private static final String GREEDY_CLASS_LOADER = "greedyClassLoader";

    private static final String LAZY_ACTIVATED = "lazyActivated";

    private static final String EXCLUDED_PACKAGES = "excludedPackages";

    private static final String RUNTIME_ELEMENT = "runtime";
    
    private static final String LIBRARY_ELEMENT = "library";

    private static final String NATIVE_LIBRARY_ELEMENT = "nativelibrary";
    
    private static final String PATH_ATTR = "path";
    
    private static final String REQUIRES_ELEMENT = "requires";
    
    private static final String IMPORT_ELEMENT = "import";

    private static final String PLUGIN_ID_ATTR = "plugin-id";
    
    private static final String PLUGIN_VERSION_ATTR = "plugin-version";
    
    private static final String MATCH_ATTR = "match";
    
    private static final String EXTENSION_ELEMENT = "extension";
    
    private static final String POINT_ID_ATTR = "point-id";
    
    private static final String PARAMETER_ELEMENT = "parameter";
    
    private static final String VALUE_ATTR = "value";

    
    private PluginDescriptor plugin;
    
    public PluginDescriptionBuilder(PluginDescriptor plugin) {
        this.plugin = plugin;
    }
    
    public PluginDescriptor read(Document document) throws InvalidAttributesException {
        //plugin element
        NodeList nodes = document.getElementsByTagName(PLUGIN_ELEMENT);
        validateLength(nodes, 1, 1, "Root 'plugin' node does not exists.");
        readPlugin((Element) nodes.item(0));
        return plugin;
    }

    private void readPlugin(Element pluginElement) throws InvalidAttributesException {
        //id attribute
        if(!pluginElement.hasAttribute(ID_ATTR)) {
            throw new InvalidAttributesException("Plugin 'id' attribute is not set.");
        }
        plugin.setId(pluginElement.getAttribute(ID_ATTR));

        //version attribute
        if(!pluginElement.hasAttribute(VERSION_ATTR)) {
            throw new InvalidAttributesException("Plugin 'version' attribute is not set.");
        }
        plugin.setVersion(pluginElement.getAttribute(VERSION_ATTR));
        
        //provider-name attribute
        if(pluginElement.hasAttribute(PROVIDER_NAME)) {
            plugin.setProviderName(pluginElement.getAttribute(PROVIDER_NAME));
        }

        //class attribute
        plugin.setPluginClassName(pluginElement.getAttribute(CLASS_ATTR));
        
        //greedy class loader
        if (pluginElement.hasAttribute(GREEDY_CLASS_LOADER)) {
        	plugin.setGreedyClassLoader(Boolean.parseBoolean(pluginElement.getAttribute(GREEDY_CLASS_LOADER)));
        }

        //lazy activated
        if (pluginElement.hasAttribute(LAZY_ACTIVATED)) {
        	plugin.setLazyActivated(Boolean.parseBoolean(pluginElement.getAttribute(LAZY_ACTIVATED)));
        }

        //excluded packages
        if (pluginElement.hasAttribute(EXCLUDED_PACKAGES)) {
        	plugin.setExcludedPackages(StringUtils.split(pluginElement.getAttribute(EXCLUDED_PACKAGES)));
        }
        
        //requires element
        NodeList nodes = pluginElement.getElementsByTagName(REQUIRES_ELEMENT);
        validateLength(nodes, 0, 1, "Node 'requires' cannot be multiple.");
        if(nodes.getLength() == 1) readRequires((Element) nodes.item(0));

        //runtime element
        nodes = pluginElement.getElementsByTagName(RUNTIME_ELEMENT);
        validateLength(nodes, 0, 1, "Node 'runtime' cannot be multiple.");
        if(nodes.getLength() == 1) readRuntime((Element) nodes.item(0));
        
        //extension elements
        nodes = pluginElement.getElementsByTagName(EXTENSION_ELEMENT);
        for(int i = 0; i < nodes.getLength(); i++) {
            readExtension((Element) nodes.item(i));
        }
    }

    private void readRuntime(Element runtimeElement) throws InvalidAttributesException {
        //library elements
        NodeList nodes = runtimeElement.getElementsByTagName(LIBRARY_ELEMENT);
        for(int i = 0; i < nodes.getLength(); i++) {
            readLibrary((Element) nodes.item(i));
        }
        
        //native library elements
        nodes = runtimeElement.getElementsByTagName(NATIVE_LIBRARY_ELEMENT);
        for (int i = 0; i < nodes.getLength(); i++) {
            readNativeLibrary((Element) nodes.item(i));
        }
    }

    private void readLibrary(Element libraryElement) throws InvalidAttributesException {
        if(!libraryElement.hasAttribute(PATH_ATTR)) {
            throw new InvalidAttributesException("Plugin.runtime.library 'path' attribute is not set.");
        }
        plugin.addLibrary(libraryElement.getAttribute(PATH_ATTR));
    }

    private void readNativeLibrary(Element nativeLibraryElement) throws InvalidAttributesException {
        if(!nativeLibraryElement.hasAttribute(PATH_ATTR)) {
            throw new InvalidAttributesException("Plugin.runtime.nativelibrary 'path' attribute is not set.");
        }
        plugin.addNativeLibrary(nativeLibraryElement.getAttribute(PATH_ATTR));
    }

    private void readExtension(Element extensionElement) throws InvalidAttributesException {
        if(!extensionElement.hasAttribute(POINT_ID_ATTR)) {
            throw new InvalidAttributesException("Plugin.extension 'point-id' attribute is not set.");
        }

        Extension extension = plugin.addExtension(extensionElement.getAttribute(POINT_ID_ATTR));

        //parameter elements
        NodeList nodes = extensionElement.getElementsByTagName(PARAMETER_ELEMENT);
        for(int i = 0; i < nodes.getLength(); i++) {
            readParameter((Element) nodes.item(i), extension);
        }
        
    }

    private void readParameter(Element parameterElement, Extension extension) throws InvalidAttributesException {
        if(!parameterElement.hasAttribute(ID_ATTR)) {
            throw new InvalidAttributesException("Plugin.extension.parameter 'id' attribute is not set.");
        }
        if(parameterElement.hasAttribute(VALUE_ATTR)) {
            extension.addParameter(parameterElement.getAttribute(ID_ATTR), new ExtensionParameter(parameterElement.getAttribute(VALUE_ATTR)));
        } else {
            ExtensionParameter extensionParameter = new ExtensionParameter();
            NodeList valuesElements = parameterElement.getElementsByTagName(VALUE_ATTR);
            for(int i = 0; i < valuesElements.getLength(); i++) {
                String value = valuesElements.item(i).getTextContent();
                if(!StringUtils.isEmpty(value)) {
                    extensionParameter.addValue(value);
                }
            }
            if(!extensionParameter.isEmpty()) {
                extension.addParameter(parameterElement.getAttribute(ID_ATTR), extensionParameter);
            }
        }
    }

    private void readRequires(Element requiresElement) throws InvalidAttributesException {
        //import elements
        NodeList nodes = requiresElement.getElementsByTagName(IMPORT_ELEMENT);
        for(int i = 0; i < nodes.getLength(); i++) {
            readImport((Element) nodes.item(i));
        }
    }

    private void readImport(Element importElement) throws InvalidAttributesException {
        //plugin-id attribute
        if(!importElement.hasAttribute(PLUGIN_ID_ATTR)) {
            throw new InvalidAttributesException("Plugin.requires.import 'plugin-id' attribute is not set.");
        }
        plugin.addPrerequisites(importElement.getAttribute(PLUGIN_ID_ATTR), importElement.getAttribute(PLUGIN_VERSION_ATTR), importElement.getAttribute(MATCH_ATTR));
    }
    
    private void validateLength(NodeList nodes, int min, int max, String message) throws InvalidAttributesException {
        if(nodes.getLength() < min || nodes.getLength() > max) {
            throw new InvalidAttributesException(message);
        }
    }

}
