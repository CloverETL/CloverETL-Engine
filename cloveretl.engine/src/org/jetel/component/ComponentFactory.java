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
package org.jetel.component;

//import org.w3c.dom.Node;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.graph.GraphElement;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.TransformationGraphXMLReaderWriter;
import org.jetel.graph.distribution.EngineComponentAllocation;
import org.jetel.plugin.Extension;
import org.jetel.plugin.PluginDescriptor;
import org.jetel.plugin.Plugins;
import org.jetel.util.XmlUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 *  Description of the Class
 *
 * @author     dpavlis
 * @since    May 27, 2002
 */
public class ComponentFactory {

    private static Log logger = LogFactory.getLog(ComponentFactory.class);

	private final static String NAME_OF_STATIC_LOAD_FROM_XML = "fromXML";
	private final static Class<?>[] PARAMETERS_FOR_METHOD = new Class[] { TransformationGraph.class, Element.class };
	private final static Map<String, ComponentDescription> componentMap = new HashMap<String, ComponentDescription>();
	
	public static void init() {
        //ask plugin framework for components
        List<Extension> componentExtensions = Plugins.getExtensions(ComponentDescription.EXTENSION_POINT_ID);
        
        //register all components
        for(Extension extension : componentExtensions) {
            try {
            	ComponentDescription description = new ComponentDescription(extension);
            	description.init();
                registerComponent(description);
            } catch(Exception e) {
                logger.error("Cannot create component description, extension in plugin manifest is not valid.\n"
                        + "pluginId = " + extension.getPlugin().getId() + "\n" + extension, e);
            }
        }
        
	}
	
    public final static void registerComponents(ComponentDescription[] components) {
        for(int i = 0; i < components.length; i++) {
        	registerComponent(components[i]);
        }
    }
	
	public final static void registerComponent(ComponentDescription component){
		componentMap.put(component.getType(), component);
	}
	
    
    /**
     * @param componentType
     * @return class from the given component type
     */
    public final static Class<? extends Node> getComponentClass(String componentType) {
        String className = null;
        ComponentDescription componentDescription = componentMap.get(componentType);
        
        try {
            if(componentDescription == null) { 
                //unknown component type, we suppose componentType as full class name classification
                className = componentType;
                //find class of component
                return Class.forName(componentType).asSubclass(Node.class); 
            } else {
                className = componentDescription.getClassName();

                PluginDescriptor pluginDescriptor = componentDescription.getPluginDescriptor();
                
                //find class of component
                return Class.forName(className, true, pluginDescriptor.getClassLoader()).asSubclass(Node.class);
            }
        } catch(ClassNotFoundException ex) {
            throw new RuntimeException("Unknown component: " + componentType + " class: " + className, ex);
        } catch(Exception ex) {
            throw new RuntimeException("Unknown component type: " + componentType, ex);
        }

    }
    
	/**
	 *  Method for creating various types of Components based on component type & XML parameter definition.<br>
	 *  If component type is not registered, it tries to use componentType parameter directly as a class name.
	 *  
	 * @param  componentType  Type of the component (e.g. SimpleCopy, Gather, Join ...)
	 * @param  xmlNode        XML element containing appropriate Node parameters
	 * @return                requested Component (Node) object or null if creation failed 
	 * @since                 May 27, 2002
	 */
	public final static Node createComponent(TransformationGraph graph, String componentType, org.w3c.dom.Node nodeXML) {
		Class<? extends Node> tClass = getComponentClass(componentType);

        try {
            //create instance of component
			Method method = tClass.getMethod(NAME_OF_STATIC_LOAD_FROM_XML, PARAMETERS_FOR_METHOD);
			Node result = (org.jetel.graph.Node) method.invoke(null, new Object[] {graph, nodeXML});
			
			//nodeDistribution attribute parsing
			//hack for extracting of node layout information - Clover3 solves this issue
			//it is the easiest way how to add new common attribute for all nodes
			ComponentXMLAttributes xattribs = new ComponentXMLAttributes((Element) nodeXML, graph);
			if (xattribs.exists(Node.XML_ALLOCATION_ATTRIBUTE)) {
				EngineComponentAllocation nodeAllocation = EngineComponentAllocation.fromString(xattribs.getString(Node.XML_ALLOCATION_ATTRIBUTE));
				result.setAllocation(nodeAllocation);
			}
			//name attribute parsing
			if (xattribs.exists(Node.XML_NAME_ATTRIBUTE)) {
				String nodeName = xattribs.getString(Node.XML_NAME_ATTRIBUTE);
				result.setName(nodeName);
			}
			
			//preset description to the node
			result.setDescription(componentMap.get(componentType));
			
			return result;
		} catch (Exception e) {
			Throwable t = e;
			if (e instanceof InvocationTargetException) {
				t = ((InvocationTargetException) e).getTargetException();
			}
			ComponentXMLAttributes xattribs = new ComponentXMLAttributes((Element) nodeXML, graph);
			String id = xattribs.getString(Node.XML_ID_ATTRIBUTE, null); 
			String name = xattribs.getString(Node.XML_NAME_ATTRIBUTE, null); 
            throw new RuntimeException("Can't create component " + GraphElement.identifiersToString(id, name) + ".", t);
		}
	}
    
    /**
     *  Method for creating various types of Components based on component type, parameters and theirs types for component constructor.<br>
     *  If component type is not registered, it tries to use componentType parameter directly as a class name.<br>
     *  i.e.<br>
     *  <code>
     *  Node reformatNode = ComponentFactory.createComponent(myGraph, "REFORMAT", new Object[] { "REF_UNIQUE_CNT", myRecordTransform}, new Class[] {String.class, RecordTransform.class});
     *  </code>
     * @param graph
     * @param componentType
     * @param constructorParameters parameters passed to component constructor
     * @param parametersType types of all constructor parameters
     * @return
     */
    public final static Node createComponent(TransformationGraph graph, String componentType, Object[] constructorParameters, Class<?>[] parametersType) {
        Class<? extends Node> tClass = getComponentClass(componentType);
        
        try {
            //create instance of component
            Constructor<? extends Node> constructor = tClass.getConstructor(parametersType);
            return constructor.newInstance(constructorParameters);
        } catch(InvocationTargetException e) {
            throw new RuntimeException("Can't create component of type '" + componentType + "'.", e.getTargetException());
        } catch(Exception e) {
            throw new RuntimeException("Can't create component of type '" + componentType + "'.", e);
        }
    }
    
    /**
     *  Method for creating various types of components based on component type and attributes passed as {@link Properties}.
     */
    public final static Node createComponent(TransformationGraph graph, String componentType, Properties properties) {
    	Document xmlDocument = XmlUtils.createDocumentFromProperties(TransformationGraphXMLReaderWriter.NODE_ELEMENT, properties);
    	return createComponent(graph, componentType, (Element) xmlDocument.getFirstChild());
    }

}


