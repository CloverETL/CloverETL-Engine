/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/

package org.jetel.component;

//import org.w3c.dom.Node;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.plugin.Extension;
import org.jetel.plugin.Plugins;

/**
 *  Description of the Class
 *
 * @author     dpavlis
 * @since    May 27, 2002
 * @revision $Revision$
 */
public class ComponentFactory {

	private final static String NAME_OF_STATIC_LOAD_FROM_XML = "fromXML";
	private final static Class[] PARAMETERS_FOR_METHOD = new Class[] { TransformationGraph.class, org.w3c.dom.Node.class };
	private final static Map componentMap = new HashMap();
	
	public static void init() {
        //ask plugin framework for components
        List componentExtensions = Plugins.getExtensions(ComponentDescription.EXTENSION_POINT_ID);
        
        //register all components
        for(Iterator it = componentExtensions.iterator(); it.hasNext();) {
            Extension extension = (Extension) it.next();
            try {
                registerComponent(new ComponentDescription(extension));
            } catch(Exception e) {
                //EMPTY - only if extension is not valid set
            }
        }
        
	}
	
    public final static void registerComponents(ComponentDescription[] components) {
        for(int i = 0; i < components.length; i++) {
            componentMap.put(components[i].getType(), components[i]);
        }
    }
	
	public final static void registerComponent(ComponentDescription component){
		componentMap.put(component.getType(), component);
	}
	
	/**
	 *  Method for creating various types of Components based on component type & XML parameter definition.<br>
	 *  If component type is not registered, it tries to use componentType parameter directly as a class name.
	 *  This way new components can be added withou modifying ComponentFactory code.
	 *  
	 * @param  componentType  Type of the component (e.g. SimpleCopy, Gather, Join ...)
	 * @param  xmlNode        XML element containing appropriate Node parameters
	 * @return                requested Component (Node) object or null if creation failed 
	 * @since                 May 27, 2002
	 */
	public final static Node createComponent(TransformationGraph graph, String componentType, org.w3c.dom.Node nodeXML) {
		Class tClass;
		Method method;
        ComponentDescription componentDescription = (ComponentDescription) componentMap.get(componentType);
		try{
			tClass = Class.forName(componentDescription.getClassName(), true, componentDescription.getClassLoader());
		} catch(ClassNotFoundException ex) {
			throw new RuntimeException("Unknown component: " + componentType + " class: " + componentDescription.getClassName());
		} catch(Exception ex) {
            throw new RuntimeException("Unknown component type: " + componentType);
		}
		try {
			method = tClass.getMethod(NAME_OF_STATIC_LOAD_FROM_XML, PARAMETERS_FOR_METHOD);
			return (org.jetel.graph.Node) method.invoke(null, new Object[] {graph, nodeXML});
		} catch(Exception ex) {
			throw new RuntimeException("Can't create object of : " + componentType + " exception: " + ex);
		}
	}
}


