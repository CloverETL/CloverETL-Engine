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

package org.jetel.data.lookup;

//import org.w3c.dom.Node;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.ComponentFactory;
import org.jetel.graph.TransformationGraph;
import org.jetel.plugin.Extension;
import org.jetel.plugin.PluginDescriptor;
import org.jetel.plugin.Plugins;
import org.w3c.dom.Element;

/**
 *  Description of the Class
 *
 * @author     dpavlis
 * @since    May 27, 2002
 * @revision $Revision$
 */
public class LookupTableFactory {

    private static Log logger = LogFactory.getLog(ComponentFactory.class);

    private final static String NAME_OF_STATIC_LOAD_FROM_XML = "fromXML";
    private final static Class[] PARAMETERS_FOR_METHOD = new Class[] { TransformationGraph.class, Element.class };
    private final static Map lookupTableMap = new HashMap();
    
    public static void init() {
        //ask plugin framework for lookup tables
        List lookupTableExtensions = Plugins.getExtensions(LookupTableDescription.EXTENSION_POINT_ID);
        
        //register all lookup tables
        for(Iterator it = lookupTableExtensions.iterator(); it.hasNext();) {
            Extension extension = (Extension) it.next();
            try {
                registerLookupTable(new LookupTableDescription(extension));
            } catch(Exception e) {
                logger.error("Cannot create lookup table description, extension in plugin manifest is not valid.\n"
                        + "pluginId = " + extension.getPlugin().getId() + "\n"
                        + "extenstion - " + extension);
            }
        }
    }
    
    public final static void registerLookupTable(LookupTableDescription[] lookupTables) {
        for(int i = 0; i < lookupTables.length; i++) {
            lookupTableMap.put(lookupTables[i].getType(), lookupTables[i]);
        }
    }
    
    public final static void registerLookupTable(LookupTableDescription lookupTable){
        lookupTableMap.put(lookupTable.getType(), lookupTable);
    }
    
    /**
     * @param lookupTableType
     * @return class from the given lookup table type
     */
    private final static Class getLookupTableClass(String lookupTableType) {
        String className = null;
        LookupTableDescription lookupTableDescription = (LookupTableDescription) lookupTableMap.get(lookupTableType);
        
        try {
            if(lookupTableDescription == null) { 
                //unknown lookup table type, we suppose lookupTableType as full class name classification
                className = lookupTableType;
                //find class of lookupTable
                return Class.forName(lookupTableType); 
            } else {
                className = lookupTableDescription.getClassName();
                //activate plugin if necessary
                PluginDescriptor pluginDescriptor = lookupTableDescription.getPluginDescriptor();
                if(!pluginDescriptor.isActive()) {
                    pluginDescriptor.activatePlugin();
                }
                
                //find class of lookupTable
                return Class.forName(className, true, pluginDescriptor.getClassLoader());
            }
        } catch(ClassNotFoundException ex) {
            logger.error("Unknown lookup table: " + lookupTableType + " class: " + className);
            throw new RuntimeException("Unknown lookup table: " + lookupTableType + " class: " + className);
        } catch(Exception ex) {
            logger.error("Unknown lookup table type: " + lookupTableType);
            throw new RuntimeException("Unknown lookup table type: " + lookupTableType);
        }
    }

    /**
     *  Method for creating various types of LookupTable based on lookup type & XML parameter definition.
     */
    public final static LookupTable createLookupTable(TransformationGraph graph, String lookupTableType, org.w3c.dom.Node nodeXML) {
        Class tClass = getLookupTableClass(lookupTableType);

        try {
            //create instance of lookup table
            Method method = tClass.getMethod(NAME_OF_STATIC_LOAD_FROM_XML, PARAMETERS_FOR_METHOD);
            return (LookupTable) method.invoke(null, new Object[] {graph, nodeXML});
        } catch(Exception ex) {
            logger.error("Can't create object of : " + lookupTableType + " exception: " + ex);
            throw new RuntimeException("Can't create object of : " + lookupTableType + " exception: " + ex);
        }
    }
    
    /**
     *  Method for creating various types of LookupTable based on lookup type, parameters and theirs types for lookup table constructor.
     */
    public final static LookupTable createLookupTable(TransformationGraph graph, String lookupTableType, Object[] constructorParameters, Class[] parametersType) {
        Class tClass = getLookupTableClass(lookupTableType);

        try {
            //create instance of lookup table
            Constructor constructor = tClass.getConstructor(parametersType);
            return (LookupTable) constructor.newInstance(constructorParameters);
        } catch(Exception ex) {
            logger.error("Can't create object of : " + lookupTableType + " exception: " + ex);
            throw new RuntimeException("Can't create object of : " + lookupTableType + " exception: " + ex);
        }
    }

}


