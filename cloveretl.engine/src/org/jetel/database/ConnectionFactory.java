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
package org.jetel.database;

//import org.w3c.dom.Node;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.HashMap;
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
 * @revision $Revision: 988 $
 */
public class ConnectionFactory {

    private static Log logger = LogFactory.getLog(ComponentFactory.class);

    private final static String NAME_OF_STATIC_LOAD_FROM_XML = "fromXML";
    private final static Class[] PARAMETERS_FOR_METHOD = new Class[] { TransformationGraph.class, Element.class };
    private final static Map<String, ConnectionDescription> connectionMap = new HashMap<String, ConnectionDescription>();
    
    public static void init() {
        //ask plugin framework for connections
        List<Extension> connectionExtensions = Plugins.getExtensions(ConnectionDescription.EXTENSION_POINT_ID);
        
        //register all connection
        for(Extension extension : connectionExtensions) {
            try {
            	ConnectionDescription description = new ConnectionDescription(extension);
            	description.init();
                registerConnection(description);
            } catch(Exception e) {
                logger.error("Cannot create connection description, extension in plugin manifest is not valid.\n"
                        + "pluginId = " + extension.getPlugin().getId() + "\n"
                        + "extenstion - " + extension);
            }
        }
        
    }
    
    public final static void registerConnection(ConnectionDescription[] connections) {
        for(int i = 0; i < connections.length; i++) {
        	registerConnection(connections[i]);
        }
    }
    
    public final static void registerConnection(ConnectionDescription connection){
        connectionMap.put(connection.getType(), connection);
    }
    
    /**
     * @param connectionType
     * @return class from the given connection type
     */
    private final static Class getConnectionClass(String connectionType) {
        String className = null;
        ConnectionDescription connectionDescription = connectionMap.get(connectionType);
        
        try {
            if(connectionDescription == null) { 
                //unknown connection type, we suppose connectionType as full class name classification
                className = connectionType;
                //find class of connection
                return Class.forName(connectionType); 
            } else {
                className = connectionDescription.getClassName();

                PluginDescriptor pluginDescriptor = connectionDescription.getPluginDescriptor();
                
                //find class of connection
                return Class.forName(className, true, pluginDescriptor.getClassLoader());
            }
        } catch(ClassNotFoundException ex) {
            logger.error("Unknown connection: " + connectionType + " class: " + className);
            throw new RuntimeException("Unknown connection: " + connectionType + " class: " + className);
        } catch(Exception ex) {
            logger.error("Unknown connection type: " + connectionType);
            throw new RuntimeException("Unknown connection type: " + connectionType);
        }
    }

    /**
     *  Method for creating various types of Connection based on connection type & XML parameter definition.
     */
    public final static IConnection createConnection(TransformationGraph graph, String connectionType, Element nodeXML) {
        Class tClass = getConnectionClass(connectionType);

        try {
            //create instance of connection
            Method method = tClass.getMethod(NAME_OF_STATIC_LOAD_FROM_XML, PARAMETERS_FOR_METHOD);
            return (IConnection) method.invoke(null, new Object[] {graph, nodeXML});
        } catch(Throwable e) {
            logger.error("Can't create object of : " + connectionType + " exception: " + e, e);
            throw new RuntimeException("Can't create object of : " + connectionType + " exception: " + e);
        }
    }
    
    /**
     *  Method for creating various types of Connection based on connection type, parameters and theirs types for connection constructor.
     */
    public final static IConnection createConnection(TransformationGraph graph, String connectionType, Object[] constructorParameters, Class[] parametersType) {
        Class tClass = getConnectionClass(connectionType);

        try {
            //create instance of connection
            Constructor constructor = tClass.getConstructor(parametersType);
            return (IConnection) constructor.newInstance(constructorParameters);
        } catch(Exception ex) {
            logger.error("Can't create object of : " + connectionType + " exception: " + ex);
            throw new RuntimeException("Can't create object of : " + connectionType + " exception: " + ex);
        }
    }

    
    /**
     * Method for querying map of descriptors for all registered connection types
     * 
     * @param conType connection type
     * @return descriptor of connection type/plugin
     */
    public final static ConnectionDescription getConnectionDescription(String conType){
    	return connectionMap.get(conType);
    }
    
}


