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

package org.jetel.database;

//import org.w3c.dom.Node;
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
 * @revision $Revision: 988 $
 */
public class ConnectionFactory {

    private static Log logger = LogFactory.getLog(ComponentFactory.class);

    private final static String NAME_OF_STATIC_LOAD_FROM_XML = "fromXML";
    private final static Class[] PARAMETERS_FOR_METHOD = new Class[] { TransformationGraph.class, Element.class };
    private final static Map connectionMap = new HashMap();
    
    public static void init() {
        //ask plugin framework for connections
        List connectionExtensions = Plugins.getExtensions(ConnectionDescription.EXTENSION_POINT_ID);
        
        //register all connection
        for(Iterator it = connectionExtensions.iterator(); it.hasNext();) {
            Extension extension = (Extension) it.next();
            try {
                registerConnection(new ConnectionDescription(extension));
            } catch(Exception e) {
                logger.error("Cannot create connection description, extension in plugin manifest is not valid.\n"
                        + "pluginId = " + extension.getPlugin().getId() + "\n"
                        + "extenstion - " + extension);
            }
        }
        
    }
    
    public final static void registerConnection(ConnectionDescription[] connections) {
        for(int i = 0; i < connections.length; i++) {
            connectionMap.put(connections[i].getType(), connections[i]);
        }
    }
    
    public final static void registerConnection(ConnectionDescription connection){
        connectionMap.put(connection.getType(), connection);
    }
    
    /**
     *  Method for creating various types of Connection based on connection type & XML parameter definition.
     */
    public final static IConnection createConnection(TransformationGraph graph, String connectionType, Element nodeXML) {
        Class tClass;
        ConnectionDescription connectionDescription = null;

        try {
            connectionDescription = (ConnectionDescription) connectionMap.get(connectionType);
            
            //activate plugin if necessary
            PluginDescriptor pluginDescriptor = connectionDescription.getPluginDescriptor();
            if(!pluginDescriptor.isActive()) {
                pluginDescriptor.activatePlugin();
            }
            
            //find class of connection
            tClass = Class.forName(connectionDescription.getClassName(), true, pluginDescriptor.getClassLoader());
        } catch(ClassNotFoundException ex) {
            throw new RuntimeException("Unknown connection: " + connectionType + " class: " + connectionDescription.getClassName());
        } catch(Exception ex) {
            throw new RuntimeException("Unknown connection type: " + connectionType);
        }
        try {
            //create instance of connection
            Method method = tClass.getMethod(NAME_OF_STATIC_LOAD_FROM_XML, PARAMETERS_FOR_METHOD);
            return (IConnection) method.invoke(null, new Object[] {graph, nodeXML});
        } catch(Exception ex) {
            throw new RuntimeException("Can't create object of : " + connectionType + " exception: " + ex);
        }
    }
}


