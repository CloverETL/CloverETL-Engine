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
package org.jetel.data.lookup;

//import org.w3c.dom.Node;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.ComponentFactory;
import org.jetel.database.ConnectionFactory;
import org.jetel.database.IConnection;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.IGraphElement;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.MetadataFactory;
import org.jetel.plugin.Extension;
import org.jetel.plugin.PluginDescriptor;
import org.jetel.plugin.Plugins;
import org.jetel.util.file.FileUtils;
import org.jetel.util.primitive.TypedProperties;
import org.jetel.util.property.ComponentXMLAttributes;
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
    private final static Class<?>[] PARAMETERS_FOR_FROM_XML_METHOD = new Class[] { TransformationGraph.class, Element.class };
    private final static String NAME_OF_STATIC_LOAD_FROM_PROPERTIES = "fromProperties";
    private final static Class<?>[] PARAMETERS_FOR_FROM_PROPERTIES_METHOD = new Class[] {TypedProperties.class };
    private final static Map<String, LookupTableDescription> lookupTableMap = new HashMap<String, LookupTableDescription>();
    
    public static void init() {
        //ask plugin framework for lookup tables
        List<Extension> lookupTableExtensions = Plugins.getExtensions(LookupTableDescription.EXTENSION_POINT_ID);
        
        //register all lookup tables
        for(Extension extension : lookupTableExtensions) {
            try {
            	LookupTableDescription description = new LookupTableDescription(extension);
            	description.init();
                registerLookupTable(description);
            } catch(Exception e) {
                logger.error("Cannot create lookup table description, extension in plugin manifest is not valid.\n"
                        + "pluginId = " + extension.getPlugin().getId() + "\n"
                        + "extenstion - " + extension);
            }
        }
    }
    
    public final static void registerLookupTable(LookupTableDescription[] lookupTables) {
        for(int i = 0; i < lookupTables.length; i++) {
        	registerLookupTable(lookupTables[i]);
        }
    }
    
    public final static void registerLookupTable(LookupTableDescription lookupTable){
        lookupTableMap.put(lookupTable.getType(), lookupTable);
    }
    
    /**
     * @param lookupTableType
     * @return class from the given lookup table type
     */
    private final static Class<? extends LookupTable> getLookupTableClass(String lookupTableType) {
        String className = null;
        LookupTableDescription lookupTableDescription = lookupTableMap.get(lookupTableType);
        
        try {
            if(lookupTableDescription == null) { 
                //unknown lookup table type, we suppose lookupTableType as full class name classification
                className = lookupTableType;
                //find class of lookupTable
                return Class.forName(lookupTableType).asSubclass(LookupTable.class); 
            } else {
                className = lookupTableDescription.getClassName();

                PluginDescriptor pluginDescriptor = lookupTableDescription.getPluginDescriptor();
                
                //find class of lookupTable
                return Class.forName(className, true, pluginDescriptor.getClassLoader()).asSubclass(LookupTable.class);
            }
        } catch(ClassNotFoundException ex) {
            logger.error("Unknown lookup table: " + lookupTableType + " class: " + className);
            throw new RuntimeException("Unknown lookup table: " + lookupTableType + " class: " + className);
        } catch(Exception ex) {
            logger.error("Unknown lookup table type: " + lookupTableType);
            throw new RuntimeException("Unknown lookup table type: " + lookupTableType);
        }
    }

	public final static LookupTable createLookupTable(TypedProperties lookupProperties) {
		String lookupTableType = lookupProperties.getProperty("type");
        Class<? extends LookupTable> tClass = getLookupTableClass(lookupTableType);
        try {
            //create instance of lookup table
            Method method = tClass.getMethod(NAME_OF_STATIC_LOAD_FROM_PROPERTIES, PARAMETERS_FOR_FROM_PROPERTIES_METHOD);
            return (LookupTable) method.invoke(null, new Object[] {lookupProperties});
        } catch(Exception ex) {
            logger.error("Can't create object of : " + lookupTableType + " exception: " + ex, ex);
            throw new RuntimeException("Can't create object of : " + lookupTableType + " exception: " + ex);
        }
	}

	/**
     *  Method for creating various types of LookupTable based on lookup type & XML parameter definition.
     */
    public final static LookupTable createLookupTable(TransformationGraph graph, String lookupTableType, org.w3c.dom.Node nodeXML) {
        Class<? extends LookupTable> tClass = getLookupTableClass(lookupTableType);

        try {
            //create instance of lookup table
            Method method = tClass.getMethod(NAME_OF_STATIC_LOAD_FROM_XML, PARAMETERS_FOR_FROM_XML_METHOD);
            return (LookupTable) method.invoke(null, new Object[] {graph, nodeXML});
        } catch(Exception ex) {
            logger.error("Can't create object of : " + lookupTableType + " exception: " + ex, ex);
            throw new RuntimeException("Can't create object of : " + lookupTableType + " exception: " + ex);
        }
    }
    
    /**
     *  Method for creating various types of LookupTable based on lookup type, parameters and theirs types for lookup table constructor.
     */
    public final static LookupTable createLookupTable(TransformationGraph graph, String lookupTableType, Object[] constructorParameters, Class<?>[] parametersType) {
        Class<? extends LookupTable> tClass = getLookupTableClass(lookupTableType);

        try {
            //create instance of lookup table
            Constructor<? extends LookupTable> constructor = tClass.getConstructor(parametersType);
            return constructor.newInstance(constructorParameters);
        } catch(Exception ex) {
            logger.error("Can't create object of : " + lookupTableType + " exception: " + ex);
            throw new RuntimeException("Can't create object of : " + lookupTableType + " exception: " + ex);
        }
    }

    public final static LookupTable createLookupTable(TransformationGraph graph, Element lookupElement) {
        ComponentXMLAttributes attributes = new ComponentXMLAttributes(lookupElement, graph);
        LookupTable result = null;
        
        if (attributes.exists("lookupConfig")) {
        	String fileURL = null;
			try {
				fileURL = attributes.getString("lookupConfig");
			} catch (AttributeNotFoundException e) {
				// can't happen: we've checked it
			}
        	TypedProperties lookupProperties = new TypedProperties(null,graph);
        	try {
            	lookupProperties.setProperty(IGraphElement.XML_ID_ATTRIBUTE, attributes.getString(IGraphElement.XML_ID_ATTRIBUTE));
				lookupProperties.load(FileUtils.getInputStream(graph.getRuntimeContext().getContextURL(), fileURL));
				String metadata = lookupProperties.getStringProperty("metadata");
				if (metadata != null && ! metadata.trim().equals("")) {
					DataRecordMetadata lookupMetadata = MetadataFactory.fromFile(graph, lookupProperties.getStringProperty("metadata"));
					lookupProperties.setProperty(LookupTable.XML_METADATA_ID, 
							graph.addDataRecordMetadata(TransformationGraph.DEFAULT_METADATA_ID, lookupMetadata));
				}
				if (lookupProperties.containsKey(LookupTable.XML_DBCONNECTION)) {
					IConnection connection = ConnectionFactory.createConnection(graph, "JDBC", 
							new Object[]{graph.getUniqueConnectionId() , lookupProperties.getStringProperty("dbConnection")},
							new Class[]{String.class, String.class});
					graph.addConnection(connection);
					lookupProperties.setProperty(LookupTable.XML_DBCONNECTION, connection.getId());
				}
				if (!lookupProperties.containsKey(IGraphElement.XML_TYPE_ATTRIBUTE)) {
					throw new XMLConfigurationException("Attribute type at Lookup table is missing");
				}
			} catch (Exception e) {
                throw new RuntimeException("Lookup table is not configured properly: " + e.getMessage());
			}
            result = LookupTableFactory.createLookupTable(lookupProperties);
        }else{
            // process Lookup table element attributes "id" & "type"
            String lookupTableType;
            try {
                lookupTableType = attributes.getString(IGraphElement.XML_TYPE_ATTRIBUTE);
            } catch (AttributeNotFoundException ex) {
                throw new RuntimeException("Attribute type at Lookup table is missing - " + ex.getMessage());
            }

            //create lookup table
            result = LookupTableFactory.createLookupTable(graph, lookupTableType, lookupElement);
        }
        
        return result;
    }
    
}


