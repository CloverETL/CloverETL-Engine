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
package org.jetel.data.sequence;

//import org.w3c.dom.Node;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.ComponentFactory;
import org.jetel.graph.GraphElement;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.plugin.Extension;
import org.jetel.plugin.PluginDescriptor;
import org.jetel.plugin.Plugins;
import org.jetel.util.property.ComponentXMLAttributes;
import org.w3c.dom.Element;

/**
 *  Description of the Class
 *
 * @author     dpavlis
 * @since    May 27, 2002
 */
public class SequenceFactory {

    private static Log logger = LogFactory.getLog(ComponentFactory.class);

    private final static String NAME_OF_STATIC_LOAD_FROM_XML = "fromXML";
    private final static Class[] PARAMETERS_FOR_METHOD = new Class[] { TransformationGraph.class, Element.class };
    private final static Map<String, SequenceDescription> sequenceMap = new HashMap<String, SequenceDescription>();
    
    public static void init() {
        //ask plugin framework for sequences
        List<Extension> sequenceExtensions = Plugins.getExtensions(SequenceDescription.EXTENSION_POINT_ID);
        
        //register all sequences
        for(Extension extension : sequenceExtensions) {
            try {
            	SequenceDescription description = new SequenceDescription(extension);
            	description.init();
                registerSequence(description);
            } catch(Exception e) {
                logger.error("Cannot create component description, extension in plugin manifest is not valid.\n"
                        + "pluginId = " + extension.getPlugin().getId() + "\n"
                        + "extenstion - " + extension);
            }
        }
        
    }
    
    public final static void registerSequences(SequenceDescription[] sequences) {
        for(int i = 0; i < sequences.length; i++) {
        	registerSequence(sequences[i]);
        }
    }
    
    public final static void registerSequence(SequenceDescription sequence){
        sequenceMap.put(sequence.getType(), sequence);
    }
    
    /**
     * @param sequenceType
     * @return class from the given sequence type
     */
    private final static Class getSequenceClass(String sequenceType) {
        String className = null;
        SequenceDescription sequenceDescription = sequenceMap.get(sequenceType);
        
        try {
            if(sequenceDescription == null) { 
                //unknown sequence type, we suppose sequenceType as full class name classification
                className = sequenceType;
                //find class of sequence
                return Class.forName(sequenceType); 
            } else {
                className = sequenceDescription.getClassName();

                PluginDescriptor pluginDescriptor = sequenceDescription.getPluginDescriptor();
                
                //find class of component
                return Class.forName(className, true, pluginDescriptor.getClassLoader());
            }
        } catch(ClassNotFoundException ex) {
            throw new RuntimeException("Unknown sequence: " + sequenceType + " class: " + className, ex);
        } catch(Exception ex) {
            throw new RuntimeException("Unknown sequence type: " + sequenceType, ex);
        }
    }
    
    /**
     *  Method for creating various types of Sequences based on sequence type & XML parameter definition.
     */
    public final static Sequence createSequence(TransformationGraph graph, String sequenceType, org.w3c.dom.Node nodeXML) {
        Class tClass = getSequenceClass(sequenceType);

        try {
            //create instance of sequence
            Method method = tClass.getMethod(NAME_OF_STATIC_LOAD_FROM_XML, PARAMETERS_FOR_METHOD);
            return (Sequence) method.invoke(null, new Object[] {graph, nodeXML});
        } catch (Exception e) {
			Throwable t = e;
			if (e instanceof InvocationTargetException) {
				t = ((InvocationTargetException) e).getTargetException();
			}
			ComponentXMLAttributes xattribs = new ComponentXMLAttributes((Element) nodeXML, graph);
			String id = xattribs.getString(Node.XML_ID_ATTRIBUTE, null); 
			String name = xattribs.getString(Node.XML_NAME_ATTRIBUTE, null); 
            throw new RuntimeException("Can't create sequence " + GraphElement.identifiersToString(id, name) + ".", t);
        }
    }
    
    /**
     *  Method for creating various types of Sequences based on sequence type, parameters and theirs types for sequence constructor.
     */
    public final static Sequence createSequence(TransformationGraph graph, String sequenceType, Object[] constructorParameters, Class[] parametersType) {
        Class tClass = getSequenceClass(sequenceType);

        try {
            //create instance of sequence
            Constructor constructor = tClass.getConstructor(parametersType);
            return (Sequence) constructor.newInstance(constructorParameters);
        } catch(Exception ex) {
            throw new RuntimeException("Can't create object of : " + sequenceType, ex);
        }
    }

}


