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
package org.jetel.ctl;

//import org.w3c.dom.Node;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.plugin.Extension;
import org.jetel.plugin.PluginDescriptor;
import org.jetel.plugin.Plugins;

/**
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 17.12.2008
 */
public class TLCompilerFactory {

    private static Log logger = LogFactory.getLog(TLCompilerFactory.class);

	private final static Class<?>[] PARAMETERS_FOR_CONSTURCTOR = new Class<?>[] { TransformationGraph.class, DataRecordMetadata[].class, DataRecordMetadata[].class, String.class };
	private final static Map<String, TLCompilerDescription> compilerMap = new HashMap<String, TLCompilerDescription>();
	
	public static void init() {
        //ask plugin framework for tl compilers
        List<Extension> compilerExtensions = Plugins.getExtensions(TLCompilerDescription.EXTENSION_POINT_ID);
        
        //register all compilers
        for(Extension extension : compilerExtensions) {
            try {
            	TLCompilerDescription desc = new TLCompilerDescription(extension);
            	desc.init();
                registerCompiler(desc);
            } catch(Exception e) {
                logger.error("Cannot create TL compiler description, extension in plugin manifest is not valid.\n"
                        + "pluginId = " + extension.getPlugin().getId() + "\n" + extension, e);
            }
        }
        
	}
	
    public final static void registerCompilers(TLCompilerDescription[] compilers) {
        for(int i = 0; i < compilers.length; i++) {
        	registerCompiler(compilers[i]);
        }
    }
	
	public final static void registerCompiler(TLCompilerDescription compiler){
		compilerMap.put(compiler.getType(), compiler);
	}
	
    
    public final static Class<? extends ITLCompiler> getCompilerClass(String compilerType) {
        String className = null;
        TLCompilerDescription compilerDescription = compilerMap.get(compilerType);
        
        try {
            if(compilerDescription == null) { 
                //unknown compiler type, we suppose compilerType as full class name classification
                className = compilerType;
                //find class of compiler
                return Class.forName(compilerType).asSubclass(ITLCompiler.class); 
            } else {
                className = compilerDescription.getClassName();

                PluginDescriptor pluginDescriptor = compilerDescription.getPluginDescriptor();
                
                //find class of compiler
                return Class.forName(className, true, pluginDescriptor.getClassLoader()).asSubclass(ITLCompiler.class);
            }
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException("Unknown TL compiler: " + compilerType + " class: " + className, ex);
        } catch (Exception ex) {
            throw new RuntimeException("Unknown TL compiler type: " + compilerType, ex);
        }

    }
    
	public final static ITLCompiler createCompiler(String compilerType, TransformationGraph graph, DataRecordMetadata[] inMetadata, DataRecordMetadata[] outMetadata, String encoding) {
		Class<? extends ITLCompiler> tClass = getCompilerClass(compilerType);
        
        try {
            //create instance of compiler
			Constructor<? extends ITLCompiler> constructor = tClass.getConstructor(PARAMETERS_FOR_CONSTURCTOR);
			return constructor.newInstance(new Object[] {graph, inMetadata, outMetadata, encoding});
        } catch(InvocationTargetException e) {
            throw new RuntimeException("Can't create TL compiler of type " + compilerType, e.getTargetException());
        } catch(Exception e) {
            throw new RuntimeException("Can't create TL compiler of type " + compilerType, e);
		}
	}

	public final static ITLCompiler createCompiler(TransformationGraph graph, DataRecordMetadata[] inMetadata, DataRecordMetadata[] outMetadata, String encoding) {
		int maxPriority = Integer.MIN_VALUE;
		TLCompilerDescription maxPriorityCompiler = null;
		
		for (TLCompilerDescription compilerDescription : compilerMap.values()) {
			if (compilerDescription.getPriority() > maxPriority) {
				maxPriorityCompiler = compilerDescription;
				maxPriority = compilerDescription.getPriority();
			}
		}
		
		if (maxPriorityCompiler != null) {
			return createCompiler(maxPriorityCompiler.getType(), graph, inMetadata, outMetadata, encoding);
		} else {
            logger.error("No TL compiler found.");
            throw new RuntimeException("No TL compiler found.");
		}
	}

}


