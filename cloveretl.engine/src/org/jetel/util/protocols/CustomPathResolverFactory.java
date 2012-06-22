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
package org.jetel.util.protocols;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.plugin.Extension;
import org.jetel.plugin.Plugins;
import org.jetel.util.file.CustomPathResolver;
import org.jetel.util.file.FileUtils;

/**
 * @author dpavlis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Jun 21, 2012
 */
public class CustomPathResolverFactory {

	 private static Log logger = LogFactory.getLog(CustomPathResolverFactory.class);

	 //   private final static Map<String, CustomPathResolverDescriptior> resolverMap = new HashMap<String, CustomPathResolverDescriptior>();
	    
	    public static void init() {
	        //ask plugin framework for connections
	        List<Extension> resolverExtensions = Plugins.getExtensions(CustomPathResolverDescriptior.EXTENSION_POINT_ID);
	        
	        //register all connection
	        for(Extension extension : resolverExtensions) {
	            try {
	            	Class<?> resolverClass = getPathResolverClass(new CustomPathResolverDescriptior(extension));
	            	Object resolverObj = resolverClass.newInstance();
	            	if (resolverObj instanceof CustomPathResolver){
	            		FileUtils.addCustomPathResolver((CustomPathResolver)resolverObj);
	            	}else{
	            		logger.error("Cannot register custom path description, class does not implement "+CustomPathResolver.class.getName()+"interface.\n"
		                        + "pluginId = " + extension.getPlugin().getId() + "\n"
		                        + "extenstion - " + extension);
	            	}
	            	
	            } catch(Exception e) {
	                logger.error("Cannot create custom path description, extension in plugin manifest is not valid.\n"
	                        + "pluginId = " + extension.getPlugin().getId() + "\n"
	                        + "extenstion - " + extension);
	            }
	        }
	        
	    }
	  
	    /**
	     * @param connectionType
	     * @return class from the given connection type
	     */
	    private final static Class<?> getPathResolverClass(CustomPathResolverDescriptior descriptor) {
	        try {
	                return Class.forName(descriptor.getClassName(), true, descriptor.getPluginDescriptor().getClassLoader());
	        } catch(ClassNotFoundException ex) {
	            logger.error("Unknown custom path resolver: " + descriptor.getType() + " class: " + descriptor.getClassName());
	            throw new RuntimeException("Unknown custom path resolver: " + descriptor.getType() + " class: " + descriptor.getClassName());
	        } catch(Exception ex) {
	            logger.error("Unknown custom path resolver type: " + descriptor.getClassName());
	            throw new RuntimeException("Unknown custom path resolver type: " + descriptor.getClassName());
	        }
	    }
}
