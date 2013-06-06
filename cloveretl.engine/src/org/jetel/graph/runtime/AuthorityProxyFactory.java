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
package org.jetel.graph.runtime;

//import org.w3c.dom.Node;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.plugin.Extension;
import org.jetel.plugin.PluginDescriptor;
import org.jetel.plugin.Plugins;

/**
 * Factory for authority proxy extension point.
 * Method {@link #createDefaultAuthorityProxy()} creates instance of {@link IAuthorityProxy}
 * with the highest priority. Basic Clover engine is delivered with single {@link PrimitiveAuthorityProxy}.
 * 
 * @see AuthorityProxyDescription
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 30.5.2013
 */
public class AuthorityProxyFactory {

    private static Log logger = LogFactory.getLog(AuthorityProxyFactory.class);

	private final static Map<String, AuthorityProxyDescription> authorityProxyMap = new HashMap<String, AuthorityProxyDescription>();
	
	/**
	 * Populate internal cache of all available authority proxies - proxies from clover plugin system.
	 */
	public static void init() {
        //ask plugin framework for authority proxies
        List<Extension> authorityProxyExtensions = Plugins.getExtensions(AuthorityProxyDescription.EXTENSION_POINT_ID);
        
        //register all compilers
        for (Extension extension : authorityProxyExtensions) {
            try {
            	AuthorityProxyDescription desc = new AuthorityProxyDescription(extension);
            	desc.init();
                registerAuthorityProxy(desc);
            } catch(Exception e) {
                logger.error("Cannot create AuthorityProxy description, extension in plugin manifest is not valid.\n"
                        + "pluginId = " + extension.getPlugin().getId() + "\n" + extension, e);
            }
        }
        
	}
	
	/**
	 * Registers authority proxy descriptor.
	 * @param authorityProxy
	 */
	public final static void registerAuthorityProxy(AuthorityProxyDescription authorityProxy){
		authorityProxyMap.put(authorityProxy.getType(), authorityProxy);
	}
	
    private final static Class<? extends IAuthorityProxy> getAuthorityProxyClass(String authorityProxyType) {
        String className = null;
        AuthorityProxyDescription authorityProxyDescription = authorityProxyMap.get(authorityProxyType);
        
        try {
            if (authorityProxyDescription == null) { 
                //unknown compiler type, we suppose compilerType as full class name classification
                className = authorityProxyType;
                //find class of compiler
                return Class.forName(authorityProxyType).asSubclass(IAuthorityProxy.class); 
            } else {
                className = authorityProxyDescription.getClassName();

                PluginDescriptor pluginDescriptor = authorityProxyDescription.getPluginDescriptor();
                
                //find class of compiler
                return Class.forName(className, true, pluginDescriptor.getClassLoader()).asSubclass(IAuthorityProxy.class);
            }
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException("Unknown AuthorityProxy: " + authorityProxyType + " class: " + className, ex);
        } catch (Exception ex) {
            throw new RuntimeException("Unknown TL compiler type: " + authorityProxyType, ex);
        }

    }
    
	/**
	 * Creates authority proxy instance referenced by its type.
	 * @param authorityProxyType
	 * @return
	 */
	public final static IAuthorityProxy createAuthorityProxy(String authorityProxyType) {
		Class<? extends IAuthorityProxy> tClass = getAuthorityProxyClass(authorityProxyType);
        
        try {
            //create instance of authority proxy
        	return tClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Can't create AuthorityProxy of type " + authorityProxyType, e);
		}
	}

	/**
	 * Creates authority proxy instance with the highest priority.
	 * @return
	 */
	public final static IAuthorityProxy createDefaultAuthorityProxy() {
		int maxPriority = Integer.MIN_VALUE;
		AuthorityProxyDescription maxPriorityAuthorityProxy = null;
		
		for (AuthorityProxyDescription authorityProxyDescription : authorityProxyMap.values()) {
			if (authorityProxyDescription.getPriority() > maxPriority) {
				maxPriorityAuthorityProxy = authorityProxyDescription;
				maxPriority = authorityProxyDescription.getPriority();
			}
		}
		
		if (maxPriorityAuthorityProxy != null) {
			return createAuthorityProxy(maxPriorityAuthorityProxy.getType());
		} else {
            throw new RuntimeException("No AuthorityProxy found.");
		}
	}

}


