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
package org.jetel.data;

import java.util.List;

import org.apache.log4j.Logger;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.plugin.Extension;
import org.jetel.plugin.Plugins;

/**
 * Parent class for all pluginable item descriptors.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 18.3.2009
 */
public abstract class PluginableItemDescriptionImpl implements PluginableItemDescription {
	private static Logger log = Logger.getLogger(PluginableItemDescriptionImpl.class);
	
	private Extension extension;

	public PluginableItemDescriptionImpl(Extension extension) {
    	this.extension = extension;
	}
    
    @Override
	public void init() throws ComponentNotReadyException {
        if (!Plugins.isLazyClassLoading()) {
            //class references should be preloaded
        	preloadClass();
        }
    }

    @Override
	public Extension getExtension() {
		return extension;
	}

    abstract protected List<String> getClassNames();

    /**
     * Just pre-loads a class reference of this graph element description.
     * It is necessary for the clover server.
     * @throws ComponentNotReadyException 
     */
    private void preloadClass() throws ComponentNotReadyException {
    	List<String> classNames = getClassNames();
    	
		for (String className : classNames) {
			log.debug("loading class "+className);
	    	try {
	    		Class.forName(className, true, extension.getPlugin().getClassLoader());
			} catch (ClassNotFoundException e) {
				throw new ComponentNotReadyException("Unable to preload class '" + className + "' registred in extension: " + extension);
			}
		}
    }

}
