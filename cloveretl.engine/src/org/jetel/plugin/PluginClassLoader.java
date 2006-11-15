/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2005-06  Javlin Consulting <info@javlinconsulting.cz>
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
package org.jetel.plugin;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Plugin class loader is descendant of URLClassLoader and is used for particular plugins.
 * Added value is in loading classes from siblings plugins.
 * Fistly try loading class from system, then from ourselfs and finally from its siblings.
 * 
 * @author Martin Zatopek
 *
 */
public class PluginClassLoader extends URLClassLoader {

    private PluginDescriptor pluginDescriptor;

    private PluginDescriptor[] importPlugins;
    
    public PluginClassLoader(ClassLoader parent, PluginDescriptor pluginDescriptor, URL[] urls) {
        super(urls, parent);
        
        this.pluginDescriptor = pluginDescriptor;
        collectImports();
    }

    
    /**
     * Collect all plugin descriptors, on which depeneds this plugin. 
     */
    private void collectImports() {
        PluginDescriptor pluginDescriptor;
        Collection prerequisites = getPluginDescriptor().getPrerequisites();
        List importPlugins = new ArrayList();
        
        for(Iterator it = prerequisites.iterator(); it.hasNext();) {
            PluginPrerequisite prerequisite = (PluginPrerequisite) it.next();
            pluginDescriptor = Plugins.getPluginDescriptor(prerequisite.getPluginId());
            if(pluginDescriptor != null) {
                importPlugins.add(pluginDescriptor);
            }
        }
        
        this.importPlugins = (PluginDescriptor[]) importPlugins.toArray(new PluginDescriptor[importPlugins.size()]);
    }


    protected Class loadClass(String name, boolean resolve) throws ClassNotFoundException {
        Class ret;
        try {
            ret = getParent().loadClass(name);
        } catch (ClassNotFoundException cnfe) {
            ret = loadClass(name, resolve, this, null);
        }
        if (ret != null) {
            return ret;
        }
        throw new ClassNotFoundException(name);
    }
    
    public synchronized Class loadClass(String name, boolean resolve, PluginClassLoader requestor, Set seen) throws ClassNotFoundException {
        //firstly we will try if this class is not loaded yet
        Class ret = null;
        ret = findLoadedClass(name);
        if(ret != null) {
            if (resolve) {
                resolveClass(ret);
            }
            return ret; // found already loaded class in this plug-in
        }
        
        //secondly we try use findClass() method from URLClassLoader
        try {
            ret = findClass(name);
        } catch (ClassNotFoundException cnfe) {
            // ignore
        }
        if(ret != null) {
            if (resolve) {
                resolveClass(ret);
            }
            return ret; // found class in this plug-in
        }

        if(seen == null) {
            seen = new HashSet();
        }
        seen.add(getPluginDescriptor().getId());

        //try all dependant plugins
        for(int i = 0; i < importPlugins.length; i++) {
            if(seen.contains(importPlugins[i].getId())) {
                continue;
            }
            ret = ((PluginClassLoader) importPlugins[i].getClassLoader()).loadClass(name, resolve, requestor, seen);
            if(ret != null) {
                if (resolve) {
                    resolveClass(ret);
                }
                break; // found class in imported plug-in
            }
        }

        return ret;
    }
    
    /**
     * @return returns the plug-in descriptor
     */
    public PluginDescriptor getPluginDescriptor() {
        return pluginDescriptor;
    }

}