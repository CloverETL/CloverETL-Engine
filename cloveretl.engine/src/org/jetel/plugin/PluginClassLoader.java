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
package org.jetel.plugin;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.jetel.util.classloader.GreedyURLClassLoader;

/**
 * Plugin class loader is descendant of URLClassLoader and is used for particular plugins.
 * Added value is in loading classes from siblings plugins.
 * Fistly try loading class from system, then from ourselfs and finally from its siblings.
 * 
 * @author Martin Zatopek
 *
 */
public class PluginClassLoader extends GreedyURLClassLoader {
	static final Logger log = Logger.getLogger(PluginClassLoader.class);

    private PluginDescriptor pluginDescriptor;
    private PluginDescriptor[] importPlugins;
    
    /**
     * Constructor.
     * @param parent parent class loader
     * @param pluginDescriptor reference to a plugin tided with this class loader
     * @param greedy whether this class loader behaves greedy or not
     * @param excludedPackages package prefixes of classes which are excluded from given loading algorithm (greedy/regular)
     */
    public PluginClassLoader(ClassLoader parent, PluginDescriptor pluginDescriptor, boolean greedy, String[] excludedPackages) {
        super(pluginDescriptor.getLibraryURLs(), parent, excludedPackages, greedy);
        this.pluginDescriptor = pluginDescriptor;
        if (log.isTraceEnabled()) {
        	log.trace("Init "+this+" parent:"+parent+" greedy:"+greedy);
        	if (pluginDescriptor.getLibraryURLs() != null){
        		log.trace("Init "+this+" libraries:"+Arrays.asList(pluginDescriptor.getLibraryURLs()));
        	}
        	if (excludedPackages != null){
        		log.trace("Init "+this+" excludedPackages:"+Arrays.asList(excludedPackages) );
        	}
        }
        collectImports();
    }

    /**
     * Collect all plugin descriptors, on which depends this plugin. 
     */
    private void collectImports() {
        PluginDescriptor pluginDescriptor;
        Collection prerequisites = getPluginDescriptor().getPrerequisites();
        List<PluginDescriptor> importPlugins = new ArrayList<PluginDescriptor>();
        
        for(Iterator it = prerequisites.iterator(); it.hasNext();) {
            PluginPrerequisite prerequisite = (PluginPrerequisite) it.next();
            pluginDescriptor = Plugins.getPluginDescriptor(prerequisite.getPluginId());
            if(pluginDescriptor != null) {
                importPlugins.add(pluginDescriptor);
            }
        }
        
        this.importPlugins = importPlugins.toArray(new PluginDescriptor[importPlugins.size()]);
        if (log.isTraceEnabled())
        	log.trace(this+" importPlugins:"+importPlugins );
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        try {
            return super.findClass(name);
        } catch(ClassNotFoundException e) {
            return findClassInPrerequisities(name, null);
        }
    }
    
    synchronized public Class<?> findClass(String name, Set<String> seen) throws ClassNotFoundException {
        //firstly we will try if this class is not loaded yet
        Class<?> ret = null;
        ret = findLoadedClass(name);
        if(ret != null) {
            return ret; // found already loaded class in this plug-in
        }
        
        //secondly we try use findClass() method from URLClassLoader
        return findClass(name);
    }

   private Class<?> findClassInPrerequisities(String name, Set<String> seen) throws ClassNotFoundException {
       if(seen == null) {
           seen = new HashSet<String>();
       }
       seen.add(getPluginDescriptor().getId());

       //try all dependant plugins
       for(int i = 0; i < importPlugins.length; i++) {
           if(seen.contains(importPlugins[i].getId())) {
               continue;
           }
           try {
               if (log.isTraceEnabled())
            	   log.trace(this+" trying to find:"+ name + " in "+getClassLoaderId(importPlugins[i].getClassLoader()));
               Class<?> cl = ((PluginClassLoader) importPlugins[i].getClassLoader()).findClass(name, seen);
               if (log.isTraceEnabled() && cl != null)
            	   log.trace(this+" class:"+ name + " found by "+getClassLoaderId(cl.getClassLoader()));
               return cl;
           } catch(ClassNotFoundException e) {
               //continue;
           }
       }
       
       throw new ClassNotFoundException(name);
   }
       
    /**
     * @return returns the plug-in descriptor
     */
    public PluginDescriptor getPluginDescriptor() {
        return pluginDescriptor;
    }

    /**
     * @return concatened lists of parent class loader, this class loader and all
     * dependant class loaders URLs (getURLs())
     */
    public URL[] getAllURLs() {
        List<URL> retURLs = new ArrayList<URL>();
        ClassLoader parentClassLoader = getParent();
        
        if(parentClassLoader instanceof URLClassLoader) {
           retURLs.addAll(Arrays.asList(((URLClassLoader) parentClassLoader).getURLs()));
        }
        retURLs.addAll(Arrays.asList(getURLs()));
        
        for(int i = 0; i < importPlugins.length; i++) {
            retURLs.addAll(Arrays.asList(((PluginClassLoader) importPlugins[i].getClassLoader()).getURLs()));
        }
        
        return retURLs.toArray(new URL[retURLs.size()]);
    }
    
    public String toString(){
    	return "PluginClassLoader:"+this.getPluginDescriptor().getId();
    }
}