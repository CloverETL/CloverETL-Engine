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

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.ComponentFactory;
import org.jetel.data.lookup.LookupTableFactory;
import org.jetel.data.sequence.SequenceFactory;
import org.jetel.database.ConnectionFactory;
import org.jetel.exception.ComponentNotReadyException;

/**
 * This class contains all static method, which provide access to all loadable plugins.
 * Firstly call init() method to load and check all version dependencies.
 * 
 * @author Martin Zatopek
 *
 */
public class Plugins {

    static Log logger = LogFactory.getLog(Plugins.class);

    /**
     * Collection of PluginDescriptor(s).
     */
    private static Map pluginDescriptors = new HashMap();

    private static Map activePlugins = new HashMap();
    
    private static Map deactivePlugins = new HashMap();

    private static File pluginDirectory;
    
    
    public static void init(String directory) {
        //check plugin directory
        pluginDirectory = new File(directory);
        if(!pluginDirectory.isDirectory()) {
            logger.error("Plugin directory does not exists. (" + directory + ")");
            return;
        }
        
        //create all plugin descriptor
        loadPluginDescription();
        
        //check dependences between plugins
        checkDependences();
        
        //init calls of all factories for components, sequences, lookups and connections
        ComponentFactory.init();
        SequenceFactory.init();
        LookupTableFactory.init();
        ConnectionFactory.init();
    }

    public static List getExtensions(String extensionName) {
        List ret = new ArrayList();
        
        for(Iterator it = pluginDescriptors.values().iterator(); it.hasNext();) {
            PluginDescriptor plugin = (PluginDescriptor) it.next();
            ret.addAll(plugin.getExtensions(extensionName));
        }
        
        return ret;
    }

    private static void loadPluginDescription() {
        File[] pd = pluginDirectory.listFiles();
        for(int i = 0; i < pd.length; i++) {
            if(pd[i].isDirectory()) {
                File[] manifest = pd[i].listFiles(new FilenameFilter() {
                    public boolean accept(File dir, String name) {
                        return name.equals("plugin.xml");
                    }
                });
                if(manifest.length == 1) {
                    PluginDescriptor pluginDescriptor = new PluginDescriptor(manifest[0]);
                    try {
                        pluginDescriptor.init();
                    } catch (ComponentNotReadyException e) {
                        //manifest is not parsable
                        continue;
                    }
                    pluginDescriptors.put(pluginDescriptor.getId(), pluginDescriptor);
                    logger.debug("Plugin " + pluginDescriptor.getId() + " loaded.\n" + pluginDescriptor.toString());
                }
            }
        }
    }

    public static List getClassLoaders(ClassLoader exclude) {
        List ret = new ArrayList();
        
        for(Iterator it = pluginDescriptors.values().iterator(); it.hasNext();) {
            ClassLoader cl = ((PluginDescriptor) it.next()).getClassLoader();
            if(cl != exclude) {
                ret.add(cl);
            }
        }
        
        return ret;
    }
    
    private static void checkDependences() {
        for(Iterator it = pluginDescriptors.values().iterator(); it.hasNext();) {
            ((PluginDescriptor) it.next()).checkDependences();
        }
    }

    public static File getPluginDirectory() {
        return pluginDirectory;
    }

    public static PluginDescriptor getPluginDescriptor(String pluginId) {
        return (PluginDescriptor) pluginDescriptors.get(pluginId);
    }
    
    public static void activatePlugin(String pluginID) {
        //some validation tests
        if(!pluginDescriptors.containsKey(pluginID)) {
            logger.error("Attempt activate unknown plugin: " + pluginID);
            return;
        }
        if(activePlugins.containsKey(pluginID)) {
            logger.error("Attempt activate already active plugin: " + pluginID);
            return;
        }
        if(deactivePlugins.containsKey(pluginID)) {
            logger.error("Attempt activate already deactive plugin: " + pluginID);
            return;
        }
        //activation
        PluginDescriptor pluginDescriptor = (PluginDescriptor) pluginDescriptors.get(pluginID);
        activePlugins.put(pluginID, pluginDescriptor);
        pluginDescriptor.activate();
    }

    public static void deactivatePlugin(String pluginID) {
        //some validation tests
        if(!pluginDescriptors.containsKey(pluginID)) {
            logger.error("Attempt deactivate unknown plugin: " + pluginID);
            return;
        }
        if(!activePlugins.containsKey(pluginID)) {
            logger.error("Attempt deactivate inactive plugin: " + pluginID);
            return;
        }
        if(deactivePlugins.containsKey(pluginID)) {
            logger.error("Attempt deactivate already deactive plugin: " + pluginID);
            return;
        }
        //deactivation
        PluginDescriptor pluginDescriptor = (PluginDescriptor) pluginDescriptors.get(pluginID);
        activePlugins.remove(pluginID);
        deactivePlugins.put(pluginID, pluginDescriptor);
        pluginDescriptor.deactivate();
    }

    /**
     * Checks whether plugin with given pluginId is active.
     * @param pluginId
     * @return
     */
    public static boolean isActive(String pluginId) {
        return getPluginDescriptor(pluginId).isActive();
    }
}
