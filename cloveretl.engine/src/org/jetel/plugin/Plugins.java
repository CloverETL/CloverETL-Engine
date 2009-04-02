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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.ComponentFactory;
import org.jetel.data.Defaults;
import org.jetel.data.lookup.LookupTableFactory;
import org.jetel.data.sequence.SequenceFactory;
import org.jetel.database.ConnectionFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.dictionary.DictionaryTypeFactory;
import org.jetel.interpreter.extensions.TLFunctionPluginRepository;
import org.jetel.util.file.FileUtils;

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
    private static Map<String, PluginDescriptor> pluginDescriptors;

    private static Map<String, PluginDescriptor> activePlugins;
    
    private static Map<String, PluginDescriptor> deactivePlugins;

    private static URL[] pluginDirectories;
    
    /**
     * Whether the class references are actively loaded by the plugin system.
     */
    private static boolean lazyClassLoading = true;
    
	public static void init() {
        init((String) null);
    }
    
    public static void init(String directory) {

        if(directory == null) {
            directory = Defaults.DEFAULT_PLUGINS_DIRECTORY;
        }
        
        //check plugin directories
        List<URL> pluginDirectories = new ArrayList<URL>();
        String[] dirs = directory.split(Defaults.DEFAULT_PATH_SEPARATOR_REGEX);
        for(String dir : dirs) {
    		File pluginRepositoryPath = new File(dir);
    		File[] pd = pluginRepositoryPath.listFiles();
    		if(pd == null) {
    			logger.error("Plugins repository '" + pluginRepositoryPath + "' is not available (skipped).");
    			continue;
    		}
    		for(int i = 0; i < pd.length; i++) {
    			if(pd[i].isDirectory()) {
    				try {
    					pluginDirectories.add(pd[i].toURI().toURL());
	    			} catch (MalformedURLException e) {
	                    logger.error("Plugin directory does not exists. (" + pd[i] + ")");
	    			}
    			}
    		}
        }

        init(pluginDirectories.toArray(new URL[pluginDirectories.size()]));
    }

    public static void init(URL[] pluginsUrls) {
        //remove all previous settings
        pluginDescriptors = new HashMap<String, PluginDescriptor>();
        activePlugins = new HashMap<String, PluginDescriptor>();
        deactivePlugins = new HashMap<String, PluginDescriptor>();

        if(pluginsUrls == null || pluginsUrls.length == 0) {
        	logger.warn("Engine starts without plugins.");
        	if(pluginsUrls == null) {
        		pluginsUrls = new URL[0];
        	}
        }
        
        Plugins.pluginDirectories = pluginsUrls;
        
        //create all plugin descriptor
        loadPluginDescription();
        
        //check dependences between plugins
        checkDependences();
        
        //init calls of all factories for components, sequences, lookups and connections
        ComponentFactory.init();
        SequenceFactory.init();
        LookupTableFactory.init();
        ConnectionFactory.init();
        TLFunctionPluginRepository.init();
        DictionaryTypeFactory.init();
    }
    
    public static Map<String, PluginDescriptor> getPluginDescriptors(){
    	return pluginDescriptors;
    }
    
    public static List<Extension> getExtensions(String extensionName) {
        List<Extension> ret = new ArrayList<Extension>();
        
        for(PluginDescriptor plugin : pluginDescriptors.values()) {
            ret.addAll(plugin.getExtensions(extensionName));
        }
        
        return ret;
    }

    private static void loadPluginDescription() {
    	//iterates over all plugin repositories
        for(URL pluginUrl : pluginDirectories) {
        	
        	
    		URL pluginManifestUrl;
    		try {
    			//find a plugin manifest "plugin.xml"
    			pluginManifestUrl = FileUtils.getFileURL(pluginUrl, "plugin.xml");
			} catch (MalformedURLException e) {
				logger.error("Plugin '" + pluginUrl + "' is not available (skipped).", e);
				continue;
			}
    		PluginDescriptor pluginDescriptor = new PluginDescriptor(pluginManifestUrl);
    		try {
    			pluginDescriptor.init();
    		} catch (ComponentNotReadyException e) {
    			//manifest is not parsable
				//logger.error("Plugin manifest '" + pluginManifestUrl + "' is not parsable (skipped).", e);
    			continue;
    		}
    		//stores prepared plugin descriptor
    		pluginDescriptors.put(pluginDescriptor.getId(), pluginDescriptor);
    		logger.debug("Plugin " + pluginDescriptor.getId() + " loaded.\n" + pluginDescriptor.toString());
        }
    }

    public static List<ClassLoader> getClassLoaders(ClassLoader exclude) {
        List<ClassLoader> ret = new ArrayList<ClassLoader>();
        
        for(PluginDescriptor plugin : pluginDescriptors.values()) {
            ClassLoader cl = plugin.getClassLoader();
            if(cl != exclude) {
                ret.add(cl);
            }
        }
        
        return ret;
    }
    
    private static void checkDependences() {
        for(PluginDescriptor plugin : pluginDescriptors.values()) {
            plugin.checkDependences();
        }
    }

    public static URL[] getPluginDirectories() {
        return pluginDirectories;
    }

    public static PluginDescriptor getPluginDescriptor(String pluginId) {
        return pluginDescriptors.get(pluginId);
    }
    
    /**
     * Activate all not yet activated plugins. All already deactivated plugins are skipped.
     * @param lazyClassLoading whether the class references are actively loaded by the plugin system 
     */
    public static void activateAllPlugins() {
    	for(String pluginId : pluginDescriptors.keySet()) {
    		if(!activePlugins.containsKey(pluginId) && !deactivePlugins.containsKey(pluginId)) {
    			activatePlugin(pluginId);
    		}
    	}
    }
    
    public static void activatePlugin(String pluginID) {
        //some validation tests
        if(!pluginDescriptors.containsKey(pluginID)) {
            logger.error("Attempt activate unknown plugin: " + pluginID);
            return;
        }
        if(activePlugins.containsKey(pluginID)) {
            logger.error("Attempt activate already actived plugin: " + pluginID);
            return;
        }
        if(deactivePlugins.containsKey(pluginID)) {
            logger.error("Attempt activate already deactived plugin: " + pluginID);
            return;
        }
        //activation
        PluginDescriptor pluginDescriptor = pluginDescriptors.get(pluginID);
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
        PluginDescriptor pluginDescriptor = pluginDescriptors.get(pluginID);
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
    
    /**
     * @return whether the class references are actively loaded by the plugin system.
     */
    public static boolean isLazyClassLoading() {
		return lazyClassLoading;
	}

    /**
     * @param lazyClassLoading whether the class references are actively loaded by the plugin system
     * @note has to be invoked before Plugins initialization
     */
    public static void setLazyClassLoading(boolean lazyClassLoading) {
    	Plugins.lazyClassLoading = lazyClassLoading;
    }
    
}
