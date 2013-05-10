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

import java.io.File;
import java.io.FilenameFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.ComponentFactory;
import org.jetel.ctl.TLCompilerFactory;
import org.jetel.data.Defaults;
import org.jetel.data.lookup.LookupTableFactory;
import org.jetel.data.sequence.SequenceFactory;
import org.jetel.database.ConnectionFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.dictionary.DictionaryTypeFactory;
import org.jetel.interpreter.extensions.TLFunctionPluginRepository;
import org.jetel.plugin.generalobject.GeneralObjectFactory;
import org.jetel.util.file.FileUtils;
import org.jetel.util.protocols.CustomPathResolverFactory;

/**
 * This class contains all static method, which provide access to all loadable plugins.
 * Firstly call init() method to load and check all version dependencies.
 * 
 * @author Martin Zatopek
 *
 */
public class Plugins {

	private static final String PLUGIN_MANIFEST_FILE_NAME = "plugin.xml";

	static Log logger = LogFactory.getLog(Plugins.class);

    /**
     * Collection of PluginDescriptor(s).
     */
    private static Map<String, PluginDescriptor> pluginDescriptors;

    private static Map<String, PluginDescriptor> activePlugins;
    
    private static Map<String, PluginDescriptor> deactivePlugins;

    private static PluginLocation[] pluginLocations;
    
    /**
     * Whether the class references are actively loaded by the plugin system.
     */
    private static boolean lazyClassLoading = true;

    private static boolean simpleClassLoading = false;

	public static void init() {
        init((String) null);
    }
    
    public static synchronized void init(String directory) {
    	if (directory == null) {
    		init((PluginRepositoryLocation[]) null);
    		return;
    	}
        String[] dirs = directory.split(Defaults.DEFAULT_PATH_SEPARATOR_REGEX);
        List<PluginRepositoryLocation> repositoryLocations = new ArrayList<PluginRepositoryLocation>();
        for (String dir : dirs) {
        	repositoryLocations.add(new PluginRepositoryLocation(new File(dir)));
        }
        init(repositoryLocations.toArray(new PluginRepositoryLocation[repositoryLocations.size()]));
	}
	
	public static synchronized void init(PluginRepositoryLocation[] repositoryLocations) {

        if (repositoryLocations == null) {
        	repositoryLocations = new PluginRepositoryLocation[] { new PluginRepositoryLocation(new File(Defaults.DEFAULT_PLUGINS_DIRECTORY)) };
        }
        
        FilenameFilter pluginManifestFilter = new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.equals(PLUGIN_MANIFEST_FILE_NAME);
			}
		};

		List<PluginLocation> pluginLocations = new ArrayList<PluginLocation>();
		
		for (PluginRepositoryLocation repositoryLocation : repositoryLocations) {
    		File pluginRepositoryPath = repositoryLocation.getLocation();
    		File[] pluginManifestFiles = pluginRepositoryPath.listFiles(pluginManifestFilter);

    		if (pluginManifestFiles != null && pluginManifestFiles.length != 0) {
    			try {
					pluginLocations.add(new PluginLocation(pluginRepositoryPath.toURI().toURL(), repositoryLocation.getClassloader()));
				} catch (MalformedURLException e) {
					logger.error("Plugin at '" + pluginRepositoryPath + "' cannot be loaded.", e);
					continue;
				}
    		} else {
    			File[] pd = pluginRepositoryPath.listFiles();
				if (pd == null) {
					logger.error("Plugins repository '" + pluginRepositoryPath + "' is not available (skipped).");
					continue;
				}
				for (int i = 0; i < pd.length; i++) {
					if (pd[i].isDirectory()) {
						try {
							pluginLocations.add(new PluginLocation(pd[i].toURI().toURL(), repositoryLocation.getClassloader()));
						} catch (MalformedURLException e) {
							logger.error("Plugin at '" + pd[i] + "' cannot be loaded.", e);
							continue;
						}
					}
				}
    		}
        }
		
		PluginLocation[] pls = pluginLocations.toArray(new PluginLocation[pluginLocations.size()]);
        Plugins.init(pls);
    }

    public static synchronized void init(PluginLocation[] pluginLocations) {
        //remove all previous settings
        pluginDescriptors = new HashMap<String, PluginDescriptor>();
        activePlugins = new HashMap<String, PluginDescriptor>();
        deactivePlugins = new HashMap<String, PluginDescriptor>();

        if (pluginLocations == null || pluginLocations.length == 0) {
        	logger.warn("Engine starts without plugins.");
        	if (pluginLocations == null) {
        		pluginLocations = new PluginLocation[0];
        	}
        }
        
        Plugins.pluginLocations = pluginLocations;
        
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
        org.jetel.ctl.extensions.TLFunctionPluginRepository.init();
        DictionaryTypeFactory.init();
        TLCompilerFactory.init();
        GeneralObjectFactory.init();
        CustomPathResolverFactory.init();
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
        for (PluginLocation pluginLocation : pluginLocations) {
        	
    		URL pluginManifestUrl;
    		try {
    			//find a plugin manifest "plugin.xml"
    			pluginManifestUrl = FileUtils.getFileURL(pluginLocation.getLocation(), PLUGIN_MANIFEST_FILE_NAME);
			} catch (MalformedURLException e) {
				logger.error("Plugin '" + pluginLocation.getLocation() + "' is not available (skipped).", e);
				continue;
			}
    		PluginDescriptor pluginDescriptor = new PluginDescriptor(pluginManifestUrl, pluginLocation.getClassloader());
    		try {
    			pluginDescriptor.init();
    		} catch (ComponentNotReadyException e) {
    			//manifest is not parsable
				//logger.error("Plugin manifest '" + pluginManifestUrl + "' is not parsable (skipped).", e);
    			continue;
    		}
    		//stores prepared plugin descriptor
    		if (!pluginDescriptors.containsKey(pluginDescriptor.getId())) {
        		pluginDescriptors.put(pluginDescriptor.getId(), pluginDescriptor);
        		logger.debug("Plugin " + pluginDescriptor.getId() + " loaded.\n" + pluginDescriptor.toString());
    		} else {
        		logger.warn("Plugin at '" + pluginManifestUrl + "' cannot be loaded. An another plugin is already registered with identical id attribute.");
    		}
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

    public static PluginLocation[] getPluginLocations() {
        return pluginLocations;
    }

    public static PluginDescriptor getPluginDescriptor(String pluginId) {
        return pluginDescriptors.get(pluginId);
    }
    
    /**
     * Activate all not yet activated plugins. All already deactivated plugins are skipped.
     * @param lazyClassLoading whether the class references are actively loaded by the plugin system 
     */
    public static synchronized void activateAllPlugins() {
    	for(String pluginId : pluginDescriptors.keySet()) {
    		if(!activePlugins.containsKey(pluginId) && !deactivePlugins.containsKey(pluginId)) {
    			activatePlugin(pluginId);
    		}
    	}
    }
    
    public static synchronized void activatePlugin(String pluginID) {
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

    public static synchronized void deactivatePlugin(String pluginID) {
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
        PluginDescriptor pluginDescriptor = getPluginDescriptor(pluginId);
        if (pluginDescriptor != null) {
        	return pluginDescriptor.isActive();
        } else {
        	throw new IllegalArgumentException("unknown pluginId '" + pluginId + "'");
        }
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
    
    public static boolean isSimpleClassLoading() {
    	return simpleClassLoading;
    }
    
    public static void setSimpleClassLoading(boolean simpleClassLoading) {
    	Plugins.simpleClassLoading = simpleClassLoading;
    }
    
}
