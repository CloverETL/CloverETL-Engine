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

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
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
import org.jetel.database.jdbc.JdbcDriverFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.dictionary.DictionaryEntryFactory;
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
        	try {
				pluginDirectories.add(new File(dir).toURL());
			} catch (MalformedURLException e) {
                logger.error("Plugin directory does not exists or is not directory. (" + directory + ")");
			}
        }

        init(pluginDirectories.toArray(new URL[pluginDirectories.size()]));
    }

    public static void init(URL[] pluginUrls) {
        //remove all previous settings
        pluginDescriptors = new HashMap<String, PluginDescriptor>();
        activePlugins = new HashMap<String, PluginDescriptor>();
        deactivePlugins = new HashMap<String, PluginDescriptor>();

        if(pluginUrls == null || pluginUrls.length == 0) {
        	logger.warn("Engine starts with no plugins repository.");
        	if(pluginUrls == null) {
        		pluginUrls = new URL[0];
        	}
        }
        
        Plugins.pluginDirectories = pluginUrls;
        
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
        JdbcDriverFactory.init();
        DictionaryEntryFactory.init();
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
        for(URL pluginsRepositoryUrl : pluginDirectories) {
        	//url uses file protocol - take advantage of ability to list a directory in a harddrive
        	if(pluginsRepositoryUrl.getProtocol() == "file") {
        		File pluginRepositoryPath = new File(pluginsRepositoryUrl.getPath());
        		File[] pd = pluginRepositoryPath.listFiles();
        		if(pd == null) {
        			logger.error("Plugins repository '" + pluginRepositoryPath + "' is not available (skipped).");
        			continue;
        		}
        		for(int i = 0; i < pd.length; i++) {
        			if(pd[i].isDirectory()) {
        				File[] manifest = pd[i].listFiles(new FilenameFilter() {
        					public boolean accept(File dir, String name) {
        						return name.equals("plugin.xml");
        					}
        				});
        				if(manifest.length == 1) {
        					PluginDescriptor pluginDescriptor;
							try {
								pluginDescriptor = new PluginDescriptor(manifest[0].toURL());
							} catch (MalformedURLException e1) {
								logger.error("Plugin manifest is not available for '" + pd[i] + "' plugin.");
								continue;
							}
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
        	} else {
            	//url uses another protocol 
	        	URL pluginListUrl;
				try {
					//try to find "pluginlist" file with a list of all contained plugins
					pluginListUrl = FileUtils.getFileURL(pluginsRepositoryUrl, "pluginlist");
		        	BufferedReader br = new BufferedReader(new InputStreamReader(pluginListUrl.openStream()));
		        	String pluginId;
		        	//process each plugin
		        	while((pluginId = br.readLine()) != null) {
		        		URL pluginManifestUrl;
		        		try {
		        			//find a plugin manifest "plugin.xml"
		        			pluginManifestUrl = FileUtils.getFileURL(pluginListUrl, pluginId + "/plugin.xml");
		    			} catch (MalformedURLException e) {
		    				logger.error("Plugin '" + pluginId + "' is not available (skipped).", e);
		    				continue;
		    			}
		        		PluginDescriptor pluginDescriptor = new PluginDescriptor(pluginManifestUrl);
		        		try {
		        			pluginDescriptor.init();
		        		} catch (ComponentNotReadyException e) {
		        			//manifest is not parsable
		        			continue;
		        		}
		        		//stores prepared plugin descriptor
		        		pluginDescriptors.put(pluginDescriptor.getId(), pluginDescriptor);
		        		logger.debug("Plugin " + pluginDescriptor.getId() + " loaded.\n" + pluginDescriptor.toString());
		        	}
				} catch (MalformedURLException e) {
					logger.error("Plugins repository '" + pluginsRepositoryUrl + "' is not valid URL (skipped).", e);
				} catch (IOException e) {
					logger.error("Plugins repository '" + pluginsRepositoryUrl + "' does not contain pluginlist file (skipped).", e);
				}
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

    public static URL[] getPluginDirectories() {
        return pluginDirectories;
    }

    public static PluginDescriptor getPluginDescriptor(String pluginId) {
        return pluginDescriptors.get(pluginId);
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
}
