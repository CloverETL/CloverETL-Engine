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
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import javax.naming.directory.InvalidAttributesException;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.util.StringUtils;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/**
 * This class describes plugin. Major part information is from xml manifest of plugin.
 * @author Martin Zatopek
 *
 */
public class PluginDescriptor {
    
    static Log logger = LogFactory.getLog(Plugins.class);

    /**
     * Identifier of plugin.
     */
    private String id;
    
    /**
     * Plugin version.
     */
    private String version;
    
    /**
     * Name of provider.
     */
    private String providerName;
    
    /**
     * Name of plugin root class.
     */
    private String pluginClassName;
    
    /**
     * List of all requires plugins.
     */
    private List prerequisites;
    
    /**
     * List of all class path. Definition for plugin class loader. 
     */
    private List libraries;
    
    /**
     * List of all imlemented extensions points by this plugin.
     */
    private List extensions;

    /**
     * ClassLoader for this plugin. Is defined base on all libraries.
     */
    private ClassLoader classLoader;

    /**
     * Instance of plugin described by this desriptor.
     * If the plugin is not active, is <b>null</b>.
     */
    private Plugin plugin;
    
    /**
     * Link to manifest file (plugin.xml).
     */
    private File manifest;
    
    
    /**
     * Is true if plugin is already active. 
     */
    private boolean isActive = false;
    
    public PluginDescriptor(File manifest) {
        this.manifest = manifest; 
        
        prerequisites = new ArrayList();
        libraries = new ArrayList();
        extensions = new ArrayList();
    }

    public void init() throws ComponentNotReadyException {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setCoalescing(true);
        Document doc;
        
        try {
            doc = dbf.newDocumentBuilder().parse(manifest);
        } catch (SAXException e) {
            logger.error("Parse error occure in plugin manifest reading - " + manifest.getAbsolutePath() + ". (" + e.getMessage() + ")");
            throw new IllegalArgumentException();
        } catch (IOException e) {
            logger.error("IO error occure in plugin manifest reading - " + manifest.getAbsolutePath() + ". (" + e.getMessage() + ")");
            throw new IllegalArgumentException();
        } catch (ParserConfigurationException e) {
            logger.error("Parse error occure in plugin manifest reading - " + manifest.getAbsolutePath() + ". (" + e.getMessage() + ")");
            throw new IllegalArgumentException();
        }
        
        PluginDescriptionBuilder builder = new PluginDescriptionBuilder(this);
        try {
            builder.read(doc);
        } catch (InvalidAttributesException e) {
            logger.error("Parse error occure in plugin manifest reading - " + manifest.getAbsolutePath() + ". (" + e.getMessage() + ")");
            throw new ComponentNotReadyException("");
        }
    }
    
    ///////////////////////
    //getters and setters//
    ///////////////////////
    
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getProviderName() {
        return providerName;
    }

    public void setProviderName(String providerName) {
        this.providerName = providerName;
    }

    public String getPluginClassName() {
        return pluginClassName;
    }

    public void setPluginClassName(String className) {
        this.pluginClassName = className;
    }

    public File getManifest() {
        return manifest;
    }

    public void setManifest(File manifest) {
        this.manifest = manifest;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    } 

    public ClassLoader getClassLoader() {
        if(!isActive()) {
            logger.warn("PluginDescription.getClassLoader(): Plugin " + getId() + " is not already active.");
            return null;
        }
        if(classLoader == null) {
            classLoader = new PluginClassLoader(PluginDescriptor.class.getClassLoader(), this);
        }
        return classLoader;
    }
    
    public URL[] getLibraryURLs() {
        URL[] urls = new URL[libraries.size()];
        
        for(int i = 0; i < libraries.size(); i++) {
            File f = new File(manifest.getParentFile().getAbsolutePath() + System.getProperty("file.separator") + libraries.get(i));
            try {
                urls[i] = f.toURL();
            } catch (MalformedURLException e) {
                logger.error("Cannot create URL to plugin (" + getManifest() + ") library " + libraries.get(i) + ".");
            }
        }
        
        return urls;
    }
    
    public void addExtension(String pointId, Properties parameters) {
        extensions.add(new Extension(pointId, parameters, this));
    }

    public List getExtensions(String pointId) {
        List ret = new ArrayList();
        for(Iterator it = extensions.iterator(); it.hasNext();) {
            Extension extension = (Extension) it.next();
            if(extension.getPointId().equals(pointId)) {
                ret.add(extension);
            }
        }
        return ret;
    }
    
    public void addLibrary(String library) {
        libraries.add(library);
    }
    
    public void addPrerequisites(String pluginId, String pluginVersion, String match) {
        prerequisites.add(new PluginPrerequisite(pluginId, pluginVersion, match));
    }

    public List getPrerequisites() {
        return prerequisites;
    }
    
    public void checkDependences() {
        for(Iterator it = prerequisites.iterator(); it.hasNext();) {
            PluginPrerequisite prerequisite = (PluginPrerequisite) it.next();
            if(Plugins.getPluginDescriptor(prerequisite.getPluginId()) == null) {
                logger.error("Plugin " + getId() + " depend on unknown plugin " + prerequisite.getPluginId());
            }
        }
    }

    /**
     * Activate this plugin. Method registers this plugin description in Plugins class as active plugin.
     */
    public void activatePlugin() {
        Plugins.activatePlugin(getId());
    }
    
    /**
     * Deactivate this plugin. Method registers this plugin description in Plugins class as deactive plugin.
     */
    public void deactivatePlugin() {
        Plugins.deactivatePlugin(getId());
    }
    
    /**
     * This method is called only from Plugins.activatePlugin() method.
     */
    protected void activate() {
        //first, we activate all prerequisites plugins
        for(Iterator it = getPrerequisites().iterator(); it.hasNext();) {
            PluginPrerequisite prerequisite = (PluginPrerequisite) it.next();
            if(!Plugins.isActive(prerequisite.getPluginId())) {
                Plugins.activatePlugin(prerequisite.pluginId);
            }
        }
        isActive = true;
        instantiatePlugin();
        if(plugin != null) {
            plugin.activate();
        }
    }

    /**
     * This method is called only from Plugins.deactivatePlugin() method.
     */
    protected void deactivate() {
        isActive = false;
        if(plugin != null) {
            plugin.deactivate();
        }
    }

    public boolean isActive() {
        return isActive;
    }

    /**
     * Create instance of plugin class.
     */
    private void instantiatePlugin() {
        if(!StringUtils.isEmpty(getPluginClassName())) {
            try {
                Class pluginClass = Class.forName(getPluginClassName(), true, getClassLoader());
                Object plugin = pluginClass.newInstance();
                if(!(plugin instanceof Plugin)) {
                    logger.error("Plugin " + getId() + " activation message: Plugin class is not instance of Plugin ascendant - " + getPluginClassName());
                    return;
                }
                //i have got plugin instance
                this.plugin = (Plugin) plugin;
            } catch (ClassNotFoundException e) {
                logger.error("Plugin " + getId() + " activation message: Plugin class does not found - " + getPluginClassName());
            } catch (Exception e) {
                logger.error("Plugin " + getId() + " activation message: Cannot create plugin instance - " + getPluginClassName() + " - class is abstract, interface or its nullary constructor is not accessible.");
            }
        }
    }
    
    public String toString() {
        StringBuffer ret = new StringBuffer();
        
        ret.append("\tid - " + getId() + "\n");
        ret.append("\tversion - " + getVersion() + "\n");
        ret.append("\tprovider-name - " + getProviderName() + "\n");
        
        for(Iterator it = extensions.iterator(); it.hasNext();) {
            Extension extension = (Extension) it.next();
            
            ret.append("\t\tpoint-id  - " + extension.getPointId() + " - " + extension.getParameters() + "\n" );
        }
        
        return ret.toString();
    }

}

