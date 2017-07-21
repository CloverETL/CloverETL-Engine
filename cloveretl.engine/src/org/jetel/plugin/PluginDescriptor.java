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
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.naming.directory.InvalidAttributesException;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.util.file.FileUtils;
import org.jetel.util.string.StringUtils;
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
     * Whether greedy classloader should be used for this plugin.
     * 'Greedy' means, that class loading does not follow standard parent-first strategy,
     * it uses self-first strategy instead. 
     * (current classloader is preferred for class name resolution).
     */
    private boolean greedyClassLoader = false;
    
    /**
	 * List of package prefixes which are excluded from greedy/regular class loading. i.e. "java." "javax." "sun.misc." etc.
	 * Prevents GreedyClassLoader from loading standard java interfaces and classes from third-party libs.
     */
    private String[] excludedPackages; 

    /**
     * List of all requires plugins.
     */
    private List<PluginPrerequisite> prerequisites;
    
    /**
     * List of all class path. Definition for plugin class loader. 
     */
    private List<String> libraries;

    /**
     * List of all native library directories. It is used to extend system property java.library.path variable. 
     */
    private List<String> nativeLibraries;

    /**
     * List of all imlemented extensions points by this plugin.
     */
    private List<Extension> extensions;

    /**
     * ClassLoader for this plugin. Is defined base on all libraries.
     */
    private ClassLoader classLoader;

    /**
     * This class loader is optional and is used as a parent for {@link PluginClassLoader}.
     * This feature is used for example by clover designer in engine initialization.
     * @see GuiPlugin
     */
    private ClassLoader parentClassLader;
    
    /**
     * Instance of plugin described by this desriptor.
     * If the plugin is not active, is <b>null</b>.
     */
    private PluginActivator pluginActivator;
    
    /**
     * Link to manifest file (plugin.xml).
     */
    private URL manifest;
    
    
    /**
     * Is true if plugin is already active. 
     */
    private boolean isActive = false;
    
    /**
     * @param manifest
     * @param parentClassLoader can be null
     */
    public PluginDescriptor(URL manifest, ClassLoader parentClassLoader) {
        this.manifest = manifest; 
        this.parentClassLader = parentClassLoader;
        
        prerequisites = new ArrayList<PluginPrerequisite>();
        libraries = new ArrayList<String>();
        nativeLibraries = new ArrayList<String>();
        extensions = new ArrayList<Extension>();
    }

    public void init() throws ComponentNotReadyException {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setCoalescing(true);
        Document doc;
        
        try {
            doc = dbf.newDocumentBuilder().parse(manifest.openStream());
        } catch (SAXException e) {
            throw new ComponentNotReadyException("Parse error occure in plugin manifest reading - " + manifest + ".", e);
        } catch (IOException e) {
            throw new ComponentNotReadyException("IO error occure in plugin manifest reading - " + manifest + ".", e);
        } catch (ParserConfigurationException e) {
            throw new ComponentNotReadyException("Parse error occure in plugin manifest reading - " + manifest+ ".", e);
        }
        
        PluginDescriptionBuilder builder = new PluginDescriptionBuilder(this);
        try {
            builder.read(doc);
        } catch (InvalidAttributesException e) {
            throw new ComponentNotReadyException("Parse error occure in plugin manifest reading - " + manifest + ".", e);
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

    public boolean isGreedyClassLoader() {
    	return greedyClassLoader;
    }

    public void setGreedyClassLoader(boolean greedyClassLoader) {
    	this.greedyClassLoader = greedyClassLoader;
    }
    
    public String[] getExcludedPackages() {
    	return excludedPackages;
    }

    public void setExcludedPackages(String[] excludedPackages) {
    	this.excludedPackages = excludedPackages;
    }

    public String getPluginClassName() {
        return pluginClassName;
    }

    public void setPluginClassName(String className) {
        this.pluginClassName = className;
    }

    public URL getManifest() {
        return manifest;
    }

    public void setManifest(URL manifest) {
        this.manifest = manifest;
    }

    public String getVersion() {
        return version;
    }

	/**
	 * @return major version parsed out from {@link #getVersion()}
	 */
	public int getMajorVersion() {
		String[] versionSegments = getVersionSegments();
		return Integer.valueOf(versionSegments[0]);
	}

	/**
	 * @return minor version parsed out from {@link #getVersion()}
	 */
	public int getMinorVersion() {
		String[] versionSegments = getVersionSegments();
		return Integer.valueOf(versionSegments[1]);
	}
	
	private String[] getVersionSegments() {
		PluginDescriptor pluginDescriptor = Plugins.getPluginDescriptor(getId());
		String version = pluginDescriptor.getVersion();
		String[] subversions = version.split("\\."); //$NON-NLS-1$
		return subversions;
	}

    public void setVersion(String version) {
        this.version = version;
    } 

    public ClassLoader getClassLoader() {
        if(!isActive()) {
            activatePlugin();
//            logger.warn("PluginDescription.getClassLoader(): Plugin " + getId() + " is not already active.");
//            return null;
        }
        if (classLoader == null) {
        	ClassLoader realParentCL = parentClassLader != null ? parentClassLader : PluginDescriptor.class.getClassLoader();
        	if (Plugins.isSimpleClassLoading()) {
        		classLoader = realParentCL;
        	} else {
        		classLoader = new PluginClassLoader(realParentCL, this, greedyClassLoader, excludedPackages);
        	}
        }
        return classLoader;
    }
    
    public URL[] getLibraryURLs() {
        URL[] urls = new URL[libraries.size()];
        
        for(int i = 0; i < libraries.size(); i++) {
            try {
                urls[i] = getURL(libraries.get(i));
            } catch (MalformedURLException e) {
                logger.error("Cannot create URL to plugin (" + getManifest() + ") library " + libraries.get(i) + ".");
                urls[i] = null;
            }
        }
        
        return urls;
    }
    
    /**
     * Converts path relative to the plugin home directory.
     * @param path
     * @return
     * @throws MalformedURLException
     */
    public URL getURL(String path) throws MalformedURLException {
        return FileUtils.getFileURL(manifest, path);
    }
    
    public Extension addExtension(String pointId) {
        Extension ret = new Extension(pointId, this); 
        extensions.add(ret);
        return ret;
    }

    public List<Extension> getExtensions(String pointId) {
        List<Extension> ret = new ArrayList<Extension>();
        for(Extension extension : extensions) {
            if(extension.getPointId().equals(pointId)) {
                ret.add(extension);
            }
        }
        return ret;
    }
    
    public List<Extension> getExtensions() {
    	return Collections.unmodifiableList(extensions);
    }
    
    public void addLibrary(String library) {
        libraries.add(library);
    }

    public void addNativeLibrary(String nativeLibrary) {
        nativeLibraries.add(nativeLibrary);
    }

    public void addPrerequisites(String pluginId, String pluginVersion, String match) {
        prerequisites.add(new PluginPrerequisite(pluginId, pluginVersion, match));
    }

    public List<PluginPrerequisite> getPrerequisites() {
        return prerequisites;
    }
    
    public void checkDependences() {
        for(PluginPrerequisite prerequisite : prerequisites) {
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
        isActive = true;
        
        //first, we activate all prerequisites plugins
        for(Iterator it = getPrerequisites().iterator(); it.hasNext();) {
            PluginPrerequisite prerequisite = (PluginPrerequisite) it.next();
            if(!Plugins.isActive(prerequisite.getPluginId())) {
                Plugins.activatePlugin(prerequisite.pluginId);
            }
        }
        //invoke plugin activator
        pluginActivator = instantiatePlugin();
        if(pluginActivator != null) {
            pluginActivator.activate();
        }
        //update java.library.path according native libraries
        applyNativeLibraries();
    }

    /**
     * This method is called only from Plugins.deactivatePlugin() method.
     */
    protected void deactivate() {
        isActive = false;
        if(pluginActivator != null) {
            pluginActivator.deactivate();
        }
    }

    public boolean isActive() {
        return isActive;
    }

    /**
     * Create instance of plugin class.
     */
    private PluginActivator instantiatePlugin() {
        if(!StringUtils.isEmpty(getPluginClassName())) {
            try {
                Class pluginClass = Class.forName(getPluginClassName(), true, getClassLoader());
                Object plugin = pluginClass.newInstance();
                if(!(plugin instanceof PluginActivator)) {
                    logger.error("Plugin " + getId() + " activation message: Plugin class is not instance of Plugin ascendant - " + getPluginClassName());
                    return null;
                }
                //i have got plugin instance
                return (PluginActivator) plugin;
            } catch (ClassNotFoundException e) {
                logger.error("Plugin " + getId() + " activation message: Plugin class does not found - " + getPluginClassName());
            } catch (Exception e) {
                logger.error("Plugin " + getId() + " activation message: Cannot create plugin instance - " + getPluginClassName() + " - class is abstract, interface or its nullary constructor is not accessible.", e);
            }
        }
        
        return null;
    }
    
    @Override
	public String toString() {
        StringBuffer ret = new StringBuffer();
        
        ret.append("\tid - " + getId() + "\n");
        ret.append("\tversion - " + getVersion() + "\n");
        ret.append("\tprovider-name - " + getProviderName() + "\n");
        
        for(Extension extension : extensions) {
            ret.append("\t\t" + extension + "\n");
            //ret.append("\t\tpoint-id  - " + extension.getPointId() + " - " + extension.getParameters() + "\n" );
        }
        
        return ret.toString();
    }

    /**
     * This is attempt to change 'java.library.path' system property in runtime.
     * Success of this attempt is not guaranteed. We are trying to administer
     * loading of dlls in runtime.
     * This method should be externalized to a seperated utils class.
     */
    private void applyNativeLibraries() {
    	if (nativeLibraries.size() > 0) {
			// Reset the "sys_paths" field of the ClassLoader to null.
			try {
				Class clazz = ClassLoader.class;
				Field field = clazz.getDeclaredField("sys_paths");
				boolean accessible = field.isAccessible();
				if (!accessible)
					field.setAccessible(true);
				field.set(clazz, null);
				field.setAccessible(accessible);
	
				// Change the value of system property java.library.path
				String pathSepartor = System.getProperty("path.separator");
				StringBuilder additionalLibraryPathes = new StringBuilder();
				for (String nativeLibrary : nativeLibraries) {
					additionalLibraryPathes.append(pathSepartor);
					nativeLibrary = URLDecoder.decode((new File(FileUtils.getFile(manifest, nativeLibrary))).getAbsolutePath(), "UTF-8");
					additionalLibraryPathes.append(nativeLibrary);
				}
				if (additionalLibraryPathes.length() > 0) {
					System.setProperty("java.library.path", 
							System.getProperty("java.library.path") 
							+ additionalLibraryPathes);
				}
			} catch (Throwable e) {
				logger.warn("Probably non-standard jvm is used. The native libraries in '" + getId() + "' plugin are ignored.");
			}
    	}
    }
    
}

