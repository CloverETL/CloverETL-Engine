/*
 * Created on Jul 4, 2005
 * GreedyURLClassLoader
 */
package org.jetel.util.classloader;

import java.net.URL;
import java.net.URLClassLoader;

import org.apache.log4j.Logger;
import org.jetel.data.Defaults;
import org.jetel.util.string.StringUtils;

/**
 * Class-loader extended from URL classloader which loads classes (greedily) from specified
 * URLs at first. If unsuccesfull, it tries to load class using parent classloader.
 * 
 * It can use common class-loading strategy (parent class-loader first) for some specified packages.
 * @author misho
 *
 */
public class GreedyURLClassLoader extends URLClassLoader {
	private static Logger log  = Logger.getLogger(GreedyURLClassLoader.class);
	
	/** packages prefixes which are excluded from Greedy class-loading and which will be loaded in common way (parent class-loader first) 
	 * Typically "java." "javax." "sun.misc." etc. */
	public static String[] excludedPackages = StringUtils.split(Defaults.PACKAGES_EXCLUDED_FROM_GREEDY_CLASS_LOADING);

	public GreedyURLClassLoader(URL[] urls) {
		super(urls);
	}

    public GreedyURLClassLoader(URL[] urls,ClassLoader parent) {
        super(urls,parent);
    }
    
	public synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
		// First, check if the class has already been loaded
		Class c = findLoadedClass(name);
		if (c == null) {
			// for specified packages use common strategy - parents first
			if (excludedPackages != null){
				for (String pack : excludedPackages){
					if (name.startsWith(pack))
						return super.loadClass(name, resolve);
				}// for
			}
		    try {
				// try to load the class by ourselves
		        c = findClass(name);
		    } catch (ClassNotFoundException e) {
		    	c = getParent().loadClass(name);
		    } catch (SecurityException se) { // intended to catch java.lang.SecurityException: sealing violation: package oracle.jdbc.driver is sealed at java.net.URLClassLoader.defineClass (URLClassLoader.java:227)
		    	log.warn("GreedyURLClassLoader: cannot load "+name+" due to "+"SecurityException:"+se.getMessage()+" loading from parent class-loader:"+this.getParent());
		    	c = getParent().loadClass(name);
		    }
		}
		if (resolve) {
		    resolveClass(c);
		}
		return c;
	}
	
	public synchronized void addURL(URL urlToAdd) {
		super.addURL(urlToAdd);
	}
	
}