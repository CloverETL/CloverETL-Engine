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
package org.jetel.util.classloader;

import java.net.URL;
import java.net.URLClassLoader;

import org.apache.log4j.Logger;
import org.jetel.data.Defaults;
import org.jetel.util.string.StringUtils;

/**
 * Class-loader extended from URL classloader which by default loads classes and finds resources
 * (greedily) from specified URLs at first. If unsuccessful, it tries to load class or to find resource
 * using parent classloader. Specified packages are excluded from greedy class loading.
 * 
 * On the other hand, class loader can behave like ordinary class loader and only the specified packages
 * are loaded greedily.
 * 
 * @author misho
 *
 */
public class GreedyURLClassLoader extends URLClassLoader {
	private static Logger log  = Logger.getLogger(GreedyURLClassLoader.class);
	
	/** packages prefixes which are excluded from Greedy class-loading and which will be loaded in common way (parent class-loader first) 
	 * Typically "java." "javax." "sun.misc." etc. */
	public static String[] excludedPackages = StringUtils.split(Defaults.PACKAGES_EXCLUDED_FROM_GREEDY_CLASS_LOADING);

	protected String[] clExcludedPackages = null; 
	
	/**
	 * Is this class loader greedy by default? 
	 */
	protected boolean greedy = true;
	
	/**
	 * Greedy class loader is created.
	 * @param urls the URLs from which to load classes and resources
	 */
	public GreedyURLClassLoader(URL[] urls) {
		super(urls);
        this.clExcludedPackages = GreedyURLClassLoader.excludedPackages;
	}

    /**
     * Greedy class loader is created.
     * @param urls the URLs from which to load classes and resources
     * @param parent the parent class loader for delegation 
     */
    public GreedyURLClassLoader(URL[] urls,ClassLoader parent) {
		super(urls,parent);
        this.clExcludedPackages = GreedyURLClassLoader.excludedPackages;
    }

    /**
     * Greedy class loader, where classes from specified packages are loaded by regular algorithm (parent-first pattern).
     * @param urls the URLs from which to load classes and resources
     * @param parent the parent class loader for delegation 
     * @param excludedPackages package prefixes which are excluded from greedy loading
     */
    public GreedyURLClassLoader(URL[] urls,ClassLoader parent, String[] excludedPackages) {
		super(urls,parent);
        this.clExcludedPackages = excludedPackages;
    }

    /**
     * Greedy/regular class loader, where classes from specified packages are loaded by regular/greedy algorithm.
     * @param urls the URLs from which to load classes and resources
     * @param parent the parent class loader for delegation 
     * @param excludedPackages package prefixes which are excluded from greedy/regular loading
     * @param greedy whther this class loader is greedy (or regular)
     */
    public GreedyURLClassLoader(URL[] urls,ClassLoader parent, String[] excludedPackages, boolean greedy) {
		super(urls,parent);
        this.clExcludedPackages = excludedPackages;
        this.greedy = greedy;
    }

	@Override
	protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
		boolean useGreedyAlgorithm = greedy;
		
		// for specified packages use inverse strategy - parents first
		if (clExcludedPackages != null){
			for (String pack : clExcludedPackages){
				if (name.startsWith(pack)) {
					useGreedyAlgorithm = !useGreedyAlgorithm;
					break;
				}
			}
		}
	
		if (useGreedyAlgorithm) {
			Class c = loadClassGreedy(name, resolve);
			return c;
		} else {
	        if (log.isTraceEnabled())
	      	   log.trace(this+" P-F loading: "+ name);
			Class c = super.loadClass(name, resolve);
	        if (log.isTraceEnabled())
	           log.trace(this+" P-F loaded:  "+ name+" by: "+getClassLoaderId(c.getClassLoader()));
			return c;
		}
	}

	protected synchronized Class<?> loadClassGreedy(String name, boolean resolve) throws ClassNotFoundException {
		// First, check if the class has already been loaded
		Class c = findLoadedClass(name);
		if (c == null) {
		    try {
				// try to load the class by ourselves
		        if (log.isTraceEnabled())
			      	   log.trace(this+" S-F loading: "+ name);
		        c = findClass(name);
		        if (log.isTraceEnabled())
			      	   log.trace(this+" S-F loaded:  "+ name + " by: "+getClassLoaderId(c.getClassLoader()));
		    } catch (ClassNotFoundException e) {
		        if (log.isTraceEnabled())
			      	   log.trace(this+" S-F loading: "+ name + " by: "+getClassLoaderId(getParent()));
		    	c = getParent().loadClass(name);
		        if (log.isTraceEnabled())
			      	   log.trace(this+" S-F loaded:  "+ name + " by: "+getClassLoaderId(c.getClassLoader()));
		    } catch (SecurityException se) { // intended to catch java.lang.SecurityException: sealing violation: package oracle.jdbc.driver is sealed at java.net.URLClassLoader.defineClass (URLClassLoader.java:227)
		    	log.warn("GreedyURLClassLoader: cannot load "+name+" due to SecurityException loading from parent class-loader:"+this.getParent(), se);
		        if (log.isTraceEnabled())
			      	   log.trace(this+" S-F loading: "+ name + " by parent: "+getClassLoaderId(getParent()));
		    	c = getParent().loadClass(name);
		        if (log.isTraceEnabled())
			      	   log.trace(this+" S-F loaded:  "+ name + " by: "+getClassLoaderId(c.getClassLoader()));
		    }
		}
		if (resolve) {
		    resolveClass(c);
		}
		return c;
	}

	/**
	 * Returns some readable ID of specified class-loader usable for logging messages.
	 * @param parent
	 * @return
	 */
	protected String getClassLoaderId(ClassLoader cl) {
		if (cl == null) {
			return "null";
		} else if (cl instanceof GreedyURLClassLoader) {
			return cl.toString();
		} else {
			return cl.getClass().toString()+"#"+cl.hashCode();
		}
	}

	@Override
	public synchronized void addURL(URL urlToAdd) {
		super.addURL(urlToAdd);
	}

	@Override
	public URL getResource(String name) {
		if (greedy) {
			URL url = findResource(name);
			if (url != null) {
				return url;
			}
		}
		
		return super.getResource(name);
	}

}