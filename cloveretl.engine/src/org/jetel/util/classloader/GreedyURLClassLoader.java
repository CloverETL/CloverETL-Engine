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

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.jetel.data.Defaults;
import org.jetel.util.CompoundEnumeration;
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
public class GreedyURLClassLoader extends URLClassLoader implements ClassDefinitionFactory, URLBasedClassLoader {
	private static Logger log  = Logger.getLogger(GreedyURLClassLoader.class);
	
	/** packages prefixes which are excluded from Greedy class-loading and which will be loaded in common way (parent class-loader first) 
	 * Typically "java." "javax." "sun.misc." etc. */
	public static String[] excludedPackages = StringUtils.split(Defaults.PACKAGES_EXCLUDED_FROM_GREEDY_CLASS_LOADING);

	protected String[] clExcludedPackages = null;
	
	private Map<String, Package> packages = new HashMap<String, Package>();
	
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
	 * @param name name of loaded packaged, resource or class
	 * @return true if 'greedy' (parent last) approach should be used by this classloader
	 */
	private boolean isGreedy(String name) {
		if (!greedy) {
			//this classloader is not 'greedy' at all
			return false;
		} else {
			//this classloader is 'greedy' by default, check excluded packages
			if (clExcludedPackages != null) {
				for (String pack : clExcludedPackages) {
					if (!StringUtils.isEmpty(pack)) {
						if (pack.endsWith(".")) {
							pack = pack.substring(0, pack.length() - 1);
						}
						if (name.equals(pack) || name.startsWith(pack + ".")) {
							return false;
						}
					}
				}
			}
			return true;
		}
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
		if (isGreedy(name)) {
			return loadClassGreedy(name, resolve);
		} else {
	        if (log.isTraceEnabled()) {
	            log.trace(this + " P-F loading: " + name);
	        }
			Class<?> c = super.loadClass(name, resolve);
	        if (log.isTraceEnabled()) {
	            log.trace(this + " P-F loaded:  " + name + " by: " + getClassLoaderId(c.getClassLoader()));
	        }
			return c;
		}
	}

	protected synchronized Class<?> loadClassGreedy(String name, boolean resolve) throws ClassNotFoundException {
		// First, check if the class has already been loaded
		Class <?> c = findLoadedClass(name);
		if (c == null) {
		    try {
				// try to load the class by ourselves
		        if (log.isTraceEnabled()) {
			        log.trace(this + " S-F loading: " + name);
		        }
		        c = findClass(name);
		        if (log.isTraceEnabled()) {
			        log.trace(this + " S-F loaded:  " + name + " by: " + getClassLoaderId(c.getClassLoader()));
		        }
		    } catch (ClassNotFoundException e) {
		        if (log.isTraceEnabled()) {
			        log.trace(this + " S-F loading: " + name + " by: " + getClassLoaderId(getParent()));
		        }
		    	c = getParent().loadClass(name);
		        if (log.isTraceEnabled()) {
			      	   log.trace(this + " S-F loaded:  "+ name + " by: " + getClassLoaderId(c.getClassLoader()));
		        }
		    }
		}
		if (resolve) {
		    resolveClass(c);
		}
		return c;
	}
	
	/*
	 * This method overrides default behaviour for package retrieval - the purpose of greedy (inverted) loading is to
	 * load code from provided URLs instead from ancestor loaders. Such code can be contained in sealed packages -
	 * in order to prevent sealing violations it is needed to provide only packages that were defined by this class loader.
	 */
	@Override
	protected Package getPackage(String name) {
		if (isGreedy(name)) {
			return findPackage(name);
		} else {
			return super.getPackage(name);
		}
	}
	
	protected Package findPackage(String name) {
		synchronized (packages) {
			return packages.get(name);
		}
	}
	
	@Override
	protected Package definePackage(String name, String specTitle, String specVersion, String specVendor,
			String implTitle, String implVersion, String implVendor, URL sealBase) throws IllegalArgumentException {
		Package pkg = super.definePackage(name, specTitle, specVersion, specVendor, implTitle, implVersion, implVendor, sealBase);
		if (pkg != null) {
			synchronized (packages) {
				packages.put(pkg.getName(), pkg);
			}
		}
		return pkg;
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
			return cl.getClass().toString() + "#" + cl.hashCode();
		}
	}

	@Override
	public synchronized void addURL(URL urlToAdd) {
		super.addURL(urlToAdd);
	}

	@Override
	public URL getResource(String name) {
		if (isGreedy(name)) {
			if (log.isTraceEnabled()) {
				log.trace(this + " S-F trying to load resource: " + name);
			}
			URL url = findResource(name);
			if (url != null) {
				if (log.isTraceEnabled()) {
					log.trace(this + " S-F loaded resource: " + name);
				}
				return url;
			}
		}
		
		if (log.isTraceEnabled()) {
			log.trace(this + " P-F trying to load resource: " + name);
		}
		URL url = super.getResource(name);
		if (log.isTraceEnabled()) {
			log.trace(this + " resource loaded from url: " + url);
		}
		return url;
	}
	
	@Override
	public Enumeration<URL> getResources(String name) throws IOException {
		if (isGreedy(name)) {
			if (log.isTraceEnabled()) {
				log.trace(this + " S-F trying to load resources: " + name);
			}
			return new CompoundEnumeration<>(Arrays.asList(findResources(name), getParent().getResources(name)));
        } else {
        	if (log.isTraceEnabled()) {
				log.trace(this + " P-F trying to load resources: " + name);
        	}
        	return super.getResources(name);
        }
	}

	@Override
	public Class<?> defineClass(String name, byte[] classBytes) {
		Class<?> klass = findLoadedClass(name);
		if (klass == null) {
			klass = defineClass(name, classBytes, 0, classBytes.length);
		}
		return klass;
	}
}