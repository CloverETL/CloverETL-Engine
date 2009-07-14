/*
 * Created on Jul 4, 2005
 * GreedyURLClassLoader
 */
package org.jetel.util.classloader;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * Classloader extended from URL classloader which loads classes (greedily) from specified
 * URLs at first. If unsuccesfull, it tries to load class using parent classloader.
 * @author misho
 *
 */
public class GreedyURLClassLoader extends URLClassLoader {

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
		    try {
				// try to load the class by ourselves
		        c = findClass(name);
		    } catch (ClassNotFoundException e) {
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