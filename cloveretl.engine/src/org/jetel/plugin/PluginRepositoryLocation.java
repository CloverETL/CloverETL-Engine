/*
 * Copyright 2006-2009 Opensys TM by Javlin, a.s. All rights reserved.
 * Opensys TM by Javlin PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * Opensys TM by Javlin a.s.; Kremencova 18; Prague; Czech Republic
 * www.cloveretl.com; info@cloveretl.com
 *
 */

package org.jetel.plugin;

import java.io.File;

/**
 * This class represent a location of a clover engine plugin repository, for instance
 * <code>./plugins</code> in default installation. Internally is implemented by
 * a {@link File} of plugin repository and optional class loader which should be used
 * as default class loader of all plugins placed in the repository.
 *   
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 14.1.2011
 */
public class PluginRepositoryLocation {

	private File location;
	
	private ClassLoader classloader;
	
	public PluginRepositoryLocation(File location) {
		this(location, null);
	}

	public PluginRepositoryLocation(File location, ClassLoader classloader) {
		this.location = location;
		this.classloader = classloader;
	}

	public File getLocation() {
		return location;
	}

	public ClassLoader getClassloader() {
		return classloader;
	}

}
