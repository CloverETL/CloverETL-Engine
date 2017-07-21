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
package org.jetel.hadoop.provider.filesystem;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/**
 * Registry for instances of Hadoop FileSystem class used to work-around FileSystem.CACHE issues (CLO-1160, CLO-730, ...).
 * 
 * Hadoop FileSystem instances retrieved by {@link FileSystem#get(URI, Configuration, String)} (or similar "get" methods)
 * are cached in internal FileSystem.CACHE so anyone asking for a FileSystem may receive an instance which they share with someone else.
 * Therefore, it is dangerous to close such a shared instance if someone else is still going to need it (this is definitively the case
 * with the DistributeFileSystem class instances -- CLO-1160).
 * 
 * So how this workaround works:
 * FileSystem owner (any entity that is going to use a FileSystem; may be e.g. a component, connection, whatever Object) 
 * indicates that it is going to use a FileSystem instance by registering the instance with this registry 
 * ({@link #getAndRegister(URI, Configuration, String, Object)}, or just {@link #registerFileSystem(FileSystem, Object)} methods).
 * Once the owner doesn't need the FileSystem anymore, it must release the FileSytem with the {@link #release(FileSystem, Object)}
 * method. This registry keeps track of all owners of all FileSystems and once there are
 * no more users of particular FileSytsem, it closes the FileSystem.
 */
public class FileSystemRegistry {

	/** Registry of FileSystems and their owners, i.e. components which access the file system */
	private static final Map<FileSystem, Set<Object>> registry = new HashMap<FileSystem, Set<Object>>();
	
	private FileSystemRegistry() {}
	
	/**
	 * Delegates to {@link FileSystem#get(URI, Configuration, String)} method and registers resulting
	 * FileSystem which later, when not needed anymore by the <code>owner</code>, must be released with 
	 * {@link #release(FileSystem, Object)} method.
	 * @param uri URI pointing to desired FS
	 * @param conf
	 * @param user username to be used when accessing the FileSystem
	 * @param owner component which will accesses the file system
	 * @return a FileSystem
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static synchronized FileSystem getAndRegister(final URI uri, final Configuration conf, final String user, Object owner) throws IOException, InterruptedException {
		FileSystem fs = FileSystem.get(uri, conf, user);
		registerFileSystem(fs, owner);
		return fs;
	}
	
	/**
	 * Registers specified FileSystem. Later, when not needed anymore by the <code>owner</code>, 
	 * the FileSystem must be released with the {@link #release(FileSystem, Object)} method.
	 * @param fs
	 * @param owner
	 */
	public static void registerFileSystem(FileSystem fs, Object owner) {
		Set<Object> fsOwners = registry.get(fs);
		if (fsOwners == null) {
			fsOwners = new HashSet<Object>();
			registry.put(fs, fsOwners);
		}
		fsOwners.add(owner);
	}
	
	/**
	 * Indicates that the <code>owner</code> will not use specified FileSystem anymore.
	 * If there are no more owners of the FileSystem <code>fs</code>, the FileSystem is closed.
	 * @param fs
	 * @param owner
	 * @throws IOException if closing of <code>fs</code> fails.
	 */
	public static synchronized void release(FileSystem fs, Object owner) throws IOException {
		Set<Object> fsOwners = registry.get(fs);
		// Note: It's not an error if the owner does not own the FS.
		// (This is the case in HadoopSequencParser/Formatter when it receives the FS from Hadoop connection.)
		if (fsOwners != null) {
			fsOwners.remove(owner);
			if (fsOwners.isEmpty()) {
				registry.remove(fs);
				fs.close();
			}
		}
	}

}
