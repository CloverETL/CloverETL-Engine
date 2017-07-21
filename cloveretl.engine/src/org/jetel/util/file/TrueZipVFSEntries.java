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
package org.jetel.util.file;

import java.util.Collection;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.schlichtherle.truezip.file.TFile;
import de.schlichtherle.truezip.fs.FsSyncException;

/**
 * This class registers all archives managed by TrueZip library for a single graph instance.
 * 
 * It is used to explicitly flush all the archives to disk after graph execution is done,
 * so that the archives can be accessed in the same JVM by other code than TrueZip.
 * 
 * @author Raszyk (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 22.6.2011
 */
public class TrueZipVFSEntries {
	private final Collection<TFile> rootArchives = new HashSet<TFile>();
	
	static Log logger = LogFactory.getLog(TrueZipVFSEntries.class);
	
	public void addVFSEntry(TFile entry) {
		TFile rootArchive = null;
		
		for (TFile enclosingArchive = entry.getEnclArchive(); enclosingArchive != null; enclosingArchive = enclosingArchive.getEnclArchive()) {
			rootArchive = enclosingArchive;
		}

		if (rootArchive != null) {
			rootArchives.add(rootArchive);
		}
		else {
			throw new IllegalArgumentException(entry + " is not in an archive");
		}
	}
	
	public void freeAll() {
		for (TFile entry : rootArchives) {
			try {
				TFile.umount(entry, false, false, false, false);
			} catch (FsSyncException e) {
				logger.warn("Cannot unmount zip archive " + entry.getAbsolutePath());
			}
		}
	}
}
