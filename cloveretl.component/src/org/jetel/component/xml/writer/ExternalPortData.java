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
package org.jetel.component.xml.writer;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.InputPort;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

/**
 * Abstarct class for implementetions od data provider which needs to cache records.
 * 
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 17 May 2011
 */
public abstract class ExternalPortData extends PortData {
	
	private File tempFile;
	private long cacheSize;
	
	/**
	 * @param inPort
	 * @param keys
	 * @param tempDirectory
	 */
	ExternalPortData(InputPort inPort, Set<List<String>> keys, String tempDirectory, long cacheSize) {
		super(inPort, keys, tempDirectory);
		this.cacheSize = cacheSize;
	}
	
	@Override
	public void init() throws ComponentNotReadyException {
		super.init();
		try {
			tempFile = File.createTempFile("berkdb", "", tempDirectory != null ? new File(tempDirectory) : null);
			tempFile.delete();
			tempFile.mkdir();
			tempFile.deleteOnExit();
		} catch (IOException e) {
			throw new ComponentNotReadyException(e);
		}
	}
	
	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
		if (tempFile.exists()) {
			File[] files = tempFile.listFiles();
			for (int i = 0; i < files.length; i++) {
				files[i].delete();
			}
		}
	}

	@Override
	public void free() {
		super.free();
		tempFile.delete();
	}

	protected Environment getEnvironment() throws ComponentNotReadyException {
		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setCacheSize(cacheSize);
		envConfig.setAllowCreate(true);
		envConfig.setLocking(false);
		envConfig.setSharedCache(true);
		envConfig.setCacheMode(CacheMode.MAKE_COLD);
		try {
			return new Environment(tempFile, envConfig);
		} catch (Exception e) {
			throw new ComponentNotReadyException(e);
		}
	}

	@Override
	public boolean readInputPort() {
		return true;
	}
	
	protected DatabaseConfig getDbConfig() {
		DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setAllowCreate(true);
		dbConfig.setTemporary(true);
		dbConfig.setSortedDuplicates(true);
		dbConfig.setExclusiveCreate(true);
		return dbConfig;
	}

}
