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
package org.jetel.component.fileoperation.hadoop;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.jetel.component.fileoperation.AbstractOperationHandler;
import org.jetel.component.fileoperation.FileManager;
import org.jetel.component.fileoperation.Operation;
import org.jetel.component.fileoperation.SimpleParameters.CreateParameters;
import org.jetel.component.fileoperation.SimpleParameters.ResolveParameters;
import org.jetel.component.fileoperation.SingleCloverURI;
import org.jetel.hadoop.connection.HadoopURLUtils;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Nov 26, 2012
 */
public class HadoopOperationHandler extends AbstractOperationHandler {

	public static final String HADOOP_SCHEME = HadoopURLUtils.HDFS_PROTOCOL;

	private FileManager manager = FileManager.getInstance();
	
	public HadoopOperationHandler() {
		super(new PrimitiveHadoopOperationHandler());
	}

	@Override
	public List<SingleCloverURI> resolve(SingleCloverURI wildcards, ResolveParameters params) throws IOException {
		return manager.defaultResolve(wildcards);
	}

	@Override
	public int getPriority(Operation operation) {
		return TOP_PRIORITY;
	}

	@Override
	public boolean canPerform(Operation operation) {
		switch (operation.kind) {
			case READ:
			case WRITE:
			case LIST:
			case INFO:
			case RESOLVE:
			case DELETE:
			case CREATE:
//			case FILE:
				return operation.scheme().equalsIgnoreCase(HADOOP_SCHEME);
//			case COPY:
//			case MOVE:
//				return operation.scheme(0).equalsIgnoreCase(HADOOP_SCHEME)
//						&& operation.scheme(1).equalsIgnoreCase(HADOOP_SCHEME);
			default: 
				return false;
		}
	}

	@Override
	public String toString() {
		return "HadoopOperationHandler";
	}

	@Override
	protected boolean create(URI uri, CreateParameters params) throws IOException {
		if (params.getLastModified() != null) {
			throw new IOException("Setting last modification date is not support on Hadoop");
		}
		return super.create(uri, params);
	}

}
