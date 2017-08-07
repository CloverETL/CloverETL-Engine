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
package org.jetel.component.fileoperation;

import java.io.IOException;
import java.util.List;

import org.jetel.component.fileoperation.SimpleParameters.ResolveParameters;

public class SMB2OperationHandler extends AbstractOperationHandler {
	
	private FileManager manager = FileManager.getInstance();

	/**
	 * @param simpleHandler
	 */
	public SMB2OperationHandler() {
		super(new PrimitiveSMB2OperationHandler());
	}

	static final String SMB_SCHEME = "smb2"; //$NON-NLS-1$
	private static final String SMB_ROOT_URL = SMB_SCHEME + "://"; //$NON-NLS-1$
	
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
			case FILE:
				return operation.scheme().equalsIgnoreCase(SMB_SCHEME);
			case COPY:
			case MOVE:
				return operation.scheme(0).equalsIgnoreCase(SMB_SCHEME)
						&& operation.scheme(1).equalsIgnoreCase(SMB_SCHEME);
			default: 
				return false;
		}
	}
	
	@Override
	public List<SingleCloverURI> resolve(SingleCloverURI uri, ResolveParameters params) throws IOException {
		return manager.defaultResolve(uri);
	}

	@Override
	public String toString() {
		return "SMB2perationHandler"; //$NON-NLS-1$
	}

}
