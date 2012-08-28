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

import java.io.File;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.List;

import org.jetel.component.fileoperation.SimpleParameters.CopyParameters;
import org.jetel.component.fileoperation.SimpleParameters.CreateParameters;
import org.jetel.component.fileoperation.SimpleParameters.DeleteParameters;
import org.jetel.component.fileoperation.SimpleParameters.FileParameters;
import org.jetel.component.fileoperation.SimpleParameters.InfoParameters;
import org.jetel.component.fileoperation.SimpleParameters.ListParameters;
import org.jetel.component.fileoperation.SimpleParameters.MoveParameters;
import org.jetel.component.fileoperation.SimpleParameters.ReadParameters;
import org.jetel.component.fileoperation.SimpleParameters.ResolveParameters;
import org.jetel.component.fileoperation.SimpleParameters.WriteParameters;
import org.jetel.util.protocols.webdav.WebdavOutputStream;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Jun 18, 2012
 */
public class WebdavOperationHandler implements IOperationHandler {

	static final String HTTP_SCHEME = "http"; //$NON-NLS-1$
	static final String HTTPS_SCHEME = "https"; //$NON-NLS-1$
	
	private class WebdavContent implements WritableContent {
		
		private final String url;

		public WebdavContent(String url) {
			this.url = url;
		}

		@Override
		public WritableByteChannel write() throws IOException {
			return Channels.newChannel(new WebdavOutputStream(url));
		}

		@Override
		public WritableByteChannel append() throws IOException {
			throw new UnsupportedOperationException(FileOperationMessages.getString("IOperationHandler.append_not_supported")); //$NON-NLS-1$
		}
		
	}
	
	@Override
	public SingleCloverURI copy(SingleCloverURI source, SingleCloverURI target, CopyParameters params)
			throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public SingleCloverURI move(SingleCloverURI source, SingleCloverURI target, MoveParameters params)
			throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public ReadableContent getInput(SingleCloverURI source, ReadParameters params) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public WritableContent getOutput(SingleCloverURI target, WriteParameters params) throws IOException {
		return new WebdavContent(target.getPath());
	}

	@Override
	public SingleCloverURI delete(SingleCloverURI target, DeleteParameters params) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<SingleCloverURI> resolve(SingleCloverURI uri, ResolveParameters params) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<Info> list(SingleCloverURI parent, ListParameters params) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Info info(SingleCloverURI target, InfoParameters params) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public SingleCloverURI create(SingleCloverURI target, CreateParameters params) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public File getFile(SingleCloverURI uri, FileParameters params) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getPriority(Operation operation) {
		return 0;
	}

	@Override
	public boolean canPerform(Operation operation) {
		switch (operation.kind) {
		case WRITE:
			return operation.scheme().equalsIgnoreCase(HTTP_SCHEME) || operation.scheme().equalsIgnoreCase(HTTPS_SCHEME);
		default: 
			return false;
		}
	}

	@Override
	public String toString() {
		return "WebdavOperationHandler"; //$NON-NLS-1$
	}

}
