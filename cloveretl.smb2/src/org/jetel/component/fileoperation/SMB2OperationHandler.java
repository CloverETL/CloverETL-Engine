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

import static java.text.MessageFormat.format;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.jetel.component.fileoperation.SimpleParameters.ResolveParameters;

public class SMB2OperationHandler extends AbstractOperationHandler {
	
	private final PrimitiveSMB2OperationHandler smbHandler;

	/**
	 * @param simpleHandler
	 */
	public SMB2OperationHandler() {
		super(new PrimitiveSMB2OperationHandler());
		this.smbHandler = (PrimitiveSMB2OperationHandler) simpleHandler;
	}

	public static final String SMB_SCHEME = "smb2"; //$NON-NLS-1$
	
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
	
	// TODO refactor this code and move it to AbstractOperationHandler
	@Override
	public List<SingleCloverURI> resolve(SingleCloverURI wildcards, ResolveParameters params) throws IOException {
		String uriString = wildcards.toURI().toString();
		if (wildcards.isRelative() || !FileManager.uriHasWildcards(uriString)) {
			return Arrays.asList(wildcards);
		}
		
		List<String> parts = FileManager.getUriParts(uriString);
		URI baseUri = URI.create(parts.get(0));
		Info baseInfo = simpleHandler.info(baseUri);
		if (baseInfo == null) {
			return new ArrayList<SingleCloverURI>(0);
		}
		List<URI> bases = Arrays.asList(baseUri);
		for (Iterator<String> it = parts.listIterator(1); it.hasNext(); ) {
			String part = it.next();
			List<URI> nextBases = new ArrayList<>(bases.size());
			boolean hasPathSeparator = part.endsWith(URIUtils.PATH_SEPARATOR);
			if (hasPathSeparator) {
				part = part.substring(0, part.length()-1);
			}
			for (URI i: bases) {
				nextBases.addAll(expand(i, part, it.hasNext() || hasPathSeparator));
			}
			bases = nextBases;
		}
		
		List<SingleCloverURI> result = new ArrayList<SingleCloverURI>(bases.size());
		for (URI u: bases) {
			result.add(CloverURI.createSingleURI(u));
		}
		return result;
	}

	@Override
	public String toString() {
		return "SMB2OperationHandler"; //$NON-NLS-1$
	}

	private List<URI> expand(URI base, String part, boolean directory) throws IOException {
		if (base == null) {
			throw new NullPointerException("base"); //$NON-NLS-1$
		}
		Info baseInfo = simpleHandler.info(base);
		if ((baseInfo == null) || !baseInfo.isDirectory()) {
			throw new IllegalArgumentException(format(FileOperationMessages.getString("FileManager.not_a_directory"), base)); //$NON-NLS-1$
		}
		if (FileManager.hasWildcards(part)) {
			part = URIUtils.urlDecode(part);
			return smbHandler.list(base, part, directory);
		} else {
			URI child = URIUtils.getChildURI(base, URI.create(part));
			Info childInfo = simpleHandler.info(child);
			if (childInfo != null) {
				return Arrays.asList(child);
			} else {
				return Collections.emptyList();
			}
		}
	}

}
