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

import java.net.URI;
import java.net.URL;

public class SMB2Path {

	private String share = "";
	private String path = "";
	private String originalPath = "";

	public SMB2Path(URI uri) {
		this(uri.getPath());
	}
	
	public SMB2Path(URL url) {
		this(URIUtils.urlDecode(url.getPath()));
	}
	
	private SMB2Path(String uriPath) {
		if (uriPath != null) {
			if (uriPath.startsWith("/")) {
				uriPath = uriPath.substring(1);
			}

			String[] parts = uriPath.split("/", 2);
			if (parts.length > 0) {
				this.share = parts[0];
			}
			if (parts.length > 1) {
				this.originalPath = parts[1];
			}

			this.path = originalPath;
			if (this.path.endsWith(URIUtils.PATH_SEPARATOR)) {
				this.path = this.path.substring(0, this.path.length() - 1);
			}
			this.path = this.path.replace('/', '\\');
		}
	}

	public String getPath() {
		return this.path;
	}
	
	public String getShare() {
		return this.share;
	}

	@Override
	public String toString() {
		return share + "/" + path;
	}

}
