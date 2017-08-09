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
		this(url.getPath());
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
