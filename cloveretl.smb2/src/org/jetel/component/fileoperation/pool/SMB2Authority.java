package org.jetel.component.fileoperation.pool;

import java.net.URI;
import java.net.URL;

import org.jetel.component.fileoperation.SMB2Path;

public class SMB2Authority extends DefaultAuthority {
	
	private final String share;

	public SMB2Authority(URI uri) {
		super(uri);
		this.share = new SMB2Path(uri).getShare();
	}

	public SMB2Authority(URL url) {
		super(url);
		this.share = new SMB2Path(url).getShare();
	}

	public String getShare() {
		return share;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((share == null) ? 0 : share.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		SMB2Authority other = (SMB2Authority) obj;
		if (share == null) {
			if (other.share != null)
				return false;
		} else if (!share.equals(other.share))
			return false;
		return true;
	}

}
