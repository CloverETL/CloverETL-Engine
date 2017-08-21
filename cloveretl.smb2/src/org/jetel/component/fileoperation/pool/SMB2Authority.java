package org.jetel.component.fileoperation.pool;

import java.net.URI;
import java.net.URL;

import org.jetel.component.fileoperation.SMB2Path;

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
