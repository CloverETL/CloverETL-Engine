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
package org.jetel.util.protocols.sftp;

import com.jcraft.jsch.UIKeyboardInteractive;

/**
 * Class for password supporting.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz) (c) Javlin
 *         Consulting (www.javlinconsulting.cz)
 */
public class URLUserInfoIteractive extends AUserInfo implements
		UIKeyboardInteractive {

	public URLUserInfoIteractive(String password) {
		super(password);
	}

	@Override
	public String getPassword() {
		return null;
	}

	@Override
	public boolean promptPassword(String message) {
		return true;
	}

	@Override
	public String[] promptKeyboardInteractive(String destination,
			String name, String instruction, String[] prompt, boolean[] echo) {
		return new String[] { password };
	}
}