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
package org.jetel.util;

import net.jpountz.lz4.LZ4Factory;

import org.jetel.util.exec.PlatformUtils;

/**
 * Workaround for CLO-3962.
 * 64bit IBM Java crashes from version 7.0.7.0 on.
 * To be tested with IBM Java 8.
 * 
 * In this environment, enforce the usage of {@link LZ4Factory#unsafeInstance()}.
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Jun 17, 2014
 * 
 * @see <a href="https://bug.javlin.eu/browse/CLO-3962">CLO-3962</a>
 */
public class LZ4Provider {

	private static boolean IBM_FIX = PlatformUtils.getJavaVendor().contains("IBM") && PlatformUtils.getOSArch().contains("amd64") && PlatformUtils.getJavaMajorVersion() >= 7;
	
	public static LZ4Factory fastestInstance() {
		return IBM_FIX ? LZ4Factory.unsafeInstance() : LZ4Factory.fastestInstance();
	}

}
