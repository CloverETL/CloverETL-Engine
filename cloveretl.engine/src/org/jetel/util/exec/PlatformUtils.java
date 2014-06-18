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
package org.jetel.util.exec;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Provides type of current platform - WINDOWS, LINUX, MAC.
 * 
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 13.6.2011
 */
public class PlatformUtils {

	public static enum PlatformType {
		LINUX, MAC, WINDOWS
	}
	
	/* Determines the system OS and sets os accordingly */
	private static PlatformType platformType;
	
	private static final String javaVendor = System.getProperty("java.vendor");
	
	private static final String javaVersion = System.getProperty("java.version");
	
	private static int javaMajorVersion = -1;
	
	private static final String osArch = System.getProperty("os.arch");
	
	static {
		String platformIdentifier = System.getProperty("os.name").toLowerCase();
		
		if (platformIdentifier.contains("mac")) {
			platformType = PlatformType.MAC;
		} else if (platformIdentifier.contains("windows")) {
			platformType = PlatformType.WINDOWS;
		} else {
			platformType = PlatformType.LINUX;
		}
		
		Matcher m = Pattern.compile("1\\.(\\d)\\.(\\d)").matcher(javaVersion);
		if (m.matches()) {
			javaMajorVersion = Integer.parseInt(m.group(1));
		}
	}
	
	/**
     * This method determine platform type.
     * 
     * @return          true if the platform is Windows else false
     * @since 23.8.2007
     */
	public static boolean isWindowsPlatform() {
		return platformType == PlatformType.WINDOWS;
	} 
	
	/**
     * This method determine platform type.
     * 
     * @return          true if the platform is MAC OS else false
     * @since 23.8.2007
     */
	public static boolean isMACPlatform() {
	    return platformType == PlatformType.MAC; 
	}
	
	/**
     * This method determine platform type.
     * 
     * @return          true if the platform is MAC OS else false
     * @since 23.8.2007
     */
	public static boolean isLinuxPlatform() {
	    return platformType == PlatformType.LINUX; 
	}

	/**
	 * @return type of current platform
	 */
	public static PlatformType getPlatformType() {
		return platformType;
	}
	
	public static String getJavaVendor() {
		return javaVendor;
	}
	
	public static String getOSArch() {
		return osArch;
	}
	
	public static String getJavaVersion() {
		return javaVersion;
	}
	
	public static int getJavaMajorVersion() {
		return javaMajorVersion;
	}
	
}
