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

import org.apache.log4j.Logger;
import org.jetel.util.MiscUtils;
import org.jetel.util.string.UnicodeBlanks;


/**
 * Provides type of current platform - WINDOWS, LINUX, MAC.
 * 
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 13.6.2011
 */
public class PlatformUtils {
	
	private static final Logger log = Logger.getLogger(PlatformUtils.class);

	public static enum PlatformType {
		LINUX, MAC, WINDOWS
	}
	
	/* Determines the system OS and sets os accordingly */
	private static PlatformType platformType;
	
	private static final String javaVendor = System.getProperty("java.vendor");
	
	private static final String javaVersion = System.getProperty("java.version");
	
	private static int javaMajorVersion = -1;
	
	private static final String osArch = System.getProperty("os.arch");
	
	private static final Pattern JAVA_VERSION_PATTERN = Pattern.compile("^(\\d+)(\\.(\\d+).*)?$");
	
	static {
		String platformIdentifier = String.valueOf(MiscUtils.getSystemPropertySafe("os.name")).toLowerCase();
		
		if (platformIdentifier.contains("mac")) {
			platformType = PlatformType.MAC;
		} else if (platformIdentifier.contains("windows")) {
			platformType = PlatformType.WINDOWS;
		} else {
			platformType = PlatformType.LINUX;
		}

		try {
			javaMajorVersion = getJavaMajorVersion(javaVersion);
		} catch (IllegalArgumentException e) {
			log.error(e);
			javaMajorVersion = -1;
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
     * @return          true if the platform is Linux else false
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
	
	/**
	 * Returns java major version from the given javaVersion string.
	 * 
	 * Returns -1 if the string is blank.
	 * @param javaVersion
	 * @return
	 * @throws IllegalArgumentException if javaVersion is invalid.
	 */
	public static int getJavaMajorVersion(String javaVersion) throws IllegalArgumentException {
		int majorVersion = -1;
		
		if (!UnicodeBlanks.isBlank(javaVersion)) {
			Matcher matcher = JAVA_VERSION_PATTERN.matcher(javaVersion);
			if (matcher.matches()) {
				try {
					int firstNumber = Integer.parseInt(matcher.group(1));
					String secondNumber = matcher.group(3);
					
					if (!UnicodeBlanks.isBlank(secondNumber)) {
						
						if (firstNumber > 1) {
							// Java version 9 or newer, e.g. 9.0
							majorVersion = firstNumber;
						} else {
							// Java versions 1.X, valid up to Java 1.8
							majorVersion = Integer.parseInt(secondNumber);
						}
					} else {
						//since Java 11 there is only one number
						majorVersion = firstNumber;
					}
				} catch (NumberFormatException e) {
					throw new IllegalArgumentException("Unsupported Java version number format: " + javaVersion);
				}
			} else {
				throw new IllegalArgumentException("Unsupported Java version number format: " + javaVersion);
			}
		}
		
		return majorVersion;
	}
	
}
