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

/**
 * Class which carries information about current CloverETL
 * library version (both major & minor).<br>
 * Can be used to check for compatibility.<br>
 * <i>Values are automatically updated by ANT build file when
 * release is built. </i>
 * 
 * @author david pavlis
 * @since  25.10.2005
 *
 */
public final class JetelVersion {

	/**
	 * ANT does NOT change this value.
	 */
    public static final String DEVEL_VERSION = "0.0.0.devel";
    
    /**
     * <code>MAJOR_VERSION</code> - determines Major version of current library
     */
    public static final String MAJOR_VERSION = "0";
    /**
     * <code>MINOR_VERSION</code> - determines Minor version of current library
     */
    public static final String MINOR_VERSION = "0";  
    /**
     * <code>REVISION_VERSION</code> - determines Revision version of current library
     */
    public static final String REVISION_VERSION = "0";  
    /**
     * <code>VERSION_SUFFIX</code> - determines detail information about version (build source) of current library
     */
    public static final String VERSION_SUFFIX = "devel";  
    /**
     * <code>BUILD_NUMBER</code> - sequential number identifying current library version
     */
    public static final String BUILD_NUMBER = "0"; 
    /**
     * <code>JAVA_REQUIRED_VERSION</code> - minimum Java version required by the library
     */
    public static final String JAVA_REQUIRED_VERSION = "1.5";
    
    public static final String LIBRARY_BUILD_DATETIME = "";
    
    public static final String LIBRARY_BUILD_YEAR = "";
    
    /**
     * @return Major version of current library - e.g. if
     * the library version is "1.6" then 1 is returned
     */
    public static final int getMajorVersion(){
        return Integer.parseInt(MAJOR_VERSION);
    }
    
    /**
     * @return Minor version of current library - e.g. if
     * the library version is "1.6" then 6 is returned
     */
    public static final int getMinorVersion(){
        return Integer.parseInt(MINOR_VERSION);
    }

    /**
     * @return Revision version of current library - e.g. if
     * the library version is "1.6.3" then 3 is returned
     */
    public static final int getRevisionVersion(){
        return Integer.parseInt(REVISION_VERSION);
    }

    /**
     * @return Build number of current library - it is a sequence number
     */
    public static final int getBuildNumber(){
        return Integer.parseInt(BUILD_NUMBER);
    }
    
    /**
     * @return Timestamp of library assembly (when it was compiled)
     */
    public static final String getBuildDatetime(){
        return LIBRARY_BUILD_DATETIME;
    }
    
    /**
     * @return Minimum version of Java which is required
     */
    public static final String getJavaRequiredVersion(){
        return JAVA_REQUIRED_VERSION;
    }

    public static String getVersion() {
    	return MAJOR_VERSION+"."+MINOR_VERSION+"."+REVISION_VERSION+"."+VERSION_SUFFIX;
    }
}
