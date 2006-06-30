/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/
package org.jetel.component;

import org.jetel.plugin.Extension;

/**
 * @author Martin Zatopek
 *
 */
public class ComponentDescription {

    public final static String EXTENSION_POINT_ID = "component";
    private final static String TYPE = "type";
    private final static String CLASS = "className";

    
	private String type;
	
	private String className;
	
    private ClassLoader classLoader;
    
	public ComponentDescription(String componentType, String className, ClassLoader classLoader) {
		this.type = componentType;
		this.className = className;
        this.classLoader = classLoader;
	}

    public ComponentDescription(Extension componentExtension) {
        if(!componentExtension.getPointId().equals(EXTENSION_POINT_ID)) {
            throw new IllegalArgumentException();
        }
        this.type = componentExtension.getParameter(TYPE);
        this.className = componentExtension.getParameter(CLASS);
        this.classLoader = componentExtension.getPlugin().getClassLoader();
        if(type == null || className == null) {
            throw new IllegalArgumentException();
        }
    }

	public String getClassName() {
		return className;
	}

	public String getType() {
		return type;
	}
	
	public void setClassName(String className) {
		this.className = className;
	}
	
	public void setType(String componentType) {
		this.type = componentType;
	}

    public ClassLoader getClassLoader() {
        return classLoader;
    }

    public void setClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }
}
