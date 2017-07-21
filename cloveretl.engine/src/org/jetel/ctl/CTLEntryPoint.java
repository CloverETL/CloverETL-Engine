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
package org.jetel.ctl;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Use this annotation for methods that should be recognized as CTL entry points
 * by the CTL-to-Java compiler.
 * 
 * @author mtomcanyi
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface CTLEntryPoint {
	
	/**
	 * @return True when the method must be implemented in the CTL code (required by given transformation inerface)
	 */
	boolean required();
	
	/**
	 * @return Name of the corresponding CTL function
	 */
	String name();
	/**
	 * @return Parameter names - empty by default
	 */
	String[] parameterNames() default {};

	/**
	 * @return deprecation message to be logged as a warning if an optional deprecated method is implemented
	 */
	String deprecated() default "";

}
