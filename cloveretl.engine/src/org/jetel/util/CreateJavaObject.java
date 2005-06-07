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
package org.jetel.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.database.SQLUtil;

/**
 * @author Wes Maciorowski
 *
 * To change the template for this generated type comment go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
public class CreateJavaObject{
	
	static Log logger = LogFactory.getLog(CreateJavaObject.class);
	
   public static Object createObject(String className) {
	   Object object = null;
	   try {
		   Class classDefinition = Class.forName(className);
		   object = classDefinition.newInstance();
	   } catch (InstantiationException e) {
		   logger.error(e);
	   } catch (IllegalAccessException e) {
		   logger.error(e);
	   } catch (ClassNotFoundException e) {
		   logger.error(e);
	   }
	   return object;
	}

	public static Object createObject(String className,
									 Object[] arguments) {

	  Object object = null;
	  Constructor constructor;
	Class[] parameterTypes = new Class[arguments.length];
	for( int i = 0 ; i < arguments.length ; i++ ) {
		parameterTypes[i] = (arguments[i]).getClass();
	}
	  try {
		Class aClass = Class.forName(className);
		constructor =  aClass.getConstructor(parameterTypes);
		object = constructor.newInstance(arguments);
		logger.info("Object: " + object.toString());
		return object;
	  } catch (InstantiationException e) {
		  logger.error(e);
	  } catch (IllegalAccessException e) {
		  logger.error(e);
	  } catch (IllegalArgumentException e) {
		  logger.error(e);
	  } catch (InvocationTargetException e) {
		  logger.error(e);
	  } catch (ClassNotFoundException e) {
		  logger.error(e);
	  } catch (NoSuchMethodException e) {
		  logger.error(e);
	  }
	  return object;
   }

	public static Object invokeMethod(String className,
									  String aMethodName,
									  Object[] argumentsConstructor,
									  Object[] argumentsMethod) {
	   Object result = null;
	   Object actOnObject = null;
	   Class classDefinition = null;

		try {
			classDefinition = Class.forName(className);
//			  Method[] someMethods = classDefinition.getMethods();
//			  for(int i = 0; i<someMethods.length ; i++) {
//				  System.out.println(someMethods[i].toString());
//			  }
		} catch (ClassNotFoundException e) {
			e.printStackTrace();  //To change body of catch statement use Options | File Templates.
		}
		if(argumentsConstructor == null) {
			actOnObject = createObject( className );
		} else {
			actOnObject = createObject( className, argumentsConstructor );
		}
		Method someMethod = null;
		Class[] parameterTypes = null;
		try {
			if(argumentsMethod != null) {

				parameterTypes = new Class[argumentsMethod.length];
				for( int i = 0 ; i < argumentsMethod.length ; i++ ) {
					parameterTypes[i] = (argumentsMethod[i]).getClass();
				}
				someMethod = classDefinition.getMethod(aMethodName, parameterTypes);
				result = someMethod.invoke(actOnObject, argumentsMethod);
			} else {
				someMethod = classDefinition.getMethod(aMethodName, null);
				result = someMethod.invoke(actOnObject, null);
			}
	   } catch (NoSuchMethodException e) {
		   logger.error(e);
	   } catch (IllegalAccessException e) {
		   logger.error(e);
	   } catch (InvocationTargetException e) {
		   logger.error(e);
	   }
	   return result;
	}
}
