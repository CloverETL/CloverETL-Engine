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
/**
 * 
 */
package org.jetel.component;

import java.util.HashMap;
import java.util.Map;

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.util.joinKey.JoinKeyUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;

/**
 * @deprecated ErrorActions toolkit will be re-factored soon
 */
@Deprecated 
public enum ErrorAction {
	STOP,
	CONTINUE;
	
	public final static ErrorAction DEFAULT_ERROR_ACTION = STOP;
	
	public static Map<Integer, ErrorAction> createMap(String errorActionsString){
	    Map<Integer, ErrorAction> errorActions = new HashMap<Integer, ErrorAction>();
		if (errorActionsString != null){
	        	String[] actions = StringUtils.split(errorActionsString);
	        	if (actions.length == 1 && !actions[0].contains("=")){
	        		errorActions.put(Integer.MIN_VALUE, ErrorAction.valueOf(actions[0].trim().toUpperCase()));
	        	}else{
	        	String[] action;
		        	for (String string : actions) {
						action = JoinKeyUtils.getMappingItemsFromMappingString(string);
						try {
							errorActions.put(Integer.parseInt(action[0]), ErrorAction.valueOf(action[1].toUpperCase()));
						} catch (NumberFormatException e) {
							if (action[0].equals(ComponentXMLAttributes.STR_MIN_INT)) {
								errorActions.put(Integer.MIN_VALUE, ErrorAction.valueOf(action[1].toUpperCase()));
							}
						}
					}
	        	}
	        }else{
	        	errorActions.put(-1, ErrorAction.CONTINUE);
	        	errorActions.put(Integer.MIN_VALUE, DEFAULT_ERROR_ACTION);
	        }
		return errorActions;
	}
	
	public static boolean checkActions(String errorActionsString) throws ComponentNotReadyException{
    	String[] actions = StringUtils.split(errorActionsString);
    	if (actions.length == 1 && !actions[0].contains("=")){
			if (ErrorAction.valueOf(actions[0].trim().toUpperCase()) == null) {
				throw new ComponentNotReadyException("Unknown error action: " + StringUtils.quote(actions[0]));
			}
    	}else{
        	String[] action;
        	for (String string : actions) {
				action = JoinKeyUtils.getMappingItemsFromMappingString(string);
				try{
					Integer.parseInt(action[0]);
				}catch(NumberFormatException e) {
					if (!action[0].equals(ComponentXMLAttributes.STR_MIN_INT)) {
						throw new ComponentNotReadyException(e);
					}
				}
				if (ErrorAction.valueOf(action[1].toUpperCase()) == null) {
					throw new ComponentNotReadyException("Unknown error action: " + StringUtils.quote(actions[1]));
				}
 			}
    	}
    	return true;
	}
}