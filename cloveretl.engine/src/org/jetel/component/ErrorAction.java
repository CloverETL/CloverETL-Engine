/**
 * 
 */
package org.jetel.component;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.util.joinKey.JoinKeyUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;

public enum ErrorAction {
	STOP,
	CONTINUE;
	
	final static ErrorAction DEFAULT_ERROR_ACTION = STOP;
	
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