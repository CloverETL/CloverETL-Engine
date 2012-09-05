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
package org.jetel.logger;

import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;

/** Layout for logging safe messages (no passwords should be logged). Text of the log message is obfuscated
 * 
 * @author Tomas Laurincik (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 20.2.2012
 */
public class SafeLogLayout extends PatternLayout {
	
	public SafeLogLayout() {
	}

	public SafeLogLayout(String pattern) {
		super(pattern);
	}

	@Override
	public boolean ignoresThrowable() {
		return false;
	}

	@Override
    public String format(LoggingEvent event) {

		// if we have a text log
		if (event.getMessage() instanceof String) {
			// obfuscate password in the message itself
            String message = event.getRenderedMessage();
    		String safeMessage = SafeLogUtils.obfuscatePassword(message);

    		// obfuscate the stack trace, if there is any
    		ThrowableInformation throwableInformation = null;
    		if (event.getThrowableInformation() != null) {
    			String[] stringRepresentation = event.getThrowableInformation().getThrowableStrRep();
    			
    			String[] safeStringRepresentation = null;
    			if (stringRepresentation != null) {
    				safeStringRepresentation = new String[stringRepresentation.length];
    				for (int i=0; i < stringRepresentation.length; i++) {
    					safeStringRepresentation[i] = SafeLogUtils.obfuscatePassword(stringRepresentation[i]);
    				}
    			}
    			
    			throwableInformation = new ThrowableInformation(safeStringRepresentation);
    		}
    		
    		// create new log event from obfuscated texts
            LoggingEvent safeEvent = new LoggingEvent(event.fqnOfCategoryClass,
            											Logger.getLogger(event.getLoggerName()), 
            											event.timeStamp, 
            											event.getLevel(), 
            											safeMessage, 
            											event.getThreadName(),
            											throwableInformation,
            											event.getNDC(),
            											null,
            											event.getProperties());

            // handle the message by parent layout. NOTE: throwable is not handled - we need to do it ourselves
            StringBuilder b = new StringBuilder(super.format(safeEvent));
            
            // go through the stack trace and add it line by line to the result string
            if (safeEvent.getThrowableInformation() != null) {
            	String[] safeStringRepresentation = safeEvent.getThrowableStrRep();
            	
				for (int i=0; i < safeEvent.getThrowableInformation().getThrowableStrRep().length; i++) {
	    			b.append(safeStringRepresentation[i]).append("\n");
				}
            }
            
            return b.toString(); 
            
        }
        return super.format(event);
    }
}
