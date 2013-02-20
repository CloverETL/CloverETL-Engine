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
package org.jetel.exception;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * An exception which can have multiple causes exceptions. Methods in ExceptionUtils should
 * support this multi-parent exception.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 6.2.2013
 */
public class CompoundException extends JetelRuntimeException {

	private static final long serialVersionUID = -8667916539978731979L;
	
	private List<Throwable> causes;
	
    public CompoundException() {
    	super();
    }

    public CompoundException(String message) {
    	this(message, (Throwable[]) null);
    }

    public CompoundException(Throwable... causes) {
        this(null, causes);
    }
	
    public CompoundException(String message, Throwable... causes) {
        super(message, causes != null && causes.length > 0 ? causes[0] : null);
        if (causes != null) {
        	this.causes = new ArrayList<Throwable>(Arrays.asList(causes));
        } else {
        	this.causes = new ArrayList<Throwable>();
        }
    }
    
    public List<Throwable> getCauses() {
    	return Collections.unmodifiableList(causes);
    }

}
