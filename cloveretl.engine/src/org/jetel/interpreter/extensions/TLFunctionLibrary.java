/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2005-07  Javlin Consulting <info@javlinconsulting.cz>
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
package org.jetel.interpreter.extensions;

import java.util.HashMap;
import java.util.Map;

/**
 * Recommended ascendant of all TL function libraries. This class is intended
 * to be subclassed by function libraries defined in external engine plugins.
 *  
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 30.5.2007
 */
public abstract class TLFunctionLibrary implements ITLFunctionLibrary {

    protected Map<String, TLFunctionPrototype> library;

    public TLFunctionLibrary() {
        library = new HashMap<String, TLFunctionPrototype>();
    }

    public TLFunctionPrototype getFunction(String functionName) {
        return library.get(functionName);
    }

}
