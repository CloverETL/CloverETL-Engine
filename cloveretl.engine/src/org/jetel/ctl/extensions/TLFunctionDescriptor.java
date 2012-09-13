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
package org.jetel.ctl.extensions;

/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2002-07  David Pavlis <david.pavlis@centrum.cz> and others.
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
 * Created on 20.3.2007
 *
 */

import java.util.Arrays;

import org.jetel.ctl.data.TLType;

public class TLFunctionDescriptor {

    protected final String name;
    protected final TLFunctionLibrary library;
    protected final String description;
    protected final TLType[] formalParameters;
    protected final TLType returnType;
	private boolean isGeneric;
	private boolean isVarArg;
	private boolean hasInit; 
	private boolean deprecated;
    
    protected TLFunctionDescriptor(TLFunctionLibrary library,String name,String description,TLType[] formalParameters,
            TLType returnType, boolean isGeneric, boolean isVarArg, boolean hasInit) {
        this.name=name;
        this.library=library;
        this.description=description;
        this.formalParameters=formalParameters;
        this.returnType=returnType;
        this.isGeneric = isGeneric;
        this.isVarArg = isVarArg;
        this.setHasInit(hasInit);
    }
    
    public String getName() {
    	return name;
        
    }
    
    public boolean isGeneric() {
		return isGeneric;
	}
    
    public boolean isVarArg() {
		return isVarArg;
	}
    
	public boolean isDeprecated() {
		return deprecated;
	}

	public void setDeprecated(boolean deprecated) {
		this.deprecated = deprecated;
	}

	public TLType[] getFormalParameters() {
        return formalParameters;
    }
    
    public TLFunctionPrototype getExecutable() {
    	return library.getExecutable(name);
    }

    
    
    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(formalParameters);
		result = prime * result + ((library == null) ? 0 : library.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((returnType == null) ? 0 : returnType.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof TLFunctionDescriptor))
			return false;
		final TLFunctionDescriptor other = (TLFunctionDescriptor) obj;
		if (!Arrays.equals(formalParameters, other.formalParameters))
			return false;
		if (library == null) {
			if (other.library != null)
				return false;
		} else if (!library.equals(other.library))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (returnType == null) {
			if (other.returnType != null)
				return false;
		} else if (!returnType.equals(other.returnType))
			return false;
		return true;
	}

	/**
     * @return the library
     * @since 2.4.2007
     */
    public TLFunctionLibrary getLibrary() {
        return library;
    }


    /**
     * @return the returnType
     * @since 2.4.2007
     */
    public TLType getReturnType() {
        return returnType;
    }
    
	public String getDescription() {
		return description;
	}

	/**
	 * @param hasInit the hasInit to set
	 */
	public void setHasInit(boolean hasInit) {
		this.hasInit = hasInit;
	}

	/**
	 * @return the hasInit
	 */
	public boolean hasInit() {
		return hasInit;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(name).append('(');
		if (formalParameters != null) {
			for (int i = 0; i < formalParameters.length; i++) {
				sb.append(formalParameters[i].name());
				if (i < formalParameters.length-1) {
					sb.append(", ");
				}
			}
		}
		sb.append(')');
		return sb.toString();
	}

}
