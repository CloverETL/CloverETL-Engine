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
package org.jetel.interpreter.extensions;

import org.jetel.interpreter.data.TLValue;

/**
 * Helper class for caching parameters of CTL functions
 * 
 * @author dpavlis
 *
 */
public class TLParameterCache {
	
	protected TLValue[] cachedValues;
	protected TLValue cached1;
	protected TLValue cached2;
	protected TLValue cached3;
	
	public TLParameterCache(TLValue[] params){
		cachedValues=new TLValue[params.length];
		for (int i=0;i<cachedValues.length;i++){
			cachedValues[i]=params[i].duplicate();
		}
	}
	
	public TLParameterCache(){
	}

	/**
	 * Determines whether this cache object contains already some cached values
	 * 
	 * @return true if some values are already cached
	 */
	public boolean isInitialized(){
		if ( cached1!=null || cachedValues!=null) 
			return true;
		else
			return false;
	}
	
	
	public void free(){
		cachedValues=null;
	}
	
	/**
	 * Cache values passed as parameters.
	 * 
	 * @param params values to be cached
	 * @param start index of first value to be cached (in params array)
	 * @param length how many values cache from the params array
	 */
	public void cache(TLValue[] params,int start,int length){
		if (cachedValues==null || cachedValues.length!=length)
				cachedValues=new TLValue[length];
		
		for (int i=0;i<length;i++){
			cachedValues[i]=params[i+start].duplicate();
		}
	}
	
	/**
	 * Cache values passed as parameter except the very first value which
	 * is usually a variable of the CTL function and therefore changes with
	 * every call.
	 * 
	 * @param params
	 */
	public void cacheButFirst(TLValue[] params){
		final int length=params.length-1;
		if (cachedValues==null || cachedValues.length!=length)
				cachedValues=new TLValue[length];
		
		for (int i=0;i<length;i++){
			cachedValues[i]=params[i+1].duplicate();
		}
	}
	
	public void cache(TLValue[] params){
		cache(params,0,params.length);
	}
	
	public void cache(TLValue par1){
		cached1=par1.duplicate();
	}
	
    public void cache(TLValue par1, TLValue par2){
    	cached1=par1.duplicate();
    	cached2=par2.duplicate();
	}
	
    public void cache(TLValue par1, TLValue par2, TLValue par3){
    	cached1=par1.duplicate();
    	cached2=par2.duplicate();
    	cached3=par3.duplicate();
	}
    
	
	public TLValue[] getCached(){
		if (cachedValues!=null){
			return cachedValues;
		}else{
			return new TLValue[] {cached1, cached2, cached3};
		}
			
	}
	
	
	public boolean hasChanged(TLValue[] params){
		return hasChanged(params,0,params.length);
	}

	/**
	 * Determines whether internally cached values are the
	 * same as passed in parameters.
	 * 
	 * @param params
	 * @param start
	 * @param length
	 * @return true if internally cached values differ from the passed-in
	 */
	public boolean hasChanged(TLValue[] params, int start, int length){
		if (length!=cachedValues.length) return true;
		for (int i=0;i<length;i++){
			if (!cachedValues[i].equals(params[start+i])) return true;
		}
		return false;
	}
	
	/**
	 * Determines whether internally cached values are the
	 * same as passed in parameters. Does not consider the very first
	 * passed-in parameter.
	 * 
	 * @param params
	 * @return
	 */
	public boolean hasChangedButFirst(TLValue[] params){
		final int length=params.length-1;
		if (length!=cachedValues.length) return true;
		for (int i=0;i<length;i++){
			if (!cachedValues[i].equals(params[i+i])) return true;
		}
		return false;
	}
	
	public boolean hasChanged(TLValue[] params, int startParams, int startCached, int length){
		for (int i=0;i<length;i++){
			if (!cachedValues[startCached+i].equals(params[startParams+i])) return true;
		}
		return false;
	}
	
	public boolean hasChanged (TLValue par1){
		return !cached1.equals(par1);
	}
	
	public boolean hasChanged (TLValue par1, TLValue par2){
		return ! (cached1.equals(par1) && cached2.equals(par2));
	}
	
	public boolean hasChanged (TLValue par1, TLValue par2, TLValue par3){
		return ! (cached1.equals(par1) && cached2.equals(par2) && cached3.equals(par3));
	}
}
