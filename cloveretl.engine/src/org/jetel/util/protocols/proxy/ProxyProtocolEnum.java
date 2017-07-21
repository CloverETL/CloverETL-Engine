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
package org.jetel.util.protocols.proxy;

import java.net.Proxy;


/**
 * Proxy protocol enumerator.
 * 
 * @author Jan Ausperger (jan.ausperger@javlin.eu)
 *         (c) Javlin, a.s. (www.javlin.eu)
 */
public enum ProxyProtocolEnum {
    NO_PROXY("direct"),
    PROXY_HTTP("proxy"),
    PROXY_SOCKS("proxysocks");
    
    private String id;
    
    private ProxyProtocolEnum(String id) {
        this.id = id;
    }
    
    public static ProxyProtocolEnum fromString(String id) {
        return fromString(id, null);
    }
    
    public static ProxyProtocolEnum fromString(String id, ProxyProtocolEnum defaultValue) {
        if(id == null) return defaultValue;
        
        for(ProxyProtocolEnum item : values()) {
            if(id.equalsIgnoreCase(item.id)) {
                return item;
            }
        }
        return defaultValue;
    }
    
    @Override
	public String toString() {
        return id;
    }
    
    public String getProxyString() {
        if (this == PROXY_HTTP) {
        	return Proxy.Type.HTTP.name();
        } else if (this == NO_PROXY) {
    		return Proxy.Type.DIRECT.name();
    	} else {
    		return Proxy.Type.SOCKS.name();
    	}
    }
} 

