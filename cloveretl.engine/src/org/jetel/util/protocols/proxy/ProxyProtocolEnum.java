package org.jetel.util.protocols.proxy;

import java.net.Proxy;


/**
 * Proxy protocol enumerator.
 * 
 * @author Jan Ausperger (jan.ausperger@javlin.eu)
 *         (c) OpenSys (www.opensys.eu)
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

