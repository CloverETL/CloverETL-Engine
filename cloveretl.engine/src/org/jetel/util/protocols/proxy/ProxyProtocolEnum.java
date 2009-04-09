package org.jetel.util.protocols.proxy;

import java.net.Proxy;

/**
 * Proxy protocol enumerator.
 * 
 * @author Jan Ausperger (jan.ausperger@javlin.eu)
 *         (c) OpenSys (www.opensys.eu)
 */
public enum ProxyProtocolEnum {
    NO_PROXY(Proxy.NO_PROXY.type().name()),
    PROXY_HTTP(Proxy.Type.HTTP.toString()),
    PROXY_SOCKS(Proxy.Type.SOCKS.toString());
    
    //alternatives
    private static final String NO_PROXY_PROT = "noproxy";
    private static final String PROXY_PROT = "proxy";
    private static final String PROXY_HTTP_PROT = "proxyhttp";
    private static final String PROXY_SOCKS_PROT = "proxysocks";
    
    private String id;
    
    private ProxyProtocolEnum(String id) {
        this.id = id;
    }
    
    public static ProxyProtocolEnum fromString(String id) {
        return fromString(id, null);
    }
    
    public static ProxyProtocolEnum fromString(String id, ProxyProtocolEnum defaultValue) {
        if(id == null) return defaultValue;
        
        if (id.equals(PROXY_PROT) || id.equals(PROXY_HTTP_PROT)) return PROXY_HTTP; // default proxy
        else if (id.equals(PROXY_SOCKS_PROT)) return PROXY_SOCKS;
        else if (id.equals(NO_PROXY_PROT)) return NO_PROXY;
        
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
} 

