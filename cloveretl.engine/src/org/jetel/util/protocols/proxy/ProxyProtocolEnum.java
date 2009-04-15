package org.jetel.util.protocols.proxy;


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
} 

