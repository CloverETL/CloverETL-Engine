/*
 * Copyright (c) 2004-2005 OpenTech s.r.o. All rights reserved.
 * 
 * $Header$
 */
package cz.opentech.jdbc.xlsdriver.metadata;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;


/**
 * @author vitek
 */
public class ConnectionInfo {
    
    private final ArrayList schemas = new ArrayList();
    
    /**
     * 
     *
     */
    public ConnectionInfo() {
        // empty
    }
    
    /**
     * 
     * @param schemas
     */
    public ConnectionInfo(SchemaMetadata[] schemas) {
        this.schemas.addAll(Arrays.asList(schemas));
    }
    
    /**
     * @return the schemas.
     */
    public SchemaMetadata[] getSchemas() {
        return (SchemaMetadata[]) schemas.toArray(new SchemaMetadata[schemas.size()]);
    }
    
    /**
     * 
     * @return
     */
    public int getSchemasCount() {
        return schemas.size();
    }
    
    /**
     * 
     * @param idx
     * @return
     */
    public SchemaMetadata getSchema(int idx) {
        return (SchemaMetadata) schemas.get(idx);
    }
    
    /**
     * 
     * @param schema
     */
    public SchemaMetadata addSchema() {
        SchemaMetadata ret = new SchemaMetadata(this);
        schemas.add(ret);
        return ret;
    }
    
    /**
     * 
     * @param idx
     */
    public void removeSchema(int idx) {
        schemas.remove(idx);
    }
    
    /**
     * 
     * @param schema
     */
    public void removeSchema(SchemaMetadata schema) {
        schemas.remove(schema);
    }

    /**
     *
     */
    public void removeAllSchemas() {
        schemas.clear();
    }
    
    public static ConnectionInfo parseInfo(String str) throws SQLException {
        return parseInfo(str, new ConnectionInfo());
    }
    
    /**
     * Parses connection info to the info instance.
     * 
     * @param str string.
     * @param info the connection info to be set up from the string.
     * @return info
     */
    public static ConnectionInfo parseInfo(String str, ConnectionInfo info) throws SQLException {
        str = str.trim();
        if (str.startsWith("{")) {
            while (str.length() > 0) {
	            SchemaMetadata schema = info.addSchema();
	            str = str.substring(1);
	            String s = extractGroup(str);
	            
	            SchemaMetadata.parseSchema(s, schema);

	            str = str.substring(s.length() + 1).trim();
	            if (str.startsWith(";")) {
	                str = str.substring(1).trim();
	            }
            }
        } else {
            SchemaMetadata schema = info.addSchema();
            SchemaMetadata.parseSchema(str, schema);
        }
        
        return info;
    }
    
    static String extractGroup(String s) {
        int sLen = s.length();
        int idx = 0;
        int depth = 0;
        boolean escape = false;
        char c;
        while (idx < sLen && ((c = s.charAt(idx)) != '}' || depth > 0)) {
            switch (c) {
            case '{': if (!escape) depth++; break;
            case '}': if (!escape) depth--; break;
            case '\'': {
                int i = idx + 1;
                if (i < sLen && s.charAt(i) == '\'') {
                    idx += 1;
                } else {
                    escape = !escape;
                }
            }
            }
            idx += 1;
        }
        return s.substring(0, idx);
    }
    static String extractProperty(String s) {
        int sLen = s.length();
        int idx = 0;
        boolean escape = false;
        char c;
        while (idx < sLen && (c = s.charAt(idx)) != ';') {
            switch (c) {
            case '\'': {
                int i = idx + 1;
                if (i < sLen && s.charAt(i) == '\'') {
                    idx += 1;
                } else {
                    escape = !escape;
                }
            }
            }
            idx += 1;
        }
        return s.substring(0, idx);
    }
    static String[] parseProperty(String value, String defName) {
        value = value.trim();
        if(!value.startsWith("'")) {
            int idx = value.indexOf('=');
            if (idx != -1) {
                defName = value.substring(0, idx).trim();
                value = value.substring(idx + 1).trim();
            }
        }
        if (value.startsWith("'")) {
            value = stripPropertyValue(value);
        }
        return new String[] {defName, value};
    }
    static String stripPropertyValue(String str) {
        int sLen = str.length();
        StringBuffer sb = new StringBuffer(str.length());
        for (int idx = 0; idx < sLen; idx++) {
            char c = str.charAt(idx);
            switch (c) {
            case '\'':
                int i = idx + 1;
                if (i < sLen && str.charAt(i) == '\'') {
                    idx += 1;
                    sb.append('\'');
                }
                break;
            default:
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
