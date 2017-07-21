/*
 * Copyright (c) 2004-2005 OpenTech s.r.o. All rights reserved.
 * 
 * $Header$
 */
package cz.opentech.jdbc.xlsdriver.db.util;

import java.util.StringTokenizer;

/**
 * @author vitek
 */
public class EscapedStringTokenizer extends StringTokenizer {

    private final char escapeChar;
    private final String escapeStr;
    private boolean escaped;
    
    public EscapedStringTokenizer(String text, String delimiters, char escapeChar) {
        super(text, delimiters);
        this.escapeChar = escapeChar;
        this.escapeStr = String.valueOf(escapeChar);
    }

    /**
     * @see java.util.StringTokenizer#countTokens()
     */
    public int countTokens() {
        throw new UnsupportedOperationException();        
    }
    /**
     * @see java.util.Enumeration#hasMoreElements()
     */
    public boolean hasMoreElements() {
        return super.hasMoreElements();
    }
    /**
     * @see java.util.StringTokenizer#hasMoreTokens()
     */
    public boolean hasMoreTokens() {
        return super.hasMoreTokens();
    }
    /**
     * @see java.util.Enumeration#nextElement()
     */
    public Object nextElement() {
        return nextToken();
    }
    /**
     * @see java.util.StringTokenizer#nextToken()
     */
    public String nextToken() {
        String ret = "";
        while (hasMoreTokens()) {
            String s = super.nextToken();
            analyzeEscaped(s);
            ret += s;
            if (!escaped) break;
        }
        return ret;
    }
    /**
     * @see java.util.StringTokenizer#nextToken(java.lang.String)
     */
    public String nextToken(String delim) {
        String ret = "";
        while (hasMoreTokens()) {
            String s = super.nextToken(delim);
            analyzeEscaped(s);
            ret += s;
            if (!escaped) break;
        }
        return ret;
    }
    
    private void analyzeEscaped(String s) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
            case '\'':
                i+= 1;
                if (i < s.length() && s.charAt(i) == '\'') {
                    // donothing
                } else {
                    escaped = !escaped;
                }
                break;
            }
        }
    }
}
