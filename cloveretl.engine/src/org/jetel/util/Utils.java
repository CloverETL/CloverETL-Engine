/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2005-06  Javlin Consulting <info@javlinconsulting.cz>
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
package org.jetel.util;

import java.util.Locale;

import org.jetel.data.Defaults;

public final class Utils {

    private Utils() { }

    /**
     * Creates locale from clover internal format - <language_identifier>[.<country_identifier>]
     * Examples:
     *  en
     *  en.GB
     *  fr
     *  fr.FR
     *  cs
     *  cs.CZ
     * @param localeStr
     * @return
     */
    public static Locale createLocale(String localeStr) {
        Locale locale = null;
        
        if(localeStr == null){
            locale = Locale.getDefault();
        } else {
            String[] localeLC = localeStr.split(Defaults.DEFAULT_LOCALE_STR_DELIMITER_REGEX);
            if (localeLC.length > 1) {
                locale = new Locale(localeLC[0], localeLC[1]);
            } else {
                locale = new Locale(localeLC[0]);
            }
        }
        
        return locale;
    }

}
