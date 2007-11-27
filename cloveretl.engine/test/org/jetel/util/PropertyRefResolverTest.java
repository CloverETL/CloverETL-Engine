/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2002-05  David Pavlis <david_pavlis@hotmail.com> and others.
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
 * Created on 15.5.2006
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package org.jetel.util;

import java.util.Properties;

import junit.framework.TestCase;

import org.jetel.data.Defaults;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.util.property.PropertyRefResolver;

public class PropertyRefResolverTest extends TestCase {

    protected void setUp() throws Exception {
        super.setUp();
        Defaults.init();
        
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void test() throws AttributeNotFoundException {
        //Test/Debug code
       Properties prop=new Properties();
       prop.put("dbDriver","org.mysql.test");
       prop.put("user","myself");
       prop.put("password","xxxyyyzzz");
       prop.put("pwd","${password}");
       
       PropertyRefResolver attr=new PropertyRefResolver(prop);
       System.out.println("DB driver is: '{${dbDriver}}' ...");
       System.out.println(attr.resolveRef("DB driver is: '{${dbDriver}}' ..."));
       System.out.println("${user} is user");
       System.out.println(attr.resolveRef("${user} is user"));
       System.out.println("${user}/${password}/${pwd} is user/password");
       System.out.println(attr.resolveRef("${user}/${password}/${pwd} is user/password"));
       System.out.println("\\${user}/\\${password} is user/password");
       System.out.println(attr.resolveRef("${user1}/${password1}/${pwd} is user/password",false));
    
    }

}
