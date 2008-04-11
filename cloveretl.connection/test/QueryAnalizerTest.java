import java.sql.SQLException;
import java.util.HashMap;

import junit.framework.TestCase;

import org.jetel.connection.jdbc.QueryAnalyzer;
import org.jetel.util.primitive.DuplicateKeyMap;

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

/**
 * @author avackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Nov 02, 2007
 */
public class QueryAnalizerTest extends TestCase {
	
	DuplicateKeyMap dbCloverMap;
	HashMap<String, String> cloverDbMap;

	/**
	 * Test method for {@link org.jetel.connection.QueryAnalyzer#QueryAnalizer(java.lang.String)}.
	 */
	public void testQueryAnalizer() throws SQLException{
		String query = "select $field1:=f1,$field2:= f2,  $field3:=f3 \n from mytable where f1=$fname and f2 = $lname";
		QueryAnalyzer analizer = new QueryAnalyzer(query);
		System.out.println(query);
		System.out.println(analizer.getNotInsertQuery());
		dbCloverMap = analizer.getDbCloverFieldMap();
		cloverDbMap = analizer.getCloverDbFieldMap();
		assertEquals(cloverDbMap.get("field1"), "f1");
		assertEquals(dbCloverMap.get("f2"), "lname");
		System.out.println();

		query = "select $field1:=tab1.f1,$field2:= tab1.f2,  $field3:=tab.2f3 \n from mytable where f1=$fname and f2 = $lname";
		analizer.setQuery(query);
		System.out.println(query);
		System.out.println(analizer.getNotInsertQuery());
		dbCloverMap = analizer.getDbCloverFieldMap();
		cloverDbMap = analizer.getCloverDbFieldMap();
		assertEquals(cloverDbMap.get("field1"), "tab1.f1");
		assertEquals(dbCloverMap.get("f2"), "lname");
		System.out.println();
		
		query = "select f1,f2,  f3 \n from mytable where f1=? and f2 = ?";
		analizer.setQuery(query);
		System.out.println(query);
		System.out.println(analizer.getNotInsertQuery());
		dbCloverMap = analizer.getDbCloverFieldMap();
		cloverDbMap = analizer.getCloverDbFieldMap();
		assertEquals(cloverDbMap.size(), 0);
		assertNull(dbCloverMap.get("f1"));
		assertNull(dbCloverMap.get("f2"));
		System.out.println();
		
		query = "select * \n from mytable";
		analizer.setQuery(query);
		System.out.println(query);
		System.out.println(analizer.getNotInsertQuery());
		dbCloverMap = analizer.getDbCloverFieldMap();
		cloverDbMap = analizer.getCloverDbFieldMap();
		assertEquals(0, cloverDbMap.size());
		assertEquals(0, dbCloverMap.size());
		System.out.println();

		query = "update abc set f1=$f1, f2=$f2 where xyz= select a from 123 where field=$xyz";
		analizer.setQuery(query);
		System.out.println(query);
		System.out.println(analizer.getNotInsertQuery());
		dbCloverMap = analizer.getDbCloverFieldMap();
		assertEquals(dbCloverMap.get("f1"), "f1");
		assertEquals(dbCloverMap.get("field"), "xyz");
		System.out.println();
		
		query = "Insert into abc (f1,f2,f3,f4) values(1,$f1 , $f2, $f3) ";
		analizer.setQuery(query);
		System.out.println(query);
		System.out.println(analizer.getInsertQuery());
		dbCloverMap = analizer.getDbCloverFieldMap();
		assertEquals(dbCloverMap.get("f3"), "f2");
		System.out.println();
		
		query = "Insert into abc (f1,f2,f3,f4) values(1,$f1 , $f2, $f3) returning $field1:=f1,$field2:=f5";
		analizer.setQuery(query);
		System.out.println(query);
		System.out.println(analizer.getInsertQuery());
		dbCloverMap = analizer.getDbCloverFieldMap();
		assertEquals(dbCloverMap.get("f3"), "f2");
		cloverDbMap = analizer.getCloverDbFieldMap();
		assertEquals(cloverDbMap.get("field1"), "f1");
		assertEquals(cloverDbMap.get("field2"), "f5");
		System.out.println();
		
		query = "Insert into abc (f1,f2,f3,f4) values(1,?, ?, \t ?) ";
		analizer.setQuery(query);
		System.out.println(query);
		System.out.println(analizer.getInsertQuery());
		dbCloverMap = analizer.getDbCloverFieldMap();
		cloverDbMap = analizer.getCloverDbFieldMap();
		assertEquals(0, cloverDbMap.size());
		assertNull(dbCloverMap.get("f2"));
		System.out.println();

		query = "Insert into abc values($f1, sysdate , $f2, $f3) ";
		analizer.setQuery(query);
		System.out.println(query);
		System.out.println(analizer.getInsertQuery());
		dbCloverMap = analizer.getDbCloverFieldMap();
		assertEquals(dbCloverMap.get(null), "f1");
		assertEquals(dbCloverMap.getNext(), "f2");
		assertEquals(dbCloverMap.getNext(), "f3");
		assertNull(dbCloverMap.getNext());
		System.out.println();
		
		query = "delete from mytable ";
		analizer.setQuery(query);
		System.out.println(query);
		System.out.println(analizer.getNotInsertQuery());
		dbCloverMap = analizer.getDbCloverFieldMap();
		cloverDbMap = analizer.getCloverDbFieldMap();
		assertEquals(0, cloverDbMap.size());
		assertEquals(0, dbCloverMap.size());

	}


}
