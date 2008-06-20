package org.jetel.connection;

import java.sql.SQLException;
import java.util.List;

import org.jetel.connection.jdbc.QueryAnalyzer;
import org.jetel.test.CloverTestCase;

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
 * @author avackova (agata.vackova@javlinconsulting.cz) ; (c) JavlinConsulting s.r.o. www.javlinconsulting.cz
 * 
 * @since Nov 02, 2007
 */
public class QueryAnalizerTest extends CloverTestCase {

	List<String[]> dbCloverMap;
	List<String[]> cloverDbMap;

	// DuplicateKeyMap dbCloverMap;
	// HashMap<String, String> cloverDbMap;

	/**
	 * Test method for {@link org.jetel.connection.QueryAnalyzer#QueryAnalizer(java.lang.String)}.
	 */
	public void testQueryAnalizer() throws SQLException {
		String[] mapping;

		String query = "select $field1:=f1,$field2:= f2,  $field3:=f3 \n from mytable where f1=$fname and f2 = $lname";
		QueryAnalyzer analizer = new QueryAnalyzer(query);
		System.out.println(query);
		System.out.println(analizer.getSelectQuery());
		dbCloverMap = analizer.getDbCloverFieldMap();
		cloverDbMap = analizer.getCloverDbFieldMap();
		mapping = dbCloverMap.get(1);
		assertEquals("f2", mapping[0]);
		assertEquals("lname", mapping[1]);
		mapping = cloverDbMap.get(0);
		assertEquals("field1", mapping[0]);
		assertEquals("f1", mapping[1]);
		System.out.println();

		query = "select $field1:=tab1.f1,$field2:= tab1.f2,  $field3:=tab.2f3 \n from mytable where f1=$fname and f2 = $lname";
		analizer.setQuery(query);
		System.out.println(query);
		System.out.println(analizer.getSelectQuery());
		dbCloverMap = analizer.getDbCloverFieldMap();
		cloverDbMap = analizer.getCloverDbFieldMap();
		mapping = dbCloverMap.get(1);
		assertEquals("f2", mapping[0]);
		assertEquals("lname", mapping[1]);
		mapping = cloverDbMap.get(0);
		assertEquals("field1", mapping[0]);
		assertEquals("tab1.f1", mapping[1]);
		System.out.println();

		query = "select f1,f2,  f3 \n from mytable where f1=? and f2 = ?";
		analizer.setQuery(query);
		System.out.println(query);
		System.out.println(analizer.getSelectQuery());
		dbCloverMap = analizer.getDbCloverFieldMap();
		cloverDbMap = analizer.getCloverDbFieldMap();
		mapping = dbCloverMap.get(1);
		assertEquals("f2", mapping[0]);
		assertEquals(null, mapping[1]);
		mapping = cloverDbMap.get(0);
		assertEquals(null, mapping[0]);
		assertEquals("f1", mapping[1]);
		System.out.println();

		query = "select * \n from mytable";
		analizer.setQuery(query);
		System.out.println(query);
		System.out.println(analizer.getSelectQuery());
		dbCloverMap = analizer.getDbCloverFieldMap();
		cloverDbMap = analizer.getCloverDbFieldMap();
		assertEquals(0, cloverDbMap.size());
		assertEquals(0, dbCloverMap.size());
		System.out.println();

		query = "update abc set f1=$f1, f2=$f2 where xyz= select a from 123 where field=$xyz";
		analizer.setQuery(query);
		System.out.println(query);
		System.out.println(analizer.getUpdateDeleteQuery());
		dbCloverMap = analizer.getDbCloverFieldMap();
		cloverDbMap = analizer.getCloverDbFieldMap();
		mapping = dbCloverMap.get(0);
		assertEquals("f1", mapping[0]);
		assertEquals("f1", mapping[1]);
		mapping = dbCloverMap.get(2);
		assertEquals("field", mapping[0]);
		assertEquals("xyz", mapping[1]);
		assertEquals(0, cloverDbMap.size());
		System.out.println();

		query = "update abc set f1=?, f2=? where xyz= ?";
		analizer.setQuery(query);
		System.out.println(query);
		System.out.println(analizer.getUpdateDeleteQuery());
		dbCloverMap = analizer.getDbCloverFieldMap();
		mapping = dbCloverMap.get(0);
		assertEquals("f1", mapping[0]);
		assertEquals(null, mapping[1]);
		mapping = dbCloverMap.get(1);
		assertEquals("f2", mapping[0]);
		assertEquals(null, mapping[1]);
		System.out.println();

		query = "Insert into abc (f1,f2,f3,f4) values(1,$f1 , $f2, $f3) ";
		analizer.setQuery(query);
		System.out.println(query);
		System.out.println(analizer.getInsertQuery());
		dbCloverMap = analizer.getDbCloverFieldMap();
		mapping = dbCloverMap.get(0);
		assertEquals("f1", mapping[0]);
		assertEquals(null, mapping[1]);
		mapping = dbCloverMap.get(2);
		assertEquals("f3", mapping[0]);
		assertEquals("f2", mapping[1]);
		System.out.println();

		query = "Insert into abc (f1,f2,f3,f4) values(1,$f1 , $f2, $f3) returning $field1:=f1,$field2:=f5";
		analizer.setQuery(query);
		System.out.println(query);
		System.out.println(analizer.getInsertQuery());
		dbCloverMap = analizer.getDbCloverFieldMap();
		cloverDbMap = analizer.getCloverDbFieldMap();
		mapping = dbCloverMap.get(2);
		assertEquals("f3", mapping[0]);
		assertEquals("f2", mapping[1]);
		mapping = cloverDbMap.get(0);
		assertEquals("field1", mapping[0]);
		assertEquals("f1", mapping[1]);
		mapping = cloverDbMap.get(1);
		assertEquals("field2", mapping[0]);
		assertEquals("f5", mapping[1]);
		System.out.println();

		query = "Insert into abc (f1,f2,f3,f4) values(1,?, ?, \t ?) ";
		analizer.setQuery(query);
		System.out.println(query);
		System.out.println(analizer.getInsertQuery());
		dbCloverMap = analizer.getDbCloverFieldMap();
		cloverDbMap = analizer.getCloverDbFieldMap();
		assertEquals(0, cloverDbMap.size());
		mapping = dbCloverMap.get(1);
		assertEquals("f2", mapping[0]);
		assertEquals(null, mapping[1]);
		System.out.println();

		query = "Insert into abc values(1,?, ?, \t ?) ";
		analizer.setQuery(query);
		System.out.println(query);
		System.out.println(analizer.getInsertQuery());
		dbCloverMap = analizer.getDbCloverFieldMap();
		cloverDbMap = analizer.getCloverDbFieldMap();
		assertEquals(0, cloverDbMap.size());
		System.out.println();

		query = "Insert into abc values($f1, sysdate , $f2, $f3) ";
		analizer.setQuery(query);
		System.out.println(query);
		System.out.println(analizer.getInsertQuery());
		dbCloverMap = analizer.getDbCloverFieldMap();
		mapping = dbCloverMap.get(0);
		assertEquals(null, mapping[0]);
		assertEquals("f1", mapping[1]);
		mapping = dbCloverMap.get(1);
		assertEquals(null, mapping[0]);
		assertEquals("f2", mapping[1]);
		mapping = dbCloverMap.get(2);
		assertEquals(null, mapping[0]);
		assertEquals("f3", mapping[1]);
		System.out.println();

		query = "delete from mytable ";
		analizer.setQuery(query);
		System.out.println(query);
		System.out.println(analizer.getUpdateDeleteQuery());
		dbCloverMap = analizer.getDbCloverFieldMap();
		cloverDbMap = analizer.getCloverDbFieldMap();
		assertEquals(0, cloverDbMap.size());
		assertEquals(0, dbCloverMap.size());

		query = "delete from mytable where id = ?";
		analizer.setQuery(query);
		System.out.println(query);
		System.out.println(analizer.getUpdateDeleteQuery());
		dbCloverMap = analizer.getDbCloverFieldMap();
		cloverDbMap = analizer.getCloverDbFieldMap();
		assertEquals(0, cloverDbMap.size());
		mapping = dbCloverMap.get(0);
		assertEquals("id", mapping[0]);
		assertEquals(null, mapping[1]);

		query = "delete from mytable where id = $field1 and lname=$last_name";
		analizer.setQuery(query);
		System.out.println(query);
		System.out.println(analizer.getUpdateDeleteQuery());
		dbCloverMap = analizer.getDbCloverFieldMap();
		cloverDbMap = analizer.getCloverDbFieldMap();
		assertEquals(0, cloverDbMap.size());
		mapping = dbCloverMap.get(1);
		assertEquals("lname", mapping[0]);
		assertEquals("last_name", mapping[1]);

		query = "update k3 set c1 = $id, c2=$end_date where c1 = $id and c2 = $end_date";
		analizer.setQuery(query);
		System.out.println(query);
		System.out.println(analizer.getUpdateDeleteQuery());
		dbCloverMap = analizer.getDbCloverFieldMap();
		cloverDbMap = analizer.getCloverDbFieldMap();
		assertEquals(0, cloverDbMap.size());
		mapping = dbCloverMap.get(0);
		assertEquals("c1", mapping[0]);
		assertEquals("id", mapping[1]);
		mapping = dbCloverMap.get(1);
		assertEquals("c2", mapping[0]);
		assertEquals("end_date", mapping[1]);
		mapping = dbCloverMap.get(2);
		assertEquals("c1", mapping[0]);
		assertEquals("id", mapping[1]);
		mapping = dbCloverMap.get(3);
		assertEquals("c2", mapping[0]);
		assertEquals("end_date", mapping[1]);

	}

}
