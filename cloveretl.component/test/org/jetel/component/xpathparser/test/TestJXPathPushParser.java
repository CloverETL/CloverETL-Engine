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
package org.jetel.component.xpathparser.test;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilderFactory;

import org.jetel.component.xpathparser.AbortParsingException;
import org.jetel.component.xpathparser.DataRecordProvider;
import org.jetel.component.xpathparser.DataRecordReceiver;
import org.jetel.component.xpathparser.XPathEvaluator;
import org.jetel.component.xpathparser.XPathPushParser;
import org.jetel.component.xpathparser.bean.JXPathEvaluator;
import org.jetel.component.xpathparser.mappping.MappingContext;
import org.jetel.component.xpathparser.mappping.MappingElementFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.tree.bean.parser.BeanValueHandler;
import org.jetel.data.tree.parser.ValueHandler;
import org.jetel.exception.BadDataFormatException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Document;

/**
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 6.12.2011
 */
public class TestJXPathPushParser {

	@BeforeClass
	public static void beforeAll() {
		Defaults.init();
	}
	
	@Test
	public void testParser() throws Exception {
		
		List<Customer> customers = getTestData();
		
		RecordProvider provider = new RecordProvider();
		RecordReceiver receiver = new RecordReceiver();
		XPathEvaluator evaluator = new JXPathEvaluator();
		ValueHandler valueHandler = new BeanValueHandler();
		
		Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(new File("test-data/xpath/mapping-test-bean.xml"));
		MappingElementFactory mappingFactory = new MappingElementFactory();
		MappingContext mapping = mappingFactory.readMapping(doc);
		
		XPathPushParser parser = new XPathPushParser(provider, receiver, evaluator, valueHandler, null);
		
		parser.parse(mapping, customers);
		
		Assert.assertEquals(3, provider.records.size());
		Assert.assertEquals(3, receiver.records.size());
		
		List<DataRecord> customerRecords = receiver.records.get(Integer.valueOf(0));
		List<DataRecord> orderRecords = receiver.records.get(Integer.valueOf(1));
		List<DataRecord> itemRecords = receiver.records.get(Integer.valueOf(2));
		
		Assert.assertEquals(2, customers.size());
		
		Assert.assertEquals("Johnny Cash", customerRecords.get(0).getField("name").getValue().toString());
		Assert.assertEquals("1", customerRecords.get(0).getField("id").getValue().toString());
		Assert.assertEquals("New York", customerRecords.get(0).getField("city").getValue().toString());
		
		Assert.assertEquals("Grande Ope", customerRecords.get(1).getField("name").getValue().toString());
		Assert.assertEquals("2", customerRecords.get(1).getField("id").getValue().toString());
		Assert.assertEquals(null, customerRecords.get(1).getField("city").getValue());
		
		Assert.assertEquals(3, orderRecords.size());
		
		Assert.assertEquals(new Date(0), orderRecords.get(0).getField("deliveryDate").getValue());
		Assert.assertEquals("1", orderRecords.get(0).getField("id").getValue().toString());
		Assert.assertEquals("1", orderRecords.get(0).getField("customer_id").getValue().toString());
		
		Assert.assertEquals(3, itemRecords.size());
		
		Assert.assertEquals("in hard, leather cover", itemRecords.get(1).getField("detail").getValue().toString());
		Assert.assertEquals("1", itemRecords.get(0).getField("order_id").getValue().toString());
		Assert.assertEquals("1", itemRecords.get(0).getField("customer_id").getValue().toString());
	}
	
	private static class RecordProvider implements DataRecordProvider {
		
		private Map<Integer, DataRecord> records = new HashMap<Integer, DataRecord>();
		
		@Override
		public DataRecord getDataRecord(int port)
			throws AbortParsingException {
			
			switch (port) {
			case 0: {
				DataRecordMetadata metadata = new DataRecordMetadata("customer");
				DataFieldMetadata fieldMetadata = new DataFieldMetadata("name", DataFieldMetadata.STRING_FIELD, "|");
				metadata.addField(fieldMetadata);
				fieldMetadata = new DataFieldMetadata("id", DataFieldMetadata.STRING_FIELD, "|");
				metadata.addField(fieldMetadata);
				fieldMetadata = new DataFieldMetadata("city", DataFieldMetadata.STRING_FIELD, "|");
				metadata.addField(fieldMetadata);
				DataRecord record = new DataRecord(metadata);
				record.init();
				records.put(Integer.valueOf(port), record);
				break;
			}
			case 1: {
				DataRecordMetadata metadata = new DataRecordMetadata("order");
				DataFieldMetadata fieldMetadata = new DataFieldMetadata("deliveryDate", DataFieldMetadata.DATE_FIELD, "|");
				metadata.addField(fieldMetadata);
				fieldMetadata = new DataFieldMetadata("id", DataFieldMetadata.STRING_FIELD, "|");
				metadata.addField(fieldMetadata);
				fieldMetadata = new DataFieldMetadata("customer_id", DataFieldMetadata.STRING_FIELD, "|");
				metadata.addField(fieldMetadata);
				DataRecord record = new DataRecord(metadata);
				record.init();
				records.put(Integer.valueOf(port), record);
				break;
			}
			case 2: {
				DataRecordMetadata metadata = new DataRecordMetadata("item");
				DataFieldMetadata fieldMetadata = new DataFieldMetadata("name", DataFieldMetadata.STRING_FIELD, "|");
				metadata.addField(fieldMetadata);
				fieldMetadata = new DataFieldMetadata("detail", DataFieldMetadata.STRING_FIELD, "|");
				metadata.addField(fieldMetadata);
				fieldMetadata = new DataFieldMetadata("order_id", DataFieldMetadata.STRING_FIELD, "|");
				metadata.addField(fieldMetadata);
				fieldMetadata = new DataFieldMetadata("customer_id", DataFieldMetadata.STRING_FIELD, "|");
				metadata.addField(fieldMetadata);
				DataRecord record = new DataRecord(metadata);
				record.init();
				records.put(Integer.valueOf(port), record);
			}
			}
			return records.get(Integer.valueOf(port));
		}
	}
	
	public static List<Customer> getTestData() {
		
		List<Customer> customers = new ArrayList<Customer>();
		
		Customer customer = new Customer();
		customer.setName("Johnny Cash");
		customer.setId("1");
		customers.add(customer);
		
		Address address = new Address();
		address.setCity("New York");
		customer.setAddress(address);
		
		Order order = new Order();
		order.setId("1");
		order.setDeliveryDate(new Date(0));
		customer.getOrders().add(order);
		
		Product product = new Product();
		product.setDescription("classical novel about the power of conscience");
		product.setName("The Tale Tell Hearth");
		product.setId("1");
		order.getItems().add(product);
		
		product = new Product();
		product.setId("2");
		product.setName("Forgotten Lore Vol. III");
		product.setDescription("good reading for bleak December nights");
		product.getDetails().add("in hard, leather cover");
		order.getItems().add(product);
		
		customer = new Customer();
		customer.setName("Grande Ope");
		customer.setId("2");
		customers.add(customer);
		
		order = new Order();
		order.setId("2");
		order.setDeliveryDate(new Date(10000000000L));
		customer.getOrders().add(order);
		
		product = new Product();
		product.setName("The Raven and Other Poems.");
		product.setDescription("best from poetry of romanticism");
		product.setId("4");
		product.getDetails().add("now 50% off");
		
		order.getItems().add(product);
		
		order = new Order();
		order.setId("3");
		customer.getOrders().add(order);
		
		return customers;
	}
	
	private static class RecordReceiver implements DataRecordReceiver {
		
		private Map<Integer, List<DataRecord>> records = new HashMap<Integer, List<DataRecord>>();
		
		@Override
		public void receive(DataRecord record, int port) {
			
			List<DataRecord> portRecords = records.get(Integer.valueOf(port));
			if (portRecords == null) {
				portRecords = new ArrayList<DataRecord>();
				records.put(Integer.valueOf(port), portRecords);
			}
			portRecords.add(record.duplicate());
		}
		
		@Override
		public void exceptionOccurred(BadDataFormatException e)
			throws AbortParsingException {
		}
	}
}
