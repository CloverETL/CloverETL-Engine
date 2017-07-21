package com.cloveretl.examples;

import org.fluttercode.datafactory.RandomPerson;
import org.fluttercode.datafactory.impl.DataFactory;
import org.jetel.component.DataRecordGenerate;
import org.jetel.data.DataRecord;
import org.jetel.exception.TransformException;


public class RandomPersonGen extends DataRecordGenerate {

	
	enum Fields { firstname, lastname,birthdate, address1,address2, address3, city,state, statecode, zip, phone, email };
	
	
	private DataFactory dataFactory;
	
	@Override
	public boolean init(){
		dataFactory = new DataFactory();
		dataFactory.randomize((int)System.currentTimeMillis());		
		return true;
	}
	
	@Override
	public int generate(DataRecord[] arg0) throws TransformException {
	RandomPerson person = dataFactory.getRandomPerson();
		
		arg0[0].getField(Fields.firstname.ordinal()).setValue(person.getFirstName());
		arg0[0].getField(Fields.lastname.ordinal()).setValue(person.getLastName());
		arg0[0].getField(Fields.birthdate.ordinal()).setValue(person.getBirthDate());
		arg0[0].getField(Fields.address1.ordinal()).setValue(person.getAddress1());
		arg0[0].getField(Fields.address2.ordinal()).setValue(person.getAddress2());
		arg0[0].getField(Fields.address3.ordinal()).setValue(person.getAddress3());
		arg0[0].getField(Fields.city.ordinal()).setValue(person.getCity());
		arg0[0].getField(Fields.state.ordinal()).setValue(person.getState());
		arg0[0].getField(Fields.statecode.ordinal()).setValue(person.getStateAbbreviation());
		arg0[0].getField(Fields.zip.ordinal()).setValue(person.getZip());
		arg0[0].getField(Fields.phone.ordinal()).setValue(person.getPhone());
		arg0[0].getField(Fields.email.ordinal()).setValue(person.getEmail());
		
		return DataRecordGenerate.ALL;
		
	}

}
