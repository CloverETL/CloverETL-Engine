import org.jetel.component.DataRecordTransform;
import org.jetel.data.DataRecord;

public class TransformForDBjoin extends DataRecordTransform{

	public boolean init(){
		return true;
	}

	public int transform(DataRecord[] source, DataRecord[] target){
		target[0].getField("customer_id").setValue(source[0].getField("customer_id").getValue());
  		target[0].getField("lname").setValue(source[0].getField("lname").getValue());
		target[0].getField("fname").setValue(source[0].getField("fname").getValue());
		target[0].getField("city").setValue(source[0].getField("city").getValue());
		target[0].getField("country").setValue(source[0].getField("country").getValue());
		target[0].getField("employee_id").setValue(source[1].getField("EMPLOYEE_ID").getValue());
		target[0].getField("full_name").setValue(source[1].getField("FULL_NAME").getValue());

		return ALL;
	}

}