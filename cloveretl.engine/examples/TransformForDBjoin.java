import org.jetel.component.DataRecordTransform;
import org.jetel.data.DataRecord;

public class TransformForDBjoin extends DataRecordTransform{


	public boolean init(){

		return true;
	}

	public boolean transform(DataRecord[] source, DataRecord[] target){
	        

	
		target[0].getField(0).setValue(source[0].getField(0).getValue());
  		target[0].getField(1).setValue(source[0].getField(1).getValue());
		target[0].getField(2).setValue(source[0].getField(2).getValue());
		target[0].getField(3).setValue(source[1].getField(0).getValue());
		target[0].getField(4).setValue(source[1].getField(1).getValue());

		return true;
	}
}