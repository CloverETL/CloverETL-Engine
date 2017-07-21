/**
 *
 */
package my.test.scala
import org.jetel.component.DataRecordTransform
import org.jetel.data.DataRecord
import org.jetel.component.RecordTransform._

/**
 * @author Kokon
 *
 */
class MyTransformation extends DataRecordTransform {

  def transform(inPerson: Person, outPerson: Person): Int = {
    outPerson.id = inPerson.id;
    outPerson.age = inPerson.age;
    outPerson.name = inPerson.name;
    
    return OK;
  }
  
 ////////////////////////////////// 
  override def transform(inputRecords: Array[DataRecord], outputRecords: Array[DataRecord]): Int = {
    return transform(new Person(inputRecords(0)), new Person(outputRecords(0)));
  }
  
}

private class Person(innerRecord: DataRecord) {
  
  val dataRecord = innerRecord;
  
  def id : String = {
    return String.valueOf(dataRecord.getField("id").getValue());
  }
  
  def id_= (value : String) : Unit = {
    dataRecord.getField("id").setValue(value); 
  }

  def name : String = {
    return String.valueOf(dataRecord.getField("name").getValue());
  }
  
  def name_= (value : String) : Unit = {
    dataRecord.getField("name").setValue(value); 
  }

  def age : Integer = {
    return dataRecord.getField("age").getValue().asInstanceOf[Integer];
  }
  
  def age_= (value : Integer) : Unit = {
    dataRecord.getField("age").setValue(value); 
  }

}
