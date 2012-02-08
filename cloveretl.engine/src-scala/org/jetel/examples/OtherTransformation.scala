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
class OtherTransformation extends DataRecordTransform {

  def transform(): Int = {
    $out_0.id = $in_0.id;
    $out_0.name = $in_0.name;
    $out_0.age = $in_0.age;
    
    return OK;
  }

  private var input1: Person = _;
  private var output1: Person = _;

  override def transform(inputRecords: Array[DataRecord], outputRecords: Array[DataRecord]): Int = {
    input1 = new Person(inputRecords(0));
    output1 = new Person(outputRecords(0));
    return transform();
  }

  private def $in_0: Person = {
    return input1;
  }

  private def $out_0: Person = {
    return output1;
  }
  
  private class Person(innerRecord: DataRecord) {

    val dataRecord = innerRecord;

    def id: String = {
      return String.valueOf(dataRecord.getField("id").getValue());
    }

    def id_=(value: String): Unit = {
      dataRecord.getField("id").setValue(value);
    }

    def name: String = {
      return String.valueOf(dataRecord.getField("name").getValue());
    }

    def name_=(value: String): Unit = {
      dataRecord.getField("name").setValue(value);
    }

    def age: Integer = {
      return dataRecord.getField("age").getValue().asInstanceOf[Integer];
    }

    def age_=(value: Integer): Unit = {
      dataRecord.getField("age").setValue(value);
    }

  }

}

