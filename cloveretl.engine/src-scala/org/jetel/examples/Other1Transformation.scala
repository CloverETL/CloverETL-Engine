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
class Other1Transformation extends DataRecordTransform {

  def transform(in: Input, out: Output): Int = {
    out.$0.id = in.$0.id;
    out.$0.name = in.$0.name;
    out.$0.age = in.$0.age;
    
    return OK;
  }

  override def transform(inputRecords: Array[DataRecord], outputRecords: Array[DataRecord]): Int = {
    return transform(new Input(inputRecords), new Output(outputRecords));
  }

  private class Input(inputRecords: Array[DataRecord]) {
    val input0: Person = new Person(inputRecords(0));
    def $0: Person = {
      return input0;
    }
  }
  
  private class Output(outputRecords: Array[DataRecord]) {
    val output0: Person = new Person(outputRecords(0));
    def $0: Person = {
      return output0;
    }
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

