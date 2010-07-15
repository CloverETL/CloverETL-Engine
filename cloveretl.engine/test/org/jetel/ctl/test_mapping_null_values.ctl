function integer transform() {
	// assign null values from third input record to first output record
	$0.Name = null;
	$0.Name = $2.Name;
	$0.Age = $2.Age;
	$0.City = $2.City;
	$0.Born = $2.Born;
	$0.BornMillisec = $2.BornMillisec;
	$0.Value = $2.Value;
	$0.Flag = $2.Flag;
	$0.ByteArray = $2.ByteArray;
	$0.Currency = $2.Currency;
	
	return 0;
}