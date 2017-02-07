//#CTL2
integer key;
 
key=-1; // assign value to it

/* 
  sample function, just to show how things work
*/
function integer sum(integer a, integer b){
	return a+b;
}

function integer transform() {
	$0.CUSTOMERID = $0.CustomerID;
	$0.PRODUCTID = ++key;
	$0.ORDERID = $0.OrderID;
	$0.CUSTOMER = join(';', [$0.ShipName, $0.ShipAddress, $0.ShipCity, $0.ShipCountry]);
	$0.SHIPTIME = dateDiff($0.RequiredDate,$0.ShippedDate,day);
	return ALL;
}

