//#CTL2
integer key;

key=-1; // assign value to it

/* 
  sample function, just to show how things work
*/
function number sum(number a, number b){
	return a+b;
}

function integer transform() {
	$0.CUSTOMERID = $0.CustomerID;
	$0.PRODUCTID = ++key;
	$0.ORDERID = $0.OrderID;
	$0.CUSTOMER = join(';',[$0.ShipName,$0.ShipAddress,$0.ShipCity,$0.ShipCountry]);
	$0.SHIPTIME = long2integer(dateDiff($0.RequiredDate,$0.ShippedDate,day));

	return 0;
}


