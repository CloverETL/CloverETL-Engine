//#TL
int key;


 
key=-1; // assign value to it

/* 
  sample function, just to show how things work
*/
function sum(a,b){
	return a+b;
}


function transform() {
	$0.CUSTOMERID := $0.CustomerID;
	$0.PRODUCTID := ++key;
	$0.ORDERID := $0.OrderID;
	$0.CUSTOMER := $0.ShipName+$0.ShipAddress+$0.ShipCity+$0.ShipCountry;
	$0.SHIPTIME := datediff($0.RequiredDate,$0.ShippedDate,day);
}

