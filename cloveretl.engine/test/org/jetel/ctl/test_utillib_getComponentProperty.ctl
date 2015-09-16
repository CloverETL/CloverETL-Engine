string transform;
string emptyName;
string nullName;
string nonExisting;

function integer transform() {
	transform = getComponentProperty("transform");
	emptyName = getComponentProperty("");
	nullName = getComponentProperty(null);
	nonExisting = getComponentProperty("nonExisting");
	
	return 0;
}