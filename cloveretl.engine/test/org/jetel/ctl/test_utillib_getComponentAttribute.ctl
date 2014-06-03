string transform;
string emptyName;
string nullName;
string nonExisting;

function integer transform() {
	transform = getComponentAttribute("transform");
	emptyName = getComponentAttribute("");
	nullName = getComponentAttribute(null);
	nonExisting = getComponentAttribute("nonExisting");
	
	return 0;
}