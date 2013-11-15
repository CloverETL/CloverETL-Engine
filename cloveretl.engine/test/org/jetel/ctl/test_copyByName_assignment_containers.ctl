function integer transform() {
	$out.metadata1.* = $in.metadata2.*;
	$out.metadata2.* = $in.metadata1.*;
	
	return 0;
}