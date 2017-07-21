package org.jetel.ctl;

import java.util.HashSet;
import java.util.Set;

import org.jetel.ctl.ASTnode.CLVFBlock;
import org.jetel.ctl.ASTnode.CLVFForStatement;
import org.jetel.ctl.ASTnode.CLVFForeachStatement;
import org.jetel.ctl.ASTnode.CLVFFunctionDeclaration;
import org.jetel.ctl.ASTnode.CLVFImportSource;
import org.jetel.ctl.ASTnode.CLVFParameters;
import org.jetel.ctl.ASTnode.CLVFStart;
import org.jetel.ctl.ASTnode.CLVFSwitchStatement;
import org.jetel.ctl.ASTnode.CLVFVariableDeclaration;
import org.jetel.ctl.ASTnode.CLVFWhileStatement;
import org.jetel.ctl.ASTnode.SimpleNode;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;


public class DuplicationTest extends InterpreterTest {
	
	/**
	 * The following AST nodes may not appear as expressions,
	 * therefore it is not necessary to test them for CLO-1895.
	 */
	private static final Set<Class<?>> EXCLUDED_NODES = new HashSet<>();
	static {
		EXCLUDED_NODES.add(CLVFFunctionDeclaration.class);
		EXCLUDED_NODES.add(CLVFVariableDeclaration.class);
		EXCLUDED_NODES.add(CLVFBlock.class);
		EXCLUDED_NODES.add(CLVFParameters.class);
		EXCLUDED_NODES.add(CLVFForStatement.class);
		EXCLUDED_NODES.add(CLVFForeachStatement.class);
		EXCLUDED_NODES.add(CLVFImportSource.class);
		EXCLUDED_NODES.add(CLVFStart.class);
		EXCLUDED_NODES.add(CLVFSwitchStatement.class);
		EXCLUDED_NODES.add(CLVFWhileStatement.class);
	}

	@Override
	protected ITLCompiler createCompiler(TransformationGraph graph, DataRecordMetadata[] inMetadata,
			DataRecordMetadata[] outMetadata) {
		return new TLCompiler(graph, inMetadata, outMetadata, "UTF-8") {

			@Override
			protected ASTBuilder createASTBuilder() {
				return new ASTBuilder(graph,inMetadata,outMetadata,parser.getFunctions(),problemReporter) {

					@Override
					protected Object visitNode(SimpleNode node, Object data) {
						if (node != null && node.jjtHasChildren()) {
							for (int i = 0; i < node.jjtGetNumChildren(); i++) {
								SimpleNode child = (SimpleNode) node.jjtGetChild(i);
								if (!EXCLUDED_NODES.contains(child.getClass())) {
									SimpleNode duplicate = child.duplicate();
									node.jjtAddChild(duplicate, i);
									duplicate.jjtSetParent(node);
								}
								node.jjtGetChild(i).jjtAccept(this, data);
							}
						}

						return node;
					}
					
				};
			}
			
		};
	}

}
