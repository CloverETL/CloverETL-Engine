/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.ctl;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.jetel.ctl.ASTnode.CLVFArrayAccessExpression;
import org.jetel.ctl.ASTnode.CLVFAssignment;
import org.jetel.ctl.ASTnode.CLVFBlock;
import org.jetel.ctl.ASTnode.CLVFCaseStatement;
import org.jetel.ctl.ASTnode.CLVFDictionaryNode;
import org.jetel.ctl.ASTnode.CLVFFieldAccessExpression;
import org.jetel.ctl.ASTnode.CLVFForeachStatement;
import org.jetel.ctl.ASTnode.CLVFFunctionDeclaration;
import org.jetel.ctl.ASTnode.CLVFImportSource;
import org.jetel.ctl.ASTnode.CLVFLiteral;
import org.jetel.ctl.ASTnode.CLVFLookupNode;
import org.jetel.ctl.ASTnode.CLVFMemberAccessExpression;
import org.jetel.ctl.ASTnode.CLVFParameters;
import org.jetel.ctl.ASTnode.CLVFSequenceNode;
import org.jetel.ctl.ASTnode.CLVFStart;
import org.jetel.ctl.ASTnode.CLVFStartExpression;
import org.jetel.ctl.ASTnode.CLVFSwitchStatement;
import org.jetel.ctl.ASTnode.CLVFType;
import org.jetel.ctl.ASTnode.CLVFUnaryNonStatement;
import org.jetel.ctl.ASTnode.CLVFVariableDeclaration;
import org.jetel.ctl.ASTnode.Node;
import org.jetel.ctl.ASTnode.SimpleNode;
import org.jetel.ctl.data.TLType;
import org.jetel.ctl.data.TLTypePrimitive;
import org.jetel.ctl.data.UnknownTypeException;
import org.jetel.data.Defaults;
import org.jetel.data.lookup.LookupTable;
import org.jetel.data.sequence.Sequence;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.NotInitializedException;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.dictionary.Dictionary;
import org.jetel.graph.dictionary.IDictionaryType;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.string.StringUtils;

/**
 * Implementation of semantic checking for CTL compiler.
 * Resolves external references, functions and records.
 * Performs code structure validation, mostly for correct 
 * derivation of expression statements.
 * 
 * @author Michal Tomcanyi <michal.tomcanyi@javlin.cz>
 *
 */
public class ASTBuilder extends NavigatingVisitor {

	/** Void metadata used by Rollup transforms when no group accumulator is used. */
	private static final DataRecordMetadata VOID_METADATA =
			new DataRecordMetadata(Defaults.CTL.VOID_METADATA_NAME);

	/** Metadata for component's input ports */
	private final DataRecordMetadata[] inputMetadata; // may be null
	/** Metadata for component's output ports */
	private final DataRecordMetadata[] outputMetadata; // may be null
	/** Name -> position mapping for input ports */
	private final Map<String, Integer> inputRecordsMap = new TreeMap<String, Integer>();
	/** Name -> position mapping for output ports */
	private final Map<String, Integer> outputRecordsMap = new TreeMap<String, Integer>();
	/** Name/ID -> metadata mapping for record-type variables */
	private final Map<String, DataRecordMetadata> graphMetadata = new TreeMap<String, DataRecordMetadata>();
	/** Name/ID -> lookup mapping for lookup nodes */
	private final Map<String, LookupTable> lookupMap = new TreeMap<String, LookupTable>();
	/** Name/ID -> lookup mapping for sequences */
	private final Map<String, Sequence> sequenceMap = new TreeMap<String, Sequence>();
	/** Function declarations */
	private final Map<String, List<CLVFFunctionDeclaration>> declaredFunctions;

	private final Dictionary dictionary;
	
	/** Set of ambiguous input metadata names. */
	private final Set<String> ambiguousInputMetadata = new HashSet<String>();
	/** Set of ambiguous output metadata names. */
	private final Set<String> ambiguousOutputMetadata = new HashSet<String>();
	/** Set of ambiguous graph metadata names. */
	private final Set<String> ambiguousGraphMetadata = new HashSet<String>();
	/** Set of ambiguous lookup table names. */
	private final Set<String> ambiguousLookupTables = new HashSet<String>();
	/** Set of ambiguous input sequence names. */
	private final Set<String> ambiguousSequences = new HashSet<String>();

	/** Problem collector */
	private ProblemReporter problemReporter;
	
	private LiteralParser literalParser = new LiteralParser();
	
	public ASTBuilder(TransformationGraph graph, DataRecordMetadata[] inputMetadata, DataRecordMetadata[] outputMetadata,
			Map<String, List<CLVFFunctionDeclaration>> declaredFunctions, ProblemReporter problemReporter) {
		this.inputMetadata = inputMetadata;
		this.outputMetadata = outputMetadata;
		this.declaredFunctions = declaredFunctions;
		this.problemReporter = problemReporter;
		this.dictionary = graph != null ? graph.getDictionary() : null;

		// populate name -> position mappings

		// input metadata names can clash
		if (inputMetadata != null) {
			for (int i = 0; i < inputMetadata.length; i++) {
				DataRecordMetadata m = inputMetadata[i];
				if (m != null) {
					if (inputRecordsMap.put(m.getName(), i) != null) {
						ambiguousInputMetadata.add(m.getName());
					}
				}
			}
		}

		// the same for output just different error message
		if (outputMetadata != null) {
			for (int i = 0; i < outputMetadata.length; i++) {
				DataRecordMetadata m = outputMetadata[i];
				if (m != null) {
					if (outputRecordsMap.put(m.getName(), i) != null) {
						ambiguousOutputMetadata.add(m.getName());
					}
				}
			}
		}

		if (graph != null) {
			
			// all graph metadata for resolving record-type variable declarations
			Iterator<String> mi = graph.getDataRecordMetadata();
			while (mi.hasNext()) {
				DataRecordMetadata m = graph.getDataRecordMetadata(mi.next());

				if (graphMetadata.put(m.getName(), m) != null) {
					ambiguousGraphMetadata.add(m.getName());
				}
			}
	
			// lookup tables
			Iterator<String> li = graph.getLookupTables();
			while (li.hasNext()) {
				LookupTable t = graph.getLookupTable(li.next());

				if (lookupMap.put(t.getName(), t) != null) {
					ambiguousLookupTables.add(t.getName());
				}
			}
	
			// sequences
			Iterator<String> si = graph.getSequences();
			while (si.hasNext()) {
				Sequence s = graph.getSequence(si.next());

				if (sequenceMap.put(s.getName(), s) != null) {
					ambiguousSequences.add(s.getName());
				}
			}

		}
	}

	/**
	 * AST builder entry method for complex transformations
	 * 
	 * @param tree
	 *            AST to resolve
	 */
	public void resolveAST(CLVFStart tree) {
		visit(tree, null);
		checkLocalFunctionsDuplicities();
	}
	
	/**
	 * AST builder entry method for simple expression (Filter)
	 * @param tree
	 */
	public void resolveAST(CLVFStartExpression tree) {
		visit(tree,null);
	}

	@Override
	public Object visit(CLVFImportSource node, Object data) {
		// store current "import context" so we can restore it after parsing this import
		String importFileUrl = problemReporter.getImportFileUrl();
		ErrorLocation errorLocation = problemReporter.getErrorLocation();

        // set new "import context", propagate error location if already defined
		problemReporter.setImportFileUrl(node.getSourceToImport());
		problemReporter.setErrorLocation((errorLocation != null)
				? errorLocation : new ErrorLocation(node.getBegin(), node.getEnd()));

		Object result = super.visit(node, data);

		// restore current "import context"
		problemReporter.setImportFileUrl(importFileUrl);
		problemReporter.setErrorLocation(errorLocation);

		return result;
	}

	@Override
	public CLVFArrayAccessExpression visit(CLVFArrayAccessExpression node, Object data) {
		node.jjtGetChild(0).jjtAccept(this, data);
		node.jjtGetChild(1).jjtAccept(this, false); // CL-2562: the index must not be treated as the LHS of the assignment
		return node;
	}

	/**
	 * Field references are context-sensitive. Field reference "$out.someField" on LHS resolves to an OUTPUT field
	 * 'someField' in record name 'out'. The flag is sent in 'data' object.
	 */
	@Override
	public CLVFAssignment visit(CLVFAssignment node, Object data) {
		// if LHS, must send flag down the tree to inform FieldAccessExpression it is an output field
		node.jjtGetChild(0).jjtAccept(this, true);
		node.jjtGetChild(1).jjtAccept(this, false);
		return node;
	}
	
	@Override
	public CLVFBlock visit(CLVFBlock node, Object data) {
		super.visit(node,data);
		
		checkBlockParseTree(node);

		return node;
	}
	
	@Override
	public Object visit(CLVFStart node, Object data) {
		super.visit(node,data);
		
		checkBlockParseTree(node);

		return node;
	}

	/**
	 * Calculates positional references according to field names. 
	 * Validates that metadata and field references are valid
	 * in the current graph.
	 */
	@Override
	public CLVFFieldAccessExpression visit(CLVFFieldAccessExpression node, Object data) {

		if (isGlobal(node)) {
			error(node, "Unable to access record field in global scope");
		}

		
		Object id = node.getRecordId();
		
		Boolean isOutput;
		Boolean isLHS = data != null ? (Boolean)data : false;
		String discriminator = node.getDiscriminator();
		if (discriminator != null) {
			if (discriminator.equals("in")) {
				if (isLHS) {
					error(node, "Input record cannot be assigned to");
				}
				isOutput = false;
			} else if (discriminator.equals("out")) {
				isOutput = true;
			} else {
				throw new IllegalArgumentException(discriminator);
			}
		} else {
			// if the FieldAccessExpression appears somewhere except assignment 
			// the 'data' will be null so we treat it as a reference to the input field
			isOutput = isLHS;
			node.setMetadata(null);
		}
		node.setOutput(isOutput);
		
		Integer recordPos = null;

		// resolve positional reference for record if necessary
		if (node.getRecordId() != null) {
			recordPos = node.getRecordId();
		} else {
			// calculate positional reference
			recordPos = isOutput ? getOutputPosition(node.getRecordName()) : getInputPosition(node.getRecordName());
			if (recordPos != null) {
				Set<String> ambiguousMetadata = isOutput ? ambiguousOutputMetadata : ambiguousInputMetadata;

				if (ambiguousMetadata.contains(node.getRecordName())) {
					// metadata names can clash - warn user that we will not be able to resolve them correctly
					warn(node, (isOutput ? "Output" : "Input") + " record name '" + node.getRecordName() + "' is ambiguous",
							"Use positional access or rename metadata to a unique name");
				}

				node.setRecordId(recordPos);
			} else {
				error(node, "Unable to resolve " + (isOutput ? "output" : "input") + " metadata '" + node.getRecordName() + "'");
				node.setType(TLType.ERROR);
				return node; // nothing else to do
			}

		}

		// check if we have metadata for this record
		DataRecordMetadata metadata = isOutput ? getOutputMetadata(recordPos) : getInputMetadata(recordPos);
		if (metadata != null) {
			node.setMetadata(metadata);
		} else {
			error(node, "Cannot " + (isOutput ? "write to output" : "read from input") + " port '" + id + "'", "Either the port has no edge connected or the operation is not permitted.");
			node.setType(TLType.ERROR);
			return node;
		}

		if (node.isWildcard()) {
			// this is not a record reference - but access all record fields
			node.setType(TLType.forRecord(node.getMetadata()));
			return node; // this node access record -> we do not want resolve field access
		}

		// resolve and validate field identifier using record metadata
		Integer fieldId;
		if (node.getFieldId() != null) {
			fieldId = node.getFieldId();
			// fields are ordered from zero
			if (fieldId > metadata.getNumFields()-1) {
				error(node,"Field '" + fieldId + "' is out of range for record '" + metadata.getName() + "'");
				node.setType(TLType.ERROR);
				return node;
			}
		} else {
			fieldId = metadata.getFieldPosition(node.getFieldName());
			if (fieldId >= 0) {
				node.setFieldId(fieldId);
			} else {
				ErrorMessage m = error(node, "Field '" + node.getFieldName() + "' does not exist in record '" + metadata.getName() + "'");
				m.setDetail(new MetadataErrorDetail(node));
				
				node.setType(TLType.ERROR);
				return node;
			}
		}

		try {
			node.setType(TLTypePrimitive.fromCloverType(node.getMetadata().getField(fieldId)));
		} catch (UnknownTypeException e) {
			error(node, "Field type '" + e.getType() + "' does not match any CTL type");
			node.setType(TLType.ERROR);
			throw new IllegalArgumentException(e);
		}
		
		return node;
	}
	
	/**
	 * @param node
	 * @return
	 */
	private boolean isGlobal(SimpleNode node) {
		Node actualNode = node;
		boolean isLastNodeCLVFStartExpression = false;
		while ((actualNode = actualNode.jjtGetParent()) != null) {
			isLastNodeCLVFStartExpression = (actualNode instanceof CLVFStartExpression);
			if (actualNode instanceof CLVFFunctionDeclaration)
				return false;
		}
		
		// if root node of SimpleNode hierarchy is CLVFStartExpression
		// then only simple expression is compiled (for instance for ExtFilter component 
		// see TLCompiler.validateExpression()) and record field access in global scope is allowed
		return !isLastNodeCLVFStartExpression;
	}


	@Override
	public Object visit(CLVFForeachStatement node, Object data) {
		super.visit(node, data);

		CLVFVariableDeclaration loopVar = (CLVFVariableDeclaration)node.jjtGetChild(0);
		if (loopVar.jjtGetNumChildren() > 1) {
			error(loopVar,"Foreach loop variable must not have a initializer","Delete the initializer expression");
			node.setType(TLType.ERROR);
		}
		
		return node;
	}
	
	/**
	 * Populates function return type and formal parameters type.
	 * Sets node's type to {@link TLType#ERROR} in case of any issues.
	 */
	@Override
	public Object visit(CLVFFunctionDeclaration node, Object data) {
		// scan (return type), parameters and function body
		super.visit(node,data);
		
//		checkStatementOrBlock(node.jjtGetChild(2));
		
		CLVFType retType = (CLVFType)node.jjtGetChild(0);
		node.setType(retType.getType());
		
		CLVFParameters params = (CLVFParameters)node.jjtGetChild(1);
		TLType[] formalParm = new TLType[params.jjtGetNumChildren()];
		for (int i=0; i<params.jjtGetNumChildren(); i++) {
			CLVFVariableDeclaration p = (CLVFVariableDeclaration)params.jjtGetChild(i);
			if ((formalParm[i] = p.getType()) == TLType.ERROR) {
				// if function accepts some metadata-typed params, we can have error resolving them
				node.setType(TLType.ERROR);
			}
		}
		
		node.setFormalParameters(formalParm);
		
		
		return data;
	}
	
	/**
	 * Parses literal value to the corresponding object
	 * May result in additional errors in parsing.
	 * 
	 * @see #parseLiteral(CLVFLiteral)
	 */
	@Override
	public Object visit(CLVFLiteral node, Object data) {
		parseLiteral(node);
		return data;
	}

	/**
	 * Resolves of identifier to the corresponding graph lookup table
	 */
	@Override
	public CLVFLookupNode visit(CLVFLookupNode node, Object data) {
		super.visit(node, data);
		
		LookupTable table = resolveLookup(node.getLookupName());
		if (table == null) {
			error(node, "Unable to resolve lookup table '" + node.getLookupName() + "'");
			node.setType(TLType.ERROR);
			return node;
		} else {
			if (ambiguousLookupTables.contains(table.getName())) {
				warn("Lookup table name '" + table.getName() + "' is ambiguous",
						"Rename the lookup table to a unique name");
			}

			node.setLookupTable(table);
			// type will be set in composite reference node as it will build up the lookup node completely
		}
		
		// we need to call init() to get access to metadata, keys, etc. (e.g. for DBLookupTable)
		try {
			if (! node.getLookupTable().isInitialized()) {
				node.getLookupTable().init();
			}
		} catch (Exception e) {
			// underlying lookup cannot be initialized
			error(node, ExceptionUtils.getMessage("Lookup table has configuration error", e));
			node.setType(TLType.ERROR);
			return node;
		} 

		TLType getOrCountReturnType = null;
		DataRecordMetadata ret = null;
		switch (node.getOperation()) {
		// OP_COUNT and OP_GET use a common arguments validation and only differ in return type 
		case CLVFLookupNode.OP_COUNT:
				getOrCountReturnType = TLTypePrimitive.INTEGER;
				/* ------ no break here deliberately : key validation follows ----- */
		case CLVFLookupNode.OP_GET:
			try {
				DataRecordMetadata keyRecordMetadata = node.getLookupTable().getKeyMetadata();
				LinkedList<Integer> decimalInfo = new LinkedList<Integer>();
				if (keyRecordMetadata == null) {
					// fail safe step in case getKey() does not work properly -> exception is caught below
					throw new UnsupportedOperationException();
				}
				
				// extract lookup parameter types
				TLType[] formal = new TLType[keyRecordMetadata.getNumFields()];
				try {
					for (int i=0; i<keyRecordMetadata.getNumFields(); i++) {
						formal[i] = TLTypePrimitive.fromCloverType(keyRecordMetadata.getField(i));
						if (formal[i].isDecimal()) {
							final DataFieldMetadata f = keyRecordMetadata.getField(i);
							decimalInfo.add(f.getFieldProperties().getIntProperty(DataFieldMetadata.LENGTH_ATTR));
							decimalInfo.add(f.getFieldProperties().getIntProperty(DataFieldMetadata.SCALE_ATTR));
						}
					}
					node.setFormalParameters(formal);
					node.setDecimalPrecisions(decimalInfo);
				} catch (UnknownTypeException e) {
					error(node,"Lookup returned an unknown parameter type: '" + e.getType() + "'");
					node.setType(TLType.ERROR);
					return node;
				}
				
			} catch (UnsupportedOperationException e) {
				// can happen in case the JDBC driver does not provide info about SQL params
				warn(node, "Validation of lookup keys is not supported for this lookup table");
				
			} catch (NotInitializedException e) {
				// should never happen
				error(node,"Lookup not initialized");
				node.setType(TLType.ERROR);
				return node;
			} catch (ComponentNotReadyException e) {
				// underlying lookup is misconfigured
				error(node, ExceptionUtils.getMessage("Lookup table has configuration errors", e));
				node.setType(TLType.ERROR);
				return node;
			}
			
			// if return type is already set to integer, we are validating a count() function - see case above
			// otherwise we are validating get() function and must compute return type from metadata
			if (getOrCountReturnType == null) {
				ret = node.getLookupTable().getMetadata();
				if (ret == null) {
					error(node,"Lookup table has no metadata specified");
					node.setType(TLType.ERROR);
					return node;
				}
				getOrCountReturnType = TLType.forRecord(ret);
			}
			node.setType(getOrCountReturnType);
			break;
			
		case CLVFLookupNode.OP_NEXT:
			// extract return type
			ret = node.getLookupTable().getMetadata();
			if (ret == null) {
				error(node,"Lookup table has no metadata specified");
				node.setType(TLType.ERROR);
				return node;
			}				
			node.setType(TLType.forRecord(ret));
			break;
			
		case CLVFLookupNode.OP_PUT:
			if (!node.getLookupTable().isPutSupported()) {
				error(node,"Lookup table '" + node.getLookupName() + "' does not support the put() operation");
				node.setType(TLType.ERROR);
				return node;
			}
			// extract parameter type
			ret = node.getLookupTable().getMetadata();
			if (ret == null) {
				error(node,"Lookup table has no metadata specified");
				node.setType(TLType.ERROR);
				return node;
			}
			node.setFormalParameters(new TLType[] {TLType.forRecord(ret)});
			node.setType(TLTypePrimitive.BOOLEAN);
			break;
			
		}

		return node;
		
	}

	private static final TLType[] CONTAINER_ELEMENT_TYPES = {
		TLTypePrimitive.INTEGER,
		TLTypePrimitive.LONG,
		TLTypePrimitive.STRING,
		TLTypePrimitive.BOOLEAN,
		TLTypePrimitive.DATETIME,
		TLTypePrimitive.DOUBLE,
		TLTypePrimitive.DECIMAL,
		TLTypePrimitive.BYTEARRAY,
	};
	
	/**
	 * @return CTL data type based on given content type used in dictionary
	 * @note this method could moved to a proper places
	 */
	public static TLType getTypeByContentType(String contentType) {
		if (StringUtils.isEmpty(contentType)) {
			return null;
		}
		for (TLType t: CONTAINER_ELEMENT_TYPES) {
			if (t.name().equals(contentType)) {
				return t;
			}
		}
		return null;
	}
	
	@Override
	public Object visit(CLVFMemberAccessExpression node, Object data) {
		super.visit(node, data);
		
		//dictionary is not available, the ctl code is compiled without graph (graph == null)
		if (dictionary == null) {
			error(node, "Dictionary is not available");
			node.setType(TLType.ERROR);
			return data;
		}
		
		// access to dictionary
		final SimpleNode prefix = (SimpleNode)node.jjtGetChild(0);
		if (prefix.getId() == TransformLangParserTreeConstants.JJTDICTIONARYNODE) {
			
			IDictionaryType dictType = dictionary.getType(node.getName()) ; 
			if (dictType == null) {
				error(node, "Dictionary entry '" + node.getName() + "' does not exist");
				node.setType(TLType.ERROR);
				return data;
			}
			
			TLType tlType = dictType.getTLType();
			if( tlType == null){
				error(node, "Dictionary entry '" + node.getName() + " has type "+dictType.getTypeId()+" which is not supported in CTL");
				node.setType(TLType.ERROR);
				return node;
			} else if (tlType.isList()) {
				final String contentType = dictionary.getContentType(node.getName());
				if (!StringUtils.isEmpty(contentType)) {
					TLType elementType = getTypeByContentType(contentType);
					tlType = TLType.createList(elementType);
				}
			} else if (tlType.isMap()) {
				final String contentType = dictionary.getContentType(node.getName());
				if (!StringUtils.isEmpty(contentType)) {
					TLType elementType = getTypeByContentType(contentType);
					tlType = TLType.createMap(TLTypePrimitive.STRING, elementType);
				}
			}
			node.setType(tlType);
		}
		
		return data;
	}
	
	/**
	 * Resolves of identifier to the corresponding graph sequence.
	 * Sequence return type is defined by user in syntax.
	 */
	@Override
	public CLVFSequenceNode visit(CLVFSequenceNode node, Object data) {
		Sequence seq = resolveSequence(node.getSequenceName());
		if (seq == null) {
			error(node, "Unable to resolve sequence '" + node.getSequenceName() + "'");
			node.setType(TLType.ERROR);
		} else {
			if (ambiguousSequences.contains(seq.getName())) {
				warn("Sequence name '" + seq.getName() + "' is ambiguous", "Rename the sequence to a unique name");
			}

			node.setSequence(seq);
		}

		return node;
	}
	
	/**
	 * Computes case statements indices
	 */
	@Override
	public Object visit(CLVFSwitchStatement node, Object data) {
		super.visit(node,data);
		
		Map <Object, SimpleNode> map = new HashMap<Object, SimpleNode>();
		Set <SimpleNode> duplicates = new HashSet<SimpleNode>();
		
		ArrayList<Integer> caseIndices = new ArrayList<Integer>();
		for (int i=0; i<node.jjtGetNumChildren(); i++) {
			SimpleNode child = (SimpleNode)node.jjtGetChild(i);
			if (child.getId() == TransformLangParserTreeConstants.JJTCASESTATEMENT) {
				if (((CLVFCaseStatement)child).isDefaultCase()) {
					node.setDefaultCaseIndex(i);
				} else {
					caseIndices.add(i);
					CLVFLiteral caseLiteral = (CLVFLiteral) child.jjtGetChild(0);
					Object value = caseLiteral.getValue();
					SimpleNode otherNode;
					if ((otherNode = map.get(value)) != null) {
						duplicates.add(child);
						duplicates.add(otherNode);
					} else {
						map.put(value, child);
					}
					for (SimpleNode duplicateNode : duplicates) {
						error(duplicateNode, "Duplicate case");
					}
				}
			}
		}

		node.setCaseIndices((Integer[]) caseIndices.toArray(new Integer[caseIndices.size()]));
		return node;
	}
	
	
	@Override
	public Object visit(CLVFType node, Object data) {
		node.setType(createType(node));
		
		return node;
	}
	
	/**
	 * Analyzes if the unary expression is not just a negative literal.
	 * If yes, the negative literal is validated, parsed and the unary expression
	 * is replaced by the literal.
	 */
	@Override
	public Object visit(CLVFUnaryNonStatement node, Object data) {
		if (((CLVFUnaryNonStatement)node).getOperator() == TransformLangParserConstants.MINUS 
				&& ((SimpleNode)node.jjtGetChild(0)).getId() == TransformLangParserTreeConstants.JJTLITERAL) {
			CLVFLiteral lit = (CLVFLiteral)node.jjtGetChild(0);
			int idx = 0;
			final SimpleNode parent = (SimpleNode)node.jjtGetParent();
			// store position of the unary expression in idx 
			for (idx = 0; idx < parent.jjtGetNumChildren(); idx++) {
				if (parent.jjtGetChild(idx) == node) {
					break;
				}
			}

			switch (lit.getTokenKind()) {
			case TransformLangParserConstants.FLOATING_POINT_LITERAL:
			case TransformLangParserConstants.LONG_LITERAL:
			case TransformLangParserConstants.INTEGER_LITERAL:
			case TransformLangParserConstants.DECIMAL_LITERAL:
				lit.setValue(lit.getTokenKind(), "-" + lit.getValueImage());
				break;
			default:
				error(node,"Operator '-' is not defined for this type of literal");
				return node;
			}

			
			
			// initialize literal value and fall back early on error
			if (!parseLiteral(lit)) {
				return node;
			} 			
			
			// literal was correctly initialized - replace the unary minus in AST
			lit.jjtSetParent(node.jjtGetParent());
			parent.jjtAddChild(lit, idx);
			
			return lit;
		}
		
		// not a minus literal, but other unary expression, validate children
		super.visit(node, data);
		return node;
	}
	
	/**
	 * Sets type of variable from type node
	 */
	@Override
	public CLVFVariableDeclaration visit(CLVFVariableDeclaration node, Object data) {
		super.visit(node, data);
		CLVFType typeNode = (CLVFType) node.jjtGetChild(0);
		// set the type of variable
		node.setType(typeNode.getType());
		return node;
	}

	@Override
	public Object visit(CLVFDictionaryNode node, Object data) {
		super.visit(node, data);
		// actual type of dictionary entry is set while visiting MemberAccessExpression.
		// this is only a default value that is not used anywhere
		node.setType(TLTypePrimitive.STRING);
		
		return node;
		
	}
	

	// ----------------------------- Utility methods -----------------------------

	/**
	 * This method check that local declarations of functions do not contain any
	 * duplicate function declaration. Types of function parameters within declarations
	 * must have been already resolved (i.e. call this after AST pass has been completed)
	 * 
	 * Function declaration is duplicate iff it has the same:
	 * 	- return type
	 * 	- function name
	 *  - parameters
	 *  as some other locally declared function
	 */
	private void checkLocalFunctionsDuplicities() {
		for (String name : declaredFunctions.keySet()) {
			final List<CLVFFunctionDeclaration> functions = declaredFunctions.get(name);
			final int overloadCount = functions.size();
			if (overloadCount < 2) {
				// no duplicates possible
				continue;
			}
			
			for (int i=1; i<overloadCount; i++) {
				for (int j=i-i; j>=0; j--) {
					CLVFFunctionDeclaration valid = functions.get(j);
					CLVFFunctionDeclaration candidate = functions.get(i);
					
					/*
					 * This follows Java approach: overloading function must have different 
					 * parameters, difference only in return type is insufficient and is
					 * treated as a duplicate
					 */
					if (Arrays.equals(valid.getFormalParameters(), candidate.getFormalParameters())) {
						// the same name, return type and parameter types: duplicate
						error(valid,"Duplicate function '" + valid.toHeaderString() + "'");
						error(candidate,"Duplicate function '" + valid.toHeaderString() + "'");
						
					}
				}
			}
		}
	}
	

	/**
	 * Initializes literal by parsing its string representation into real type.
	 * 
	 * @return false (and reports error) when parsing did not succeed, true otherwise
	 */
	private boolean parseLiteral(CLVFLiteral lit) {
		String errorMessage = null;
		String hint = null;
		try {
			lit.computeValue(literalParser);
		} catch (NumberFormatException e) {
			switch (lit.getTokenKind()) {
			case TransformLangParserConstants.FLOATING_POINT_LITERAL:
				errorMessage = "Literal '" + lit.getValueImage() + "' is out of range for type 'number'";
				hint = "Use 'D' distincter to treat literal as 'decimal' ";
				break;
			case TransformLangParserConstants.LONG_LITERAL:
				errorMessage = "Literal '" + lit.getValueImage() + "' is out of range for type 'long'";
				hint = "Use 'D' distincter to treat literal as 'decimal'";
				break;
			case TransformLangParserConstants.INTEGER_LITERAL:
				errorMessage = "Literal '" + lit.getValueImage() + "' is out of range for type 'int'";
				hint = "Use 'L' distincter to treat literal as 'long'";
				break;
			default:
				// should never happen
				errorMessage = "Unrecognized literal type '" + lit.getTokenKind() + "' with value '" + lit.getValueImage() + "'";
				hint = "Report as bug";
				break;
			}
		} catch (ParseException e) {
			switch (lit.getTokenKind()) {
			case TransformLangParserConstants.DATE_LITERAL:
				errorMessage = ExceptionUtils.getMessage(e);
				hint = "Date literal must match format pattern 'YYYY-MM-dd' and has to be valid date value.";
				break;
			case TransformLangParserConstants.DATETIME_LITERAL:
				errorMessage = ExceptionUtils.getMessage(e);
				hint = "Date-time literal must match format pattern 'YYYY-MM-DD HH:MM:SS' and has to be valid date-time value.";
				break;
			default:
				// should never happen
				errorMessage = "Unrecognized literal type '" + lit.getTokenKind() + "' with value '" + lit.getValueImage() + "'";
				hint = "Report as bug";
				break;
			}
		}
		
		// report error and fall back early
		if (errorMessage != null) {
			error(lit,errorMessage,hint);
			return false;
		}
		
		return true;
	}
	
	private Integer getInputPosition(String name) {
		return inputRecordsMap.get(name);
	}

	private Integer getOutputPosition(String name) {
		return outputRecordsMap.get(name);
	}

	private DataRecordMetadata getInputMetadata(int recordId) {
		return getMetadata(inputMetadata, recordId);
	}

	private DataRecordMetadata getOutputMetadata(int recordId) {
		return getMetadata(outputMetadata, recordId);
	}

	private DataRecordMetadata getMetadata(DataRecordMetadata[] metadata, int recordId) {
		// no metadata specified on component, or metadata not assigned on edge corresponding to recordId
		if (metadata == null || recordId >= metadata.length || metadata[recordId] == null) {
			return null;
		}

		return metadata[recordId];
	}

	private Sequence resolveSequence(String name) {
		return sequenceMap.get(name);
	}

	private LookupTable resolveLookup(String name) {
		return lookupMap.get(name);
	}

	private DataRecordMetadata resolveMetadata(String recordName) {
		return graphMetadata.get(recordName);
	}

	private TLType createType(CLVFType typeNode) {
		switch (typeNode.getKind()) {
		case TransformLangParserConstants.INT_VAR:
			return TLTypePrimitive.INTEGER;
		case TransformLangParserConstants.LONG_VAR:
			return TLTypePrimitive.LONG;
		case TransformLangParserConstants.DOUBLE_VAR:
			return TLTypePrimitive.DOUBLE;
		case TransformLangParserConstants.DECIMAL_VAR:
			return TLTypePrimitive.DECIMAL;
		case TransformLangParserConstants.STRING_VAR:
			return TLTypePrimitive.STRING;
		case TransformLangParserConstants.DATE_VAR:
			return TLTypePrimitive.DATETIME;
		case TransformLangParserConstants.BYTE_VAR:
			return TLTypePrimitive.BYTEARRAY;
		case TransformLangParserConstants.BOOLEAN_VAR:
			return TLTypePrimitive.BOOLEAN;
		case TransformLangParserConstants.IDENTIFIER:
			DataRecordMetadata meta = resolveMetadata(typeNode.getMetadataName());
			if (meta == null) {
				if (voidMetadataAllowed(typeNode)) {
					meta = VOID_METADATA;
				} else {
					error(typeNode, "Unknown variable type or metadata name '" + typeNode.getMetadataName() + "'");
					return TLType.ERROR;
				}
			} else if (ambiguousGraphMetadata.contains(meta.getName())) {
				warn(typeNode, "Metadata name '" + meta.getName() + "' is ambiguous",
						"Rename the metadata to a unique name");
			} else if (voidMetadataAllowed(typeNode)) {
				warn(typeNode, "Reference to '" + VOID_METADATA.getName() + "' is ambiguous",
						"Rename metadata '" + VOID_METADATA.getName() + "'");
			}
			return TLType.forRecord(meta);
		case TransformLangParserConstants.MAP_VAR:
			TLType keyType = createType((CLVFType) typeNode.jjtGetChild(0));
			if (!keyType.isPrimitive()) {
				error(typeNode, "Map key must be a boolean, date, decimal, integer, long, number or string");
				return TLType.ERROR;
			}
			return TLType.createMap(keyType,
					createType((CLVFType) typeNode.jjtGetChild(1)));
		case TransformLangParserConstants.LIST_VAR:
			return TLType.createList(createType((CLVFType)typeNode.jjtGetChild(0)));
		case TransformLangParserConstants.VOID_VAR:
			return TLType.VOID;
		default:
			error(typeNode, "Unknown variable type: '" + typeNode.getKind() + "'");
			throw new IllegalArgumentException("Unknown variable type: '" + typeNode.getKind() + "'");
		}
	}

	private boolean voidMetadataAllowed(CLVFType typeNode) {
		if (!typeNode.getMetadataName().equals(VOID_METADATA.getName())) {
			return false;
		}

		Node parent = typeNode.jjtGetParent();

		return (parent != null && parent.jjtGetParent() instanceof CLVFParameters);
	}

	/**
	 * Checks if block contains only legal statements (or statement expressions)
	 * 
	 * @param node	block node to check
	 */
	private final void checkBlockParseTree(SimpleNode node) {
		for (int i=0; i<node.jjtGetNumChildren(); i++) {
			final SimpleNode child = (SimpleNode)node.jjtGetChild(i);
			switch (child.getId()) {
			case TransformLangParserTreeConstants.JJTASSIGNMENT:
			case TransformLangParserTreeConstants.JJTBLOCK:
			case TransformLangParserTreeConstants.JJTBREAKSTATEMENT:
			case TransformLangParserTreeConstants.JJTCASESTATEMENT:
			case TransformLangParserTreeConstants.JJTCONTINUESTATEMENT:
			case TransformLangParserTreeConstants.JJTDOSTATEMENT:
			case TransformLangParserTreeConstants.JJTFOREACHSTATEMENT:
			case TransformLangParserTreeConstants.JJTFORSTATEMENT:
			case TransformLangParserTreeConstants.JJTFUNCTIONCALL:
			case TransformLangParserTreeConstants.JJTIFSTATEMENT:
			case TransformLangParserTreeConstants.JJTLOOKUPNODE:
			case TransformLangParserTreeConstants.JJTPOSTFIXEXPRESSION:
			case TransformLangParserTreeConstants.JJTISNULLNODE:
			case TransformLangParserTreeConstants.JJTNVLNODE:
			case TransformLangParserTreeConstants.JJTNVL2NODE:
			case TransformLangParserTreeConstants.JJTIIFNODE:
			case TransformLangParserTreeConstants.JJTPRINTERRNODE:
			case TransformLangParserTreeConstants.JJTPRINTLOGNODE:
			case TransformLangParserTreeConstants.JJTPRINTSTACKNODE:
			case TransformLangParserTreeConstants.JJTRAISEERRORNODE:
			case TransformLangParserTreeConstants.JJTRETURNSTATEMENT:
			case TransformLangParserTreeConstants.JJTSEQUENCENODE:
			case TransformLangParserTreeConstants.JJTSWITCHSTATEMENT:
			case TransformLangParserTreeConstants.JJTUNARYSTATEMENT:
			case TransformLangParserTreeConstants.JJTVARIABLEDECLARATION:
			case TransformLangParserTreeConstants.JJTWHILESTATEMENT:
				// all expression statements that can occur within block
				break;
			case TransformLangParserTreeConstants.JJTFUNCTIONDECLARATION:
			case TransformLangParserTreeConstants.JJTIMPORTSOURCE:
				// these two are only in for CLVFStart
				break;
			default:
				error(child,"Syntax error, statement expected");
			break;
			}
		}
	}
	
	

	// ----------------- Error Reporting --------------------------

	private ErrorMessage error(SimpleNode node, String error) {
		return problemReporter.error(node.getBegin(), node.getEnd(),error, null);
	}

	private ErrorMessage error(SimpleNode node, String error, String hint) {
		return problemReporter.error(node.getBegin(),node.getEnd(),error,hint);
	}

	private ErrorMessage warn(SimpleNode node, String warn) {
		return problemReporter.warn(node.getBegin(),node.getEnd(), warn, null);
	}

	private ErrorMessage warn(SimpleNode node, String warn, String hint) {
		return problemReporter.warn(node.getBegin(),node.getEnd(), warn, hint);
	}

	private ErrorMessage warn(String warn, String hint) {
		return problemReporter.warn(1, 1, 1, 2, warn, hint);
	}

}
