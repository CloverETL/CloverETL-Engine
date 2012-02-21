package org.jetel.component.groovy.support

import org.jetel.metadata.DataRecordMetadata
import groovy.text.SimpleTemplateEngine

String.metaClass.normalize = {
	delegate.split("_").collect { word ->
		word.toLowerCase().capitalize()
	}.join("")
}

class Generator {
	
	DataRecordMetadata[] inputMetadata
	DataRecordMetadata[] outputMetadata
	String code
	String rootPath
	
	SimpleTemplateEngine templateEngine = new SimpleTemplateEngine()
	
	Generator(DataRecordMetadata[] sourcesMetadata, DataRecordMetadata[] targetMetadata, String code, String rootPath) {
		inputMetadata = sourcesMetadata
		outputMetadata = targetMetadata
		
		this.code = code
		this.rootPath = rootPath
	}
	
	String generate() {
		
		String result = generateHeader()
		
		inputMetadata.eachWithIndex{ metadata, index ->
			result += generateInputPortAccessor(metadata, index)
		}
		
		result += generateInputs(inputMetadata)
		
		outputMetadata.eachWithIndex{ metadata, index ->
			result += generateOutputPortAccessor(metadata, index)
		}	
		
		result += generateOutputs(outputMetadata)
		
		result += generateUserComponent()
		result += generateTransformComponent()
		
		result
	}
	
	String generateHeader() {
		executeTemplate("header.template", [:])
	}
	
	String generateTransformComponent() {
		executeTemplate("transform.template", [:])
	}
	
	String generateUserComponent() {
		executeTemplate("custom.template", ["code":code])
	}
	
	String generateInputPortAccessor(DataRecordMetadata metadata, int index) {
		def methods = getMethodsData(metadata)
		executeTemplate("input_port.template", ["index":index, "methods":methods])
	}
	
	String generateOutputPortAccessor(DataRecordMetadata metadata, int index) {
		def methods = getMethodsData(metadata)
		executeTemplate("output_port.template", ["index":index, "methods":methods])
	}
	
	String generateInputs(DataRecordMetadata[] metadata) {
		executeTemplate("inputs.template", ["metadata": metadata])
	}
	
	String generateOutputs(DataRecordMetadata[] metadata) {
		executeTemplate("outputs.template", ["metadata": metadata])
	}
	
	String executeTemplate(String templatePath, binding) {
		String template = (new File(rootPath+"/"+templatePath)).getText()
		
		def templateInst = templateEngine.createTemplate(template).make(binding)
		
		templateInst.toString()
	}
	
	String toGroovyType(String cloverType) {
		switch (cloverType) {
		case "string":
			"String"
			break;
		
		case "long":
			"Long"
			break;
			
		case "integer":
			"Integer"
			break;
		
		case "number":
			"Double"
			break;
			
		case "decimal":
			"BigDecimal"
			break;

		default:
			throw new RuntimeException("Unknown type!")
		}
	}
	
	def getMethodsData(DataRecordMetadata metadata){
		def methods = []
		
		metadata.eachWithIndex {field, fieldIndex ->
			methods.add([
				"index": fieldIndex,
				"return_type": toGroovyType(field.getTypeAsString()),
				"name": field.name.normalize()
			])
		}
		
		methods
	}
}

Generator generator = new Generator($inputMetadata, $outputMetadata, $code, $rootPath)
generator.generate()