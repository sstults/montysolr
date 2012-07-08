package org.apache.lucene.queryParser.aqp.processors;

import org.apache.lucene.queryparser.flexible.messages.MessageImpl;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.messages.QueryParserMessages;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.aqp.AqpCommonTree;
import org.apache.lucene.queryparser.flexible.aqp.config.AqpFeedback;
import org.apache.lucene.queryparser.flexible.aqp.nodes.AqpANTLRNode;
import org.apache.lucene.queryparser.flexible.aqp.processors.AqpQProcessorPost;

public class AqpAdslabsQPOSITIONProcessor extends AqpQProcessorPost {
	
	@Override
	public boolean nodeIsWanted(AqpANTLRNode node) {
		if (node.getTokenLabel().equals("QPOSITION")) {
			return true;
		}
		return false;
	}
	
	@Override
	/*
	 * We must produce AST tree which is the same as a tree generated by ANTLR,
	 * and we must be careful to get it right, otherwise it will break the other
	 * processors
	 * 
	 *                          |
	 *                        QFUNC
	 *                          |
	 *                        /  \
	 *                <funcName>  DEFOP
	 *                             |
	 *                           COMMA  (or maybe SEMICOLON?)
	 *                             |
	 *                       / - - | - - \  ....  (nodes)
	 *                      /      |      \
	 *                         MODIFIER
	 *                             |
	 *                         TMODIFIER
	 *                             |
	 *                           FIELD
	 *                             |
	 *                           QNORMAL
	 *                             |
	 *                           <value>          
	 */
	public QueryNode createQNode(AqpANTLRNode node) throws QueryNodeException {
		AqpANTLRNode subChild = (AqpANTLRNode) node.getChildren().get(0);
		String input = subChild.getTokenInput();
		
		if (input.equals("^~")) {
			throw new QueryNodeException(new MessageImpl(
	                QueryParserMessages.INVALID_SYNTAX,
	                "^~ is very concise and therefore cool, but I am afraid you must tell me more. Try something like: word^0.5~"));
		}
		
		Integer start = -1;
		Integer end = 1;
		
		if (input.startsWith("^")) {
			input = input.substring(1, input.length());
			start = 1;
		}
		
		if (input.endsWith("$")) {
			input = input.substring(0, input.length()-1);
			end = -1;
		}
		
		input = input.trim(); // it may contain trailing spaces, especially when: ^name, j, k   AND somethi...
		
		if ((input.contains(",") || input.contains(" ")) && !input.startsWith("\"")) { 
			input = "\"" + input + "\"";
		}
		
		AqpCommonTree tree = node.getTree();
		
		// content under QFUNC
		AqpANTLRNode semicolonNode = new AqpANTLRNode(tree);
		semicolonNode.setTokenName("OPERATOR");
		semicolonNode.setTokenLabel("COMMA");
		
		// 1. value (field name)
		AqpANTLRNode field = new AqpANTLRNode(tree);
		field.setTokenLabel("TOKEN");
		field.setTokenName("TOKEN");
		field.setTokenInput(getFieldName(node, "author")); // the first argument should be the field name
		semicolonNode.add(getChain(field));
		
		// 2nd value (user input)
		AqpANTLRNode author = new AqpANTLRNode(tree);
		author.setTokenName(input.startsWith("\"") ? "PHRASE" : "TOKEN");
		author.setTokenLabel(input.startsWith("\"") ? "PHRASE" : "TOKEN");
		author.setTokenInput(input);
		semicolonNode.add(getChain(author));
		
		// 3rd value (starting position)
		AqpANTLRNode startNode = new AqpANTLRNode(tree);
		startNode.setTokenName("TOKEN");
		startNode.setTokenLabel("TOKEN");
		startNode.setTokenInput(String.valueOf(start));
		semicolonNode.add(getChain(startNode));
		
		// 4th value (ending position)
		AqpANTLRNode endNode = new AqpANTLRNode(tree);
		endNode.setTokenName("TOKEN");
		endNode.setTokenLabel("TOKEN");
		endNode.setTokenInput(String.valueOf(end));
		semicolonNode.add(getChain(endNode));
		
		
		// now we build QFUNC tree and attach semicolon to it
		AqpANTLRNode funcNode = new AqpANTLRNode(tree);
		funcNode.setTokenName("QFUNC");
		funcNode.setTokenLabel("QFUNC");
		
		AqpANTLRNode funcValue = new AqpANTLRNode(tree);
		funcValue.setTokenName("TOKEN");
		funcValue.setTokenName("TOKEN");
		funcValue.setTokenInput("pos(");
		
		funcNode.add(funcValue);
		funcNode.add(semicolonNode);
		
		
		// finally, generate warning
		AqpFeedback feedback = getFeedbackAttr();
		feedback.sendEvent(feedback.createEvent(AqpFeedback.TYPE.SYNTAX_SUGGESTION, 
				this.getClass(), 
				node, 
				"This is an obsolete syntax! One day you may wake up and discover strange errors..." +
				"Please use: {{{pos(author," + author + "," + start + "," + end + "}}}"));
		
		return funcNode;
	}
	
	// tries to discover the field (if present, otherwise returns the default)
	private String getFieldName(AqpANTLRNode node, String defaultField) {
		String fieldName = defaultField;
		
		if (node.getParent().getChildren().size() != 2) {
			return fieldName;
		}
		
		QueryNode possibleField = node.getParent().getChildren().get(0);
		if (possibleField instanceof AqpANTLRNode) {
			String testValue = ((AqpANTLRNode) possibleField).getTokenInput();
			if (testValue != null) {
				fieldName = testValue;
			}
		}
		return fieldName;
	}

	protected AqpANTLRNode getChain(AqpANTLRNode finalNode) {
		
		AqpCommonTree tree = finalNode.getTree();
		
		AqpANTLRNode modifierNode = new AqpANTLRNode(tree);
		modifierNode.setTokenName("MODIFIER");
		modifierNode.setTokenLabel("MODIFIER");
		
		AqpANTLRNode tmodifierNode = new AqpANTLRNode(tree);
		tmodifierNode.setTokenName("TMODIFIER");
		tmodifierNode.setTokenLabel("TMODIFIER");
		
		AqpANTLRNode fieldNode = new AqpANTLRNode(tree);
		fieldNode.setTokenName("FIELD");
		fieldNode.setTokenLabel("FIELD");
		
		AqpANTLRNode qNode = new AqpANTLRNode(tree);
		qNode.setTokenName("QNORMAL");
		qNode.setTokenLabel("QNORMAL");
		
		
		modifierNode.add(tmodifierNode);
		tmodifierNode.add(fieldNode);
		fieldNode.add(qNode);
		qNode.add(finalNode);
		
		return modifierNode;
	}
	
}
