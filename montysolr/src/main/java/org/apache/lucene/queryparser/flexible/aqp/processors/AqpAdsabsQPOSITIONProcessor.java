package org.apache.lucene.queryparser.flexible.aqp.processors;

import org.apache.lucene.queryparser.flexible.aqp.builders.AqpFunctionQueryBuilder;
import org.apache.lucene.queryparser.flexible.aqp.config.AqpAdsabsQueryConfigHandler;
import org.apache.lucene.queryparser.flexible.aqp.config.AqpFeedback;
import org.apache.lucene.queryparser.flexible.aqp.nodes.AqpANTLRNode;
import org.apache.lucene.queryparser.flexible.aqp.nodes.AqpFunctionQueryNode;
import org.apache.lucene.queryparser.flexible.aqp.util.AqpCommonTree;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.core.messages.QueryParserMessages;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;

import java.util.ArrayList;
import java.util.List;

public class AqpAdsabsQPOSITIONProcessor extends AqpQProcessorPost {

    @Override
    public boolean nodeIsWanted(AqpANTLRNode node) {
        return node.getTokenLabel().equals("QPOSITION");
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

        input = input.trim();

        if (input.equals("^~") || input.equals("^") || input.equals("")) {
            throw new QueryNodeException(new MessageImpl(
                    QueryParserMessages.INVALID_SYNTAX,
                    "^~ is very concise and therefore cool, but I am afraid you must tell me more. Try something like: word^0.5~"));
        }

        Integer start = -1;
        Integer end = 1;

        if (input.startsWith("^")) {
            input = input.substring(1);
            start = 1;
        }

        if (input.endsWith("$")) {
            throw new QueryNodeException(new MessageImpl(
                    "Invalid argument: $",
                    "We do not support the syntax for finding the last author, you can use range pos(author:\"xxx\", 1, 5)"));

            //input = input.substring(0, input.length()-1);
            //end = -1;
        }

        input = input.trim(); // it may contain trailing spaces, especially when: ^name, j, k   AND somethi...

        if (input.charAt(0) != '"') {
            input = "\"" + input + "\"";
        }

        // finally, generate warning
        AqpFeedback feedback = getFeedbackAttr();
        feedback.sendEvent(feedback.createEvent(AqpFeedback.TYPE.SYNTAX_SUGGESTION,
                this.getClass(),
                node,
                "This is an obsolete syntax! One day you may wake up and discover strange errors..." +
                        "Please use: {{{pos(author, " + start + ", " + end + ", \"" + input + "\"}}}"));

        QueryConfigHandler config = getQueryConfigHandler();

        if (!config.has(AqpAdsabsQueryConfigHandler.ConfigurationKeys.FUNCTION_QUERY_BUILDER_CONFIG)) {
            throw new QueryNodeException(new MessageImpl(
                    "Invalid configuration",
                    "Missing FunctionQueryBuilder provider"));
        }

        AqpFunctionQueryBuilder builder = config.get(AqpAdsabsQueryConfigHandler.ConfigurationKeys.FUNCTION_QUERY_BUILDER_CONFIG)
                .getBuilder("pos", node, config);

        if (builder == null) {
            throw new QueryNodeException(new MessageImpl(
                    "Unknown function pos()"));
        }


        String fieldName = getFieldName(node, "author");
        List<OriginalInput> values = new ArrayList<OriginalInput>();

        // was it old syntax =author:"^...." ?
        String prefix = "";
        QueryNode p = node;
        int i = 3;
        while (i != 0 && p != null) {
            p = p.getParent();
            i -= 1;
        }
        if (p != null && p.getChildren().get(0) instanceof AqpANTLRNode &&
                ((AqpANTLRNode) p.getChildren().get(0)).getTokenName().equals("EQUAL")) {
            prefix = "=";
        }
        values.add(new OriginalInput(prefix + fieldName + ":" + input, subChild.getInputTokenStart(), subChild.getInputTokenEnd()));
        values.add(new OriginalInput(String.valueOf(start), -1, -1));
        values.add(new OriginalInput(String.valueOf(end), -1, -1));


        return new AqpFunctionQueryNode("pos", builder, values);

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
