/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on 27.08.2018 by Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
package org.knime.bigdata.spark.node.scripting.python.util;

import javax.swing.text.BadLocationException;

import org.fife.ui.rsyntaxtextarea.SyntaxConstants;
import org.knime.base.node.jsnippet.guarded.GuardedDocument;
import org.knime.base.node.jsnippet.guarded.GuardedSection;
import org.knime.core.node.workflow.FlowVariable;

/**
 * PySpark Document with guarded sections
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class PySparkDocument extends GuardedDocument {
    private static final long serialVersionUID = -9007276381124805214L;

    /** The name of the guarded section for imports. */
    public static final String GUARDED_IMPORTS = "imports";

    /** The name of the guarded section for the start of the body. */
    public static final String GUARDED_FLOW_VARIABLES = "flowVariables";

    /** The name of the guarded section for the start of the body. */
    public static final String GUARDED_BODY_START = "bodyStart";

    /** The name of the guarded section for the end of the body. */
    public static final String GUARDED_BODY_END = "bodyEnd";

    /**
     * Creates a PySpark Document with guarded sections
     */
    public PySparkDocument() {
        super(SyntaxConstants.SYNTAX_STYLE_PYTHON);
        try {
            addGuardedSection(GUARDED_IMPORTS, getLength());
            insertString(getLength(), "#Custom imports \n", null);
            final GuardedSection flowVariables = addGuardedSection(GUARDED_FLOW_VARIABLES, getLength());
            flowVariables.setText("#Flowvariables \n");

            insertString(getLength(), "\n #Custom globals \n", null);
            final GuardedSection bodyStart = addGuardedSection(GUARDED_BODY_START, getLength());
            bodyStart.setText("# expression start\n");

            insertString(getLength(), "\n", null);

            final GuardedSection bodyEnd = addGuardedFooterSection(GUARDED_BODY_END, getLength());
            bodyEnd.setText("# expression end\n");
        } catch (BadLocationException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /**
     * Overwrites the flow variables section with the given list of flow variables
     * @param variables the list of flow variables to set
     */
    public void writeFlowVariables(final FlowVariable[] variables) {
        StringBuilder sb = new StringBuilder();
        sb.append("#Flowariables \n");
        for(FlowVariable flowVariable : variables) {
        switch (flowVariable.getType()) {
                case INTEGER:
                    sb.append("v_" + flowVariable.getName() + " = " + flowVariable.getIntValue() + "\n");
                    break;
                case DOUBLE:
                    sb.append("v_" + flowVariable.getName() + " = " + flowVariable.getDoubleValue() + "\n");
                    break;
                case STRING:
                    sb.append("v_" + flowVariable.getName() + " = \"" + flowVariable.getStringValue() + "\"\n");
                    break;
                default:
                    sb.append("v_" + flowVariable.getName() + " = \"" + flowVariable.getValueAsString() + "\"\n");
                    break;
            }
        }
        sb.append("\n");
        try {
        GuardedSection guardedFLow = getGuardedSection(PySparkDocument.GUARDED_FLOW_VARIABLES);
        guardedFLow.setText(sb.toString());
        }catch(BadLocationException ex) {
            //Should never happen
            throw new IllegalStateException(ex);
        }
    }

}
