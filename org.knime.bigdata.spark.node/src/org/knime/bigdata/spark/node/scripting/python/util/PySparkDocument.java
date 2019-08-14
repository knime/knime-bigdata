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
import org.knime.core.node.workflow.FlowVariable;
import org.knime.rsyntaxtextarea.guarded.GuardedDocument;
import org.knime.rsyntaxtextarea.guarded.GuardedSection;

/**
 * PySpark Document with guarded sections
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class PySparkDocument extends GuardedDocument {

    private static final long serialVersionUID = -9007276381124805214L;

    /** The start string for the flowvariable access string. */
    public static final String FLOW_VARIABLE_START = "flow_variables['v_";

    /** The end string for the flowvariable access string. */
    public static final String FLOW_VARIABLE_END = "']";

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
            insertString(getLength(), "# Your custom imports:\n\n", null);
            final GuardedSection flowVariables = addGuardedSection(GUARDED_FLOW_VARIABLES, getLength());
            // For some reason the document implementation cannot handle replacement of only empty lines so we put some
            // extra line here to fix BD-901
            flowVariables.setText("# Flowvariables\n # This section is reserved for flowvariables.\n");

            insertString(getLength(), "# Your custom global variables:\n\n", null);
            final GuardedSection bodyStart = addGuardedSection(GUARDED_BODY_START, getLength());
            bodyStart.setText("# expression start\n");

            insertString(getLength(), "\n", null);

            final GuardedSection bodyEnd = addGuardedFooterSection(GUARDED_BODY_END, getLength());
            bodyEnd.setText("# expression end\n");
        } catch (final BadLocationException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /**
     * Overwrites the flow variables section with the given list of flow variables
     *
     * @param variables the list of flow variables to set
     */
    public void writeFlowVariables(final FlowVariable[] variables) {
        final StringBuilder sb = new StringBuilder();
        sb.append("# Flowvariables\n\n");
        sb.append("flow_variables = {} \n");
        for (final FlowVariable flowVariable : variables) {
            final String escapedName = VariableNameUtil.escapeName(flowVariable.getName());
            switch (flowVariable.getType()) {
                case INTEGER:
                    sb.append(FLOW_VARIABLE_START + escapedName + FLOW_VARIABLE_END + " = " + flowVariable.getIntValue()
                        + "\n");
                    break;
                case DOUBLE:
                    sb.append(FLOW_VARIABLE_START + escapedName + FLOW_VARIABLE_END + " = "
                        + flowVariable.getDoubleValue() + "\n");
                    break;
                case STRING:
                    sb.append(FLOW_VARIABLE_START + escapedName + FLOW_VARIABLE_END + " = \""
                        + flowVariable.getStringValue() + "\"\n");
                    break;
                default:
                    sb.append(FLOW_VARIABLE_START + escapedName + FLOW_VARIABLE_END + " = \""
                        + flowVariable.getValueAsString() + "\"\n");
                    break;
            }
        }
        sb.append("\n");
        try {
            final GuardedSection guardedFLow = getGuardedSection(PySparkDocument.GUARDED_FLOW_VARIABLES);
            guardedFLow.setText(sb.toString());
        } catch (final BadLocationException ex) {
            //Should never happen
            throw new IllegalStateException(ex);
        }
    }

}
