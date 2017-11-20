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
 *   Created on May 11, 2016 by bjoern
 */
package com.knime.bigdata.spark.node.scripting.java.util;

import java.util.ArrayList;
import java.util.List;

import javax.swing.text.Document;

import org.knime.base.node.jsnippet.util.FlowVariableRepository;
import org.knime.base.node.jsnippet.util.ValidationReport;
import org.knime.base.node.jsnippet.util.field.InVar;
import org.knime.core.node.workflow.FlowVariable;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class SparkJSnippetValidator {

    /**
     * Validate settings which is typically called in the configure method of a node. This method validates that input
     * member fields correspond to flow variables and that the snippet code compiles.
     *
     * @param flowVariableRepository the flow variables at the inport
     * @param snippet
     * @return the validation results
     */
    public static ValidationReport validate(final FlowVariableRepository flowVariableRepository,
        final SparkJSnippet snippet) {

        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();

        // check whether input variables match flow variables
        for (InVar field : snippet.getSettings().getJavaSnippetFields().getInVarFields()) {
            FlowVariable var = flowVariableRepository.getFlowVariable(field.getKnimeName());
            if (var != null) {
                if (!var.getType().equals(field.getFlowVarType())) {
                    errors.add("The type of the flow variable \"" + field.getKnimeName() + "\" has changed.");
                }
            } else {
                errors.add("The flow variable \"" + field.getKnimeName() + "\" is not found in the input.");
            }
        }

        try {
            // validate that snippet compiles
            final Document doc = snippet.getDocument();
            new SourceCompiler(snippet.getSnippetClassName(), doc.getText(0, doc.getLength()),
                snippet.getClassPath());
        } catch (Exception e) {
            errors.add("Compilation error(s): " + e.getMessage());
        }
        return new ValidationReport(errors.toArray(new String[errors.size()]),
            warnings.toArray(new String[warnings.size()]));
    }
}
