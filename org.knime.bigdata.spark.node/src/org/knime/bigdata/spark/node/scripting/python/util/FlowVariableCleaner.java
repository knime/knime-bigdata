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
 *   Created on 08.08.2019 by Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */

package org.knime.bigdata.spark.node.scripting.python.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.knime.bigdata.spark.node.scripting.python.PySparkNodeConfig;
import org.knime.core.node.workflow.FlowVariable;

/**
 * This class fixes issues with FlowVariables handling in PySpark Scripts see BD-942. In case of deprecation of the
 * PySpark nodes this class should no longer be necessary
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class FlowVariableCleaner {

    private FlowVariableCleaner() {
        //Utility class
    }

    /**
     * Cleans the list of old FlowVariables based on the currently available variables. If a FlowVariable matching the
     * escaped name is found in the list the FlowVariable is added to the list of used FlowVariables.
     *
     * In case of multiple matches the first found variable is used and a warning is appended to the result string.
     *
     * @param config the current {@link PySparkNodeConfig} that has to be adapted
     * @param currentVariables the currently available FlowVariables
     * @return String containing possible warnings during FlowVariable cleaning.
     */
    public static String cleanFlowVariables(final PySparkNodeConfig config, final List<FlowVariable> currentVariables) {

        final List<String> usedFlowVariables = new ArrayList<>();
        final StringBuilder warning = new StringBuilder();

        final List<String> oldVariables = new ArrayList<>(config.getOldEscapedFlowVariablesNames());

        for (final String escapedName : oldVariables) {

            final List<String> possibleCurrentFlowVariables = getPossibleFlowVariables(currentVariables, escapedName);

            if (!possibleCurrentFlowVariables.isEmpty()) {
                final String usedVariableName = possibleCurrentFlowVariables.get(0);
                config.getOldEscapedFlowVariablesNames().remove(escapedName);
                if (possibleCurrentFlowVariables.size() > 1) {
                    appendAmbiguityWarning(warning, escapedName, possibleCurrentFlowVariables, usedVariableName);
                }
                usedFlowVariables.add(usedVariableName);
            } else {
                appendNotFoundWarning(warning, escapedName);
            }
        }

        if (!usedFlowVariables.isEmpty()) {
            updateVariableLists(config, usedFlowVariables);
        }

        return warning.toString();

    }

    private static void updateVariableLists(final PySparkNodeConfig config, final List<String> usedFlowVariables) {
        final String[] allUsedFlowVariables = appendExistingUsedFlowVariables(config, usedFlowVariables);
        config.setUsedFlowVariablesNames(allUsedFlowVariables);
    }

    private static List<String> getPossibleFlowVariables(final List<FlowVariable> currentVariables,
        final String escapedName) {
        final List<String> possibleNewValues = new ArrayList<>();
        for (final FlowVariable flowV : currentVariables) {
            final String escapedCurrentName = VariableNameUtil.escapeName(flowV.getName());
            if (escapedName.equals(escapedCurrentName)) {
                possibleNewValues.add(flowV.getName());
            }
        }
        return possibleNewValues;
    }

    private static void appendNotFoundWarning(final StringBuilder warning, final String escapedName) {
        if (warning.length() != 0) {
            warning.append(System.lineSeparator());
        }
        warning.append(String.format("No flow variable found for escaped Name %s", escapedName));
    }

    private static void appendAmbiguityWarning(final StringBuilder warning, final String escapedName,
        final List<String> possibleNewValues, final String usedVariableName) {

        if (warning.length() != 0) {
            warning.append(System.lineSeparator());
        }
        warning.append(String.format(
            "Escaped variable string %s leads to ambiguous flow variable match. Possible values are: [", escapedName));
        for (final String value : possibleNewValues) {
            warning.append(String.format("%s, ", value));
        }
        warning.deleteCharAt(warning.length() - 1);
        warning.deleteCharAt(warning.length() - 1);
        warning.append(String.format("] Using: %s", usedVariableName));

    }

    private static String[] appendExistingUsedFlowVariables(final PySparkNodeConfig config,
        final List<String> usedFlowVariables) {
        usedFlowVariables.addAll(Arrays.asList(config.getUsedFlowVariablesNames()));
        return usedFlowVariables.toArray(new String[usedFlowVariables.size()]);
    }

}
