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
 *   Created on Nov 16, 2017 by Sascha Wolke, KNIME GmbH
 */
package com.knime.bigdata.spark.node.sql_function;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.JComponent;
import javax.swing.JEditorPane;
import javax.swing.JScrollPane;
import javax.swing.ScrollPaneConstants;

import org.apache.commons.lang3.StringEscapeUtils;
import org.knime.core.data.DataType;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.database.aggregation.AggregationFunctionProvider;

import com.knime.bigdata.spark.core.sql_function.SparkSQLFunctionProviderRegistry;
import com.knime.bigdata.spark.core.version.SparkVersion;

/**
 * Brings {@link SparkSQLFunctionProviderRegistry} and {@link SparkSQLFunctionDialogProviderRegistry} together.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class SparkSQLFunctionCombinationProvider implements AggregationFunctionProvider<SparkSQLAggregationFunction> {

    private final HashMap<String, String> m_sparkFactories;

    private final HashMap<String, SparkSQLFunctionDialogFactory<SparkSQLAggregationFunction>> m_aggFuncFactories;

    /**
     * Find {@link SparkSQLFunctionProviderRegistry} and {@link SparkSQLFunctionDialogProviderRegistry} of given Spark
     * version.
     *
     * @param sparkVersion desired Spark Version
     * @throws InvalidSettingsException if spark version is unsupported or essential functions are missing
     */
    public SparkSQLFunctionCombinationProvider(final SparkVersion sparkVersion) throws InvalidSettingsException {
        final SparkSQLFunctionDialogProviderRegistry functionDialogRegistry = SparkSQLFunctionDialogProviderRegistry.getInstance();
        final Map<String, String> functions =
                SparkSQLFunctionProviderRegistry.getInstance().getSupportedFunctions(sparkVersion);
        m_sparkFactories = new HashMap<>();

        // add all supported aggregation functions
        m_aggFuncFactories = new HashMap<>();
        for(SparkSQLFunctionDialogFactory<SparkSQLAggregationFunction> dialogFact : functionDialogRegistry.getAggregationFunctionFactories(sparkVersion)) {
            final String sparkFactory = functions.get(dialogFact.getId());
            if (sparkFactory != null) {
                m_sparkFactories.put(dialogFact.getId(), sparkFactory);
                m_aggFuncFactories.put(dialogFact.getId(), dialogFact);
            }
        }

        // no provider found
        if (functions.isEmpty()) {
            throw new InvalidSettingsException("No Spark SQL Function provider with Spark Version " + sparkVersion
                + " found. Unsupported Spark Version.");
        }

        // Special case column function
        if (!functions.containsKey("column")) {
            throw new InvalidSettingsException("No provider with required column function found!");
        } else {
            m_sparkFactories.put("column", functions.get("column"));
        }

        // Special case count function
        if (functions.containsKey("count")) {
            m_sparkFactories.put("count", functions.get("count"));
        }

        // Special case window function
        if (functions.containsKey("window")) {
            m_sparkFactories.put("window", functions.get("window"));
        }
    }

    /**
     * @param id Unique function name
     * @return Class name of Spark Side Function Factory
     */
    public String getSparkSideFactory(final String id) {
        return m_sparkFactories.get(id);
    }

    /** @return true if a spark provider with count function was found */
    public boolean hasCountFunction() {
        return m_sparkFactories.containsKey("count");
    }

    /** @return true if a spark provider with window function was found */
    public boolean hasWindowFunction() {
        return m_sparkFactories.containsKey("window");
    }

    @Override
    public List<SparkSQLAggregationFunction> getCompatibleFunctions(final DataType type, final boolean sorted) {
        final List<SparkSQLAggregationFunction> compatible = new ArrayList<>(m_aggFuncFactories.size());
        for (SparkSQLFunctionDialogFactory<SparkSQLAggregationFunction> factory : m_aggFuncFactories.values()) {
            final SparkSQLAggregationFunction function = factory.getInstance();
            if (type == null || function.isCompatible(type)) {
                compatible.add(function);
            }
        }
        if (sorted) {
            Collections.sort(compatible, SparkSQLFunctionIdComperator.ASC);
        }
        return compatible;
    }

    @Override
    public SparkSQLAggregationFunction getFunction(final String id) {
        return m_aggFuncFactories.get(id).getInstance();
    }

    @Override
    public SparkSQLAggregationFunction getDefaultFunction(final DataType type) {
        for (SparkSQLFunctionDialogFactory<SparkSQLAggregationFunction> factory : m_aggFuncFactories.values()) {
            final SparkSQLAggregationFunction function = factory.getInstance();
            if (function.isCompatible(type)) {
                return function;
            }
        }

        return null;
    }

    @Override
    public List<SparkSQLAggregationFunction> getFunctions(final boolean sorted) {
        return getCompatibleFunctions(null, sorted);
    }

    @Override
    public JComponent getDescriptionPane() {
        final StringBuilder buf = getHTMLDescription();
        final JEditorPane editorPane = new JEditorPane("text/html", buf.toString());
        editorPane.setEditable(false);
        final JScrollPane scrollPane = new JScrollPane(editorPane, ScrollPaneConstants.VERTICAL_SCROLLBAR_AS_NEEDED,
                ScrollPaneConstants.HORIZONTAL_SCROLLBAR_AS_NEEDED);
        return scrollPane;
    }

    /**
     * @return the HTML String that lists all available aggregation methods
     * and their description as a definition list.
     */
    private StringBuilder getHTMLDescription() {
        final List<SparkSQLAggregationFunction> functions = getFunctions(true);
        final StringBuilder buf = new StringBuilder();
        buf.append("<dl>");
        buf.append("\n");
        for (final SparkSQLAggregationFunction function : functions) {
            buf.append("<dt><b>");
            buf.append(function.getLabel());
            buf.append("</b></dt>");
            buf.append("\n");
            buf.append("<dd>");
            buf.append(StringEscapeUtils.escapeHtml4(function.getDescription()));
            buf.append("</dd>");
            buf.append("\n");
        }
        //close the last definition list
        buf.append("</dl>");
        return buf;
    }
}
