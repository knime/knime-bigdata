/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ------------------------------------------------------------------------
 *
 * History
 *   Sep 25, 2014 (Patrick Winter): created
 */

package org.knime.bigdata.spark.node.scripting.python;

import javax.swing.text.BadLocationException;

import org.knime.bigdata.spark.node.scripting.python.util.DefaultPySparkHelper;
import org.knime.bigdata.spark.node.scripting.python.util.PySparkDocument;
import org.knime.bigdata.spark.node.scripting.python.util.PySparkHelper;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.python2.generic.SourceCodeConfig;
import org.knime.python2.generic.VariableNames;

/**
 * Configuration for PySpark Node
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class PySparkNodeConfig extends SourceCodeConfig {

    private VariableNames m_variableNames;

    private PySparkHelper m_helper;

    private int m_inCount;

    private int m_outCount;

    private static final String CFG_IMPORT = "imports";

    private static final String CFG_UDF = "udf";

    private static final String CFG_GLOBALS = "pyglobals";

    private static final String CFG_VARIABLES = "flowvar";

    /**
     * Creates a new configuration for the PySpark nodes
     *
     * @param inCount the number of inputs
     * @param outCount the number of outputs
     * @param pySparkHelper the helper to create the document
     */
    public PySparkNodeConfig(final int inCount, final int outCount, final PySparkHelper pySparkHelper) {
        super();
        createVariableNames(inCount, outCount);
        m_inCount = inCount;
        m_outCount = outCount;
        m_helper = pySparkHelper;
        setDoc(m_helper.createGuardedPySparkDocument(inCount, outCount));
    }

    /**
     * Creates a new configuration for the PySpark nodes
     *
     * @param inCount the number of inputs
     * @param outCount the number of outputs
     */
    public PySparkNodeConfig(final int inCount, final int outCount) {
        PySparkDocument guardedDoc = new PySparkDocument();
        createVariableNames(inCount, outCount);
        try {
            //set default UDF
            guardedDoc.replaceBetween(PySparkDocument.GUARDED_BODY_START, PySparkDocument.GUARDED_BODY_END,
                DefaultPySparkHelper.createDefaultUDFSection(inCount, outCount));
        } catch (BadLocationException ex) {
            //should never happen
            throw new IllegalStateException(ex.getMessage(), ex);
        }
        setDoc(guardedDoc);
    }

    private void createVariableNames(final int inCount, final int outCount) {
        m_variableNames = new VariableNames("flow_variables", DefaultPySparkHelper.getInputFrames(inCount), null, null,
            DefaultPySparkHelper.getInputFrames(outCount), DefaultPySparkHelper.getOutputFrames(outCount), null, null);
    }

    /**
     * Get the variable names for this node
     *
     * @return The variable names
     */
    public VariableNames getVariableNames() {
        return m_variableNames;
    }


    @Override
    public void saveTo(final NodeSettingsWO settings) {
        PySparkDocument guardedDoc = (PySparkDocument) getDoc();
        try {
            String imports =
                guardedDoc.getTextBetween(PySparkDocument.GUARDED_IMPORTS, PySparkDocument.GUARDED_FLOW_VARIABLES);
            settings.addString(CFG_IMPORT, imports);
            String globals =
                guardedDoc.getTextBetween(PySparkDocument.GUARDED_FLOW_VARIABLES, PySparkDocument.GUARDED_BODY_START);
            settings.addString(CFG_GLOBALS, globals);
            String udf =
                guardedDoc.getTextBetween(PySparkDocument.GUARDED_BODY_START, PySparkDocument.GUARDED_BODY_END);
            settings.addString(CFG_UDF, udf);

            String flowVariables = guardedDoc.getGuardedSection(PySparkDocument.GUARDED_FLOW_VARIABLES).getText();
            settings.addString(CFG_VARIABLES, flowVariables);

        } catch (BadLocationException ex) {
            // this should never happen
            throw new IllegalStateException(ex);
        }
    }

    /**
     * Load configuration from the given node settings.
     *
     * @param settings The settings to load from
     * @throws InvalidSettingsException If the settings are invalid
     */
    @Override
    public void loadFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        PySparkDocument guardedDoc = (PySparkDocument) getDoc();
        try {
            guardedDoc.replaceBetween(PySparkDocument.GUARDED_IMPORTS, PySparkDocument.GUARDED_FLOW_VARIABLES,
                settings.getString(CFG_IMPORT));
            guardedDoc.replaceBetween(PySparkDocument.GUARDED_FLOW_VARIABLES, PySparkDocument.GUARDED_BODY_START,
                settings.getString(CFG_GLOBALS));
            guardedDoc.replaceBetween(PySparkDocument.GUARDED_BODY_START, PySparkDocument.GUARDED_BODY_END,
                settings.getString(CFG_UDF));
            guardedDoc.getGuardedSection(PySparkDocument.GUARDED_FLOW_VARIABLES)
                .setText(settings.getString(CFG_VARIABLES));
        } catch (BadLocationException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /**
     * Load configuration from the given node settings (using defaults if necessary).
     *
     * @param settings The settings to load from
     */
    @Override
    public void loadFromInDialog(final NodeSettingsRO settings) {
        PySparkDocument guardedDoc = (PySparkDocument) getDoc();
        try {
            guardedDoc.replaceBetween(PySparkDocument.GUARDED_IMPORTS, PySparkDocument.GUARDED_FLOW_VARIABLES,
                settings.getString(CFG_IMPORT, "\n#Custom imports\n"));
            guardedDoc.replaceBetween(PySparkDocument.GUARDED_FLOW_VARIABLES, PySparkDocument.GUARDED_BODY_START,
                settings.getString(CFG_GLOBALS, "\n#Custom globals\n"));
            guardedDoc.replaceBetween(PySparkDocument.GUARDED_BODY_START, PySparkDocument.GUARDED_BODY_END,
                settings.getString(CFG_UDF, DefaultPySparkHelper.createDefaultUDFSection(m_inCount, m_outCount)));
            guardedDoc.getGuardedSection(PySparkDocument.GUARDED_FLOW_VARIABLES)
                .setText(settings.getString(CFG_VARIABLES, "#Flowvariables\n"));
        } catch (BadLocationException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
