/*
 * ------------------------------------------------------------------------
 *
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
 * ---------------------------------------------------------------------
 */
package org.knime.bigdata.spark.core.databricks.jobapi;

import java.util.Map;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Spark Job to prepare the context.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class DatabricksPrepareContextJobOutput extends JobOutput {

    private static final String KEY_SPARK_WEB_UI = "sparkWebUI";

    private static final String KEY_SPARK_CONF = "sparkConf";

    private static final String KEY_TESTFILE_NAME = "testfileName";

    private static final String KEY_ADAPTIVE_EXECUTION_ENABLED = "adaptiveExecutionEnabled";

    /**
     * Empty constructor for (de)serialization.
     */
    public DatabricksPrepareContextJobOutput() {
    }

    /**
     * Spark 2.x and below constructor without adaptive execution support.
     *
     * @param sparkWebUI URL of spark web UI
     * @param sparkConf map with spark configuration
     * @param testfileName name of test file in staging area
     */
    public DatabricksPrepareContextJobOutput(final String sparkWebUI, final Map<String, String> sparkConf, final String testfileName) {
        set(KEY_SPARK_WEB_UI, sparkWebUI);
        set(KEY_SPARK_CONF, sparkConf);
        set(KEY_TESTFILE_NAME, testfileName);
        set(KEY_ADAPTIVE_EXECUTION_ENABLED, false);
    }

    /**
     * Spark 3.x and above constructor with adaptive execution support.
     *
     * @param sparkWebUI URL of spark web UI
     * @param sparkConf map with spark configuration
     * @param testfileName name of test file in staging area
     * @param adaptiveExecutionEnabled {@code true} if adaptive query execution is enabled
     */
    public DatabricksPrepareContextJobOutput(final String sparkWebUI, final Map<String, String> sparkConf,
        final String testfileName, final boolean adaptiveExecutionEnabled) {

        set(KEY_SPARK_WEB_UI, sparkWebUI);
        set(KEY_SPARK_CONF, sparkConf);
        set(KEY_TESTFILE_NAME, testfileName);
        set(KEY_ADAPTIVE_EXECUTION_ENABLED, adaptiveExecutionEnabled);
    }

    /**
     * @return URL of spark web UI
     */
    public String getSparkWebUI() {
        return get(KEY_SPARK_WEB_UI);
    }

    /**
     * @return map with spark configuration
     */
    public Map<String, String> getSparkConf() {
        return get(KEY_SPARK_CONF);
    }

    /**
     * @return name of test file in staging area
     */
    public String getTestfileName() {
        return get(KEY_TESTFILE_NAME);
    }

    /**
     * @return {@code true} if adaptive query execution is enabled
     */
    public boolean adaptiveExecutionEnabled() {
        return get(KEY_ADAPTIVE_EXECUTION_ENABLED);
    }
}
