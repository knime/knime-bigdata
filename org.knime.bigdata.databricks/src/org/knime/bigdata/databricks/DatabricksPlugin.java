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
package org.knime.bigdata.databricks;

import java.io.File;
import java.net.URL;

import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.Path;
import org.eclipse.ui.plugin.AbstractUIPlugin;
import org.knime.bigdata.spark.core.version.CompatibilityChecker;
import org.knime.bigdata.spark.core.version.FixedVersionCompatibilityChecker;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.node.KNIMEConstants;
import org.osgi.framework.BundleContext;

/**
 * OSGI Bundle Activator for the Databricks connector plugin.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class DatabricksPlugin extends AbstractUIPlugin {

    /**
     * Compatibility checker for the Spark versions currently supported by the Databricks connector plugin.
     */
    public static final CompatibilityChecker DATABRICKS_SPARK_VERSION_CHECKER = new FixedVersionCompatibilityChecker( //
        SparkVersion.V_2_4, //
        SparkVersion.V_3_0, SparkVersion.V_3_1, SparkVersion.V_3_2, SparkVersion.V_3_3, SparkVersion.V_3_4,
        SparkVersion.V_3_5);

    /**
     * Holds the singleton instance of the Databricks connector plugin, once it has been created by the OSGI framework.
     */
    private static volatile DatabricksPlugin plugin;

    private String m_pluginRootPath;

    /**
     * The constructor.
     */
    public DatabricksPlugin() {
        synchronized (DatabricksPlugin.class) {
            plugin = this;
        }
    }

    /**
     * @return HTTP User-Agent name
     */
    public static String getUserAgent() {
        return "KNIME/" + KNIMEConstants.VERSION;
    }

    /**
     * This method is called upon plug-in activation.
     *
     * @param context The bundle context.
     * @throws Exception If cause by super class.
     */
    @Override
    public void start(final BundleContext context) throws Exception {
        super.start(context);
        final URL pluginURL = FileLocator.resolve(FileLocator.find(plugin.getBundle(), new Path(""), null));
        final File tmpFile = new File(pluginURL.getPath());
        m_pluginRootPath = tmpFile.getAbsolutePath();
    }

    /**
     * This method is called when the plug-in is stopped.
     *
     * @param context The bundle context.
     * @throws Exception If cause by super class.
     */
    @Override
    public void stop(final BundleContext context) throws Exception {
        plugin = null;
        super.stop(context);
    }

    /**
     * Returns the singleton instance of this plugin, once it has been created by the OSGI framework.
     *
     * @return The singleton instance of this plugin.
     */
    public static DatabricksPlugin getDefault() {
        return plugin;
    }

    /**
     * @return the absolute root path of this plugin
     */
    public String getPluginRootPath() {
        return m_pluginRootPath;
    }
}
