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
package org.knime.bigdata.hadoop.filehandling;

import javax.ws.rs.ext.RuntimeDelegate;

import org.eclipse.ui.plugin.AbstractUIPlugin;
import org.knime.bigdata.commons.config.CommonConfigContainer;
import org.knime.bigdata.commons.hadoop.HadoopInitializer;
import org.knime.core.node.NodeLogger;
import org.osgi.framework.BundleContext;

import com.sun.ws.rs.ext.RuntimeDelegateImpl;

/**
 * Plugin activator for the Big Data Hadoop filehandling plugin.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class HadoopFilehandlingPlugin extends AbstractUIPlugin {

    private static final NodeLogger LOG = NodeLogger.getLogger(HadoopFilehandlingPlugin.class);

    /**
     * The constructor.
     */
    public HadoopFilehandlingPlugin() {
        CommonConfigContainer.getInstance().hdfsSupported();
    }

    /**
     * This method is called upon plug-in activation, validates that JAX-RS has a working runtime delegate and
     * initializes the Hadoop libraries.
     *
     * @param context The bundle context.
     * @throws Exception If cause by super class.
     */
    @Override
    public void start(final BundleContext context) throws Exception {
        super.start(context);

        try {
            // This method initializes the JAX-RS 1.x RuntimeDelegate, which is provided by com.sun.jersey.core.
            // Due to the way the RuntimeDelegates is used deep down in some libraries, we may not get a good error message
            // if this doesn't work for some reason (which sometimes happens on broken installations). This code here
            // at least fails with a usable error message.
            if (RuntimeDelegate.getInstance() == null) {
                throw new RuntimeException("No implementation found");
            }
        } catch (Throwable t) {
            LOG.debug("Failed to initialize the JAX-RS 1.x RuntimeDelegate. Provided error message: " + t.getMessage());
            RuntimeDelegate.setInstance(new RuntimeDelegateImpl());
        }

        HadoopInitializer.ensureInitialized();
    }
}
