package org.knime.bigdata.impala;
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
 *   Created on 30.10.2017 by bjoern
 */

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.eclipse.core.runtime.Plugin;
import org.osgi.framework.BundleContext;

/**
 * Bundle activator for the Impala plugin.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class ImpalaPlugin extends Plugin {

    /**
     * This method is called upon plug-in activation.
     *
     * @param context The bundle context.
     * @throws Exception If cause by super class.
     */
    @Override
    public void start(final BundleContext context) throws Exception {
        super.start(context);

        // don't set this to DEBUG (other than for testing reasons), because the Hive driver will log
        // the received data with DEBUG priority, resulting tons of logspam.
        Logger.getLogger("org.apache.hive").setLevel(Level.INFO);

        // we are setting zookeeper to debug because it allows to debug zookeeper service discovery a little better
        // (and it doesn't produce a lot of logspam).
        Logger.getLogger("org.apache.zookeeper").setLevel(Level.DEBUG);
    }
}
