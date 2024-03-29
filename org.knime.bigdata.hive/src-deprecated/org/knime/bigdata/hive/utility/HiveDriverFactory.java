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
 *   Created on 20.05.2016 by koetter
 */
package org.knime.bigdata.hive.utility;

import java.io.IOException;

import org.apache.hive.jdbc.HiveDriver;
import org.knime.core.node.port.database.connection.DefaultDBDriverFactory;
import org.osgi.framework.FrameworkUtil;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@Deprecated
public class HiveDriverFactory extends DefaultDBDriverFactory {
    /**Hive driver class name.*/
    public static final String DRIVER = HiveDriver.class.getName();

    /**Constructor.
     * @throws IOException */
    public HiveDriverFactory() throws IOException {
        super(DRIVER, FrameworkUtil.getBundle(HiveDriver.class));
    }
}
