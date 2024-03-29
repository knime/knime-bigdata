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
package org.knime.bigdata.phoenix.utility;

import org.knime.core.node.port.database.connection.DefaultDBDriverFactory;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
class PhoenixDriverFactory extends DefaultDBDriverFactory {
    /**The driver class name.*/
    static final String DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    /**Constructor.*/
    PhoenixDriverFactory() {
        super(DRIVER, "lib/phoenix-4.4.0-HBase-0.98-client.jar",
            "lib/slf4j-api-1.7.5.jar",
            "lib/slf4j-log4j12.jar",
            "conf");
    }
}
