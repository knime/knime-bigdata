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
 *   Created on Jun 27, 2018 by bjoern
 */
package org.knime.bigdata.filehandling.util;

import java.io.IOException;

import org.knime.bigdata.commons.hadoop.HadoopInitializer;

/**
 * TO BE REMOVED SOON, replaced by org.knime.bigdata.commons.hadoop.HadoopInitializer
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class HadoopWinutilsInitializer {

    /**
     * TO BE REMOVED SOON, replaced by org.knime.bigdata.commons.hadoop.HadoopInitializer
     *
     * @throws IOException
     */
    public synchronized static void ensureInitialized() throws IOException {
        HadoopInitializer.ensureInitialized();
    }

}
