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
 *   Created on May 28, 2019 by bjoern
 */
package org.knime.bigdata.spark.core.util;

import java.io.IOException;
import java.nio.file.Path;

import org.knime.core.util.FileUtil;

/**
 * Implementation of {@link TempFileSupplier} that uses the KNIME {@link FileUtil}.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class KNIMETempFileSupplier implements TempFileSupplier {

    private static KNIMETempFileSupplier SINGLETON_INSTANCE;

    /**
     * @return a {@link TempFileSupplier} instance.
     */
    public static synchronized TempFileSupplier getInstance() {
        if (SINGLETON_INSTANCE == null) {
            SINGLETON_INSTANCE = new KNIMETempFileSupplier();
        }

        return SINGLETON_INSTANCE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Path newTempFile() throws IOException {
        return FileUtil.createTempFile("spark", null, true).toPath();
    }

}
