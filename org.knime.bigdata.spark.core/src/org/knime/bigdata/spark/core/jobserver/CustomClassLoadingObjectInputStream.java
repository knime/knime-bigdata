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
 *   Created on May 3, 2016 by bjoern
 */
package com.knime.bigdata.spark.core.jobserver;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

import com.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
@SparkClass
public class CustomClassLoadingObjectInputStream extends ObjectInputStream {

    private final ClassLoader m_classLoader;

    public CustomClassLoadingObjectInputStream(final InputStream in, final ClassLoader classLoader) throws IOException {
        super(in);
        m_classLoader = classLoader;
    }

    @Override
    protected Class<?> resolveClass(final ObjectStreamClass desc) throws IOException, ClassNotFoundException {
        String name = desc.getName();
        try {
            return Class.forName(name, false, m_classLoader);
        } catch (ClassNotFoundException ex) {
            return super.resolveClass(desc);
        }
    }
}
