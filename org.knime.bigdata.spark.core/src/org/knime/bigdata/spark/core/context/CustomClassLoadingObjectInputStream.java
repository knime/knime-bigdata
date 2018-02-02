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
package org.knime.bigdata.spark.core.context;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * A subclass of {@link ObjectInputStream} which accepts a custom classloader. The custom classloader will be used to
 * load the classes of deserialized objects.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class CustomClassLoadingObjectInputStream extends ObjectInputStream {

    private final ClassLoader m_classLoader;

    /**
     * @param in {@link InputStream} to read from
     * @param classLoader the {@link ClassLoader} to use while reading from the {@link InputStream}
     * @throws IOException
     */
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
