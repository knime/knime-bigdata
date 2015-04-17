/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
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
 *   Created on 16.04.2015 by Alexander
 */
package com.knime.bigdata.spark.node.mllib.pmml.predictor;

import java.io.Serializable;
import java.util.Map;

import org.knime.base.pmml.translation.java.compile.CompiledModel;

/**
 *
 * @author Alexander Fillbrunn
 */
public class SerializableCompiledModel implements Serializable {

    private Map<String,byte[]> m_bytecode;
    private transient CompiledModel m_model = null;

    public SerializableCompiledModel(final Map<String,byte[]> bytecode) {
        m_bytecode = bytecode;
    }

    public CompiledModel getCompiledModel() {
        if (m_model == null) {
            ClassLoader cl = new ClassLoader(Thread.currentThread().getContextClassLoader()) {
                /**
                 * {@inheritDoc}
                 */
                @Override
                protected Class<?> findClass(final String name) throws ClassNotFoundException {
                    byte[] bc = m_bytecode.get(name);
                    return defineClass(name, bc, 0, bc.length);
                }
            };
            try {
                m_model = new CompiledModel(cl);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                return null;
            }
        }
        return m_model;
    }
}
