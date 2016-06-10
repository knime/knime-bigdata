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
 *   Created on May 12, 2016 by bjoern
 */
package com.knime.bigdata.spark.node.scripting.java;

import java.io.File;
import java.util.Objects;

import org.knime.core.util.Pair;

/**
 * Used to cache the jar file resulting from Spark java snippet compilation. This cache exists once per node model
 * instance. Cache key is the hash of the snippet class source code. This cache contains max one jar file. Updating the
 * cache deletes the previously cached jar file.
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class SnippetCompilationCache {

    private String m_currHash;

    private Pair<String, File> m_currCompiledSnippet;

    public boolean isCached(final String hash) {
        return Objects.equals(m_currHash, hash);
    }

    public Pair<String, File> getCachedCompiledSnippet() {
        return m_currCompiledSnippet;
    }

    public void updateCache(final String newHash, final String snippetClassName, final File newCompiledSnippet) {
        if (m_currCompiledSnippet != null) {
            m_currCompiledSnippet.getSecond().delete();
        }

        m_currHash = newHash;
        m_currCompiledSnippet = new Pair<>(snippetClassName, newCompiledSnippet);
    }
}
