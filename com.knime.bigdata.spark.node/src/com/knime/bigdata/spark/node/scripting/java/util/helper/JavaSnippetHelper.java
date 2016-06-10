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
 *   Created on May 6, 2016 by bjoern
 */
package com.knime.bigdata.spark.node.scripting.java.util.helper;

import java.io.File;
import java.util.List;

import org.knime.base.node.jsnippet.guarded.GuardedDocument;
import org.knime.base.node.jsnippet.util.JavaSnippetFields;
import org.knime.base.node.jsnippet.util.JavaSnippetSettings;

import com.knime.bigdata.spark.core.version.SparkProvider;

/**
 * Instances of this class are used by the Spark Java snippet nodes to obtain everything that is required to write Spark
 * version-specific snippets. This means for example the {@link GuardedDocument} and jar files to put into the
 * classpath.
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public interface JavaSnippetHelper extends SparkProvider {

    public enum SnippetType {
            SOURCE,

            SINK,

            INNER;
    }

    GuardedDocument createGuardedSnippetDocument(SnippetType type, JavaSnippetSettings settings);

    void updateGuardedSections(SnippetType type, GuardedDocument doc, JavaSnippetFields fields);

    void updateAllSections(SnippetType snippetType, GuardedDocument document, JavaSnippetSettings settings);

    String getSnippetClassName(SnippetType type);

    String getSnippetClassName(SnippetType type, String suffix);

    void updateGuardedClassnameSuffix(SnippetType type, GuardedDocument doc, JavaSnippetFields fields, String suffix);

    List<File> getSnippetClasspath();

    String getDefaultContent(SnippetType snippetType);
}
