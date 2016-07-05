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
 *   Created on May 13, 2016 by bjoern
 */
package com.knime.bigdata.spark.node.scripting.java.util.template;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.knime.base.node.jsnippet.template.AbstractJSnippetTemplateProvider;
import org.knime.base.node.jsnippet.template.TemplateProvider;
import org.knime.base.node.jsnippet.template.TemplateRepository;

import com.knime.bigdata.spark.core.version.SparkProviderRegistry;
import com.knime.bigdata.spark.core.version.SparkVersion;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class JavaSnippetTemplateProviderRegistry extends SparkProviderRegistry<JavaSnippetTemplateRepositoryProvider> {

    private static final TemplateRepository<SparkJavaSnippetTemplate> WORKSPACE_REPOSITORY =
        new WorkspaceTemplateRepositoryProvider().getRepository();

    /** The id of the converter extension point. */
    private static final String EXT_POINT_ID = "com.knime.bigdata.spark.node.JavaSnippetTemplateRepository";

    private static class AggregateTemplateRepository
        extends AbstractJSnippetTemplateProvider<SparkJavaSnippetTemplate> {

        public AggregateTemplateRepository() {
            super(WORKSPACE_REPOSITORY);

        }

        public void addRepository(final TemplateRepository<SparkJavaSnippetTemplate> repo) {
            // despite the name this actually adds
            setRepositories(Collections.singletonList(repo));
        }
    }

    private static JavaSnippetTemplateProviderRegistry instance;

    private final Map<SparkVersion, AggregateTemplateRepository> m_repositoryMap = new HashMap<>();

    private JavaSnippetTemplateProviderRegistry() {
    }

    /**
     * Returns the only instance of this class.
     *
     * @return the only instance
     */
    public static synchronized JavaSnippetTemplateProviderRegistry getInstance() {
        if (instance == null) {
            instance = new JavaSnippetTemplateProviderRegistry();
            instance.registerExtensions(EXT_POINT_ID);
        }
        return instance;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void addProvider(final JavaSnippetTemplateRepositoryProvider sparkProvider) {

        for (SparkVersion sparkVersion : sparkProvider.getSupportedSparkVersions()) {
            AggregateTemplateRepository repositoryForVersion = m_repositoryMap.get(sparkVersion);

            if (repositoryForVersion == null) {
                repositoryForVersion = new AggregateTemplateRepository();
                m_repositoryMap.put(sparkVersion, repositoryForVersion);
            }

            repositoryForVersion.addRepository(sparkProvider.getRepository());
        }
    }

    /**
     * @param sparkVersion
     * @return a {@link TemplateRepository}s that aggregates all snippet templates registered for the given Spark
     *         version.
     */
    public synchronized TemplateProvider<SparkJavaSnippetTemplate> getTemplateProvider(final SparkVersion sparkVersion) {
        return m_repositoryMap.get(sparkVersion);
    }
}