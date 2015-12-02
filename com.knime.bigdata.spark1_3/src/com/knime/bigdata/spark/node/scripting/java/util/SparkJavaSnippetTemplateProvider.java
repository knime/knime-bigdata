/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ------------------------------------------------------------------------
 *
 * History
 *   02.04.2012 (hofer): created
 */
package com.knime.bigdata.spark.node.scripting.java.util;



import java.util.ArrayList;
import java.util.List;

import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.IConfigurationElement;
import org.eclipse.core.runtime.IExtensionRegistry;
import org.eclipse.core.runtime.Platform;
import org.knime.base.node.jsnippet.template.AbstractJSnippetTemplateProvider;
import org.knime.base.node.jsnippet.template.TemplateProvider;
import org.knime.base.node.jsnippet.template.TemplateRepository;
import org.knime.base.node.jsnippet.template.TemplateRepositoryProvider;
import org.knime.core.node.NodeLogger;


/**
 * A central provider for Java Snippet templates.
 *
 * @author Tobias Koetter, KNIME.com
 */
public final class SparkJavaSnippetTemplateProvider extends AbstractJSnippetTemplateProvider<SparkJavaSnippetTemplate> {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SparkJavaSnippetTemplateProvider.class);

    private static final Object LOCK = new Object[0];
    private static final String EXTENSION_POINT_ID = "com.knime.bigdata.spark1_3.TemplateRepository";
    private static TemplateProvider<SparkJavaSnippetTemplate> provider;
    /**
     * prevent instantiation from outside.
     */
    private SparkJavaSnippetTemplateProvider() {
        super(new SparkJavaSnippetFileTemplateRepositoryProvider().getRepository());
        setRepositories(loadExtension());
    }

    private List<TemplateRepository<SparkJavaSnippetTemplate>> loadExtension() {
        final List<TemplateRepository<SparkJavaSnippetTemplate>> m_repos = new ArrayList<>();
        final IExtensionRegistry extensionRegistry = Platform.getExtensionRegistry();
        final IConfigurationElement[] config =
                extensionRegistry.getConfigurationElementsFor(EXTENSION_POINT_ID);
        for (IConfigurationElement e : config) {
            try {
                final Object o = e.createExecutableExtension("provider-class");
                if (o instanceof TemplateRepositoryProvider) {
                    @SuppressWarnings("unchecked")
                    TemplateRepository<SparkJavaSnippetTemplate> repo =
                        ((TemplateRepositoryProvider<SparkJavaSnippetTemplate>)o).getRepository();
                    if (null != repo) {
                        repo.addChangeListener(this);
                        m_repos.add(repo);
                    }
                }
            } catch (CoreException ex) {
                LOGGER.error("Error while reading Spark snippet template repositories.", ex);
            }
        }
        return m_repos;
    }

    /**
     * Get default shared instance.
     * @return default TemplateProvider
     */
    public static TemplateProvider<SparkJavaSnippetTemplate> getDefault() {
         synchronized (LOCK) {
            if (null == provider) {
                provider = new SparkJavaSnippetTemplateProvider();
            }
        }
        return provider;
    }

}
