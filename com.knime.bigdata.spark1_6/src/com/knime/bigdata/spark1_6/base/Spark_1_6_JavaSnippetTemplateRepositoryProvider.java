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
 *   07.06.2012 (hofer): created
 */
package com.knime.bigdata.spark1_6.base;

import org.knime.base.node.jsnippet.template.PluginTemplateRepositoryProvider;
import org.knime.base.node.jsnippet.template.SnippetTemplateFactory;
import org.knime.base.node.jsnippet.template.TemplateRepository;
import org.knime.core.node.NodeSettingsRO;
import org.osgi.framework.FrameworkUtil;

import com.knime.bigdata.spark.node.scripting.java.util.template.AbstractJavaSnippetTemplateRepositoryProvider;
import com.knime.bigdata.spark.node.scripting.java.util.template.SparkJavaSnippetTemplate;
import com.knime.bigdata.spark1_6.api.AllSpark_1_6_CompatibilityChecker;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class Spark_1_6_JavaSnippetTemplateRepositoryProvider extends AbstractJavaSnippetTemplateRepositoryProvider {

    /**
     * Directory relative to the plugin root where the snippet templates are stored.
     */
    private static final String SNIPPET_TEMPLATES_DIRECTORY = "snippetTemplates";

    private final PluginTemplateRepositoryProvider<SparkJavaSnippetTemplate> proxiedInstance;

    /**
     * Create a instance for the bundle "com.knime.bigdata.spark" and the relative path "/snippetTemplates".
     */
    public Spark_1_6_JavaSnippetTemplateRepositoryProvider() {
        super(AllSpark_1_6_CompatibilityChecker.INSTANCE);

        final String currentPluginName = FrameworkUtil.getBundle(getClass()).getSymbolicName();

        proxiedInstance = new PluginTemplateRepositoryProvider<>(currentPluginName, SNIPPET_TEMPLATES_DIRECTORY,
            new SnippetTemplateFactory<SparkJavaSnippetTemplate>() {
                @Override
                public SparkJavaSnippetTemplate create(final NodeSettingsRO settings) {
                    return SparkJavaSnippetTemplate.create(settings);
                }
            });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TemplateRepository<SparkJavaSnippetTemplate> getRepository() {
        return proxiedInstance.getRepository();
    }
}
