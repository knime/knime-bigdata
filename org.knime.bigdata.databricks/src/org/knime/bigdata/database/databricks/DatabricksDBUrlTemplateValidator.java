/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
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
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
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
 * ---------------------------------------------------------------------
 *
 * History
 *   Oct 25, 2019 (Sascha Wolke, KNIME GmbH): created
 */
package org.knime.bigdata.database.databricks;

import static java.util.Collections.unmodifiableMap;
import static org.knime.database.driver.URLTemplates.VARIABLE_NAME_HOST;
import static org.knime.database.driver.URLTemplates.VARIABLE_NAME_PORT;
import static org.knime.database.driver.URLTemplates.validateDriverUrlTemplate;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.IConfigurationElement;
import org.eclipse.core.runtime.IExecutableExtension;
import org.eclipse.core.runtime.IExecutableExtensionFactory;
import org.knime.core.node.NodeLogger;
import org.knime.database.node.connector.AbstractUrlTemplateValidator;
import org.knime.database.node.connector.server.ServerUrlTemplateValidator;
import org.knime.database.util.NestedTokenException;
import org.knime.database.util.NoSuchTokenException;
import org.knime.database.util.StringTokenException;

/**
 * Validator for Databricks database URLs.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public final class DatabricksDBUrlTemplateValidator extends AbstractUrlTemplateValidator implements IExecutableExtension {

    /**
     * Executable extension factory that prevents the creation of multiple {@link ServerUrlTemplateValidator} objects by
     * the executable extension creation process. Although multiple factory objects are still created, those instances
     * can be discarded after production of the singleton result.
     *
     * @author Noemi Balassa
     */
    public static class ExecutableExtensionFactory implements IExecutableExtensionFactory {
        @Override
        public Object create() throws CoreException {
            return INSTANCE;
        }
    }

    private static final NodeLogger LOGGER = NodeLogger.getLogger(ServerUrlTemplateValidator.class);

    private static final Map<String, String> VARIABLES;
    static {
        final Map<String, String> variables = new LinkedHashMap<String, String>();
        variables.put(VARIABLE_NAME_HOST, "The hostname of a server.");
        variables.put(VARIABLE_NAME_PORT, "The port on which the server is listening.");
        VARIABLES = unmodifiableMap(variables);
    }

    private static final Set<String> VARIABLE_NAMES = VARIABLES.keySet();

    /**
     * The singleton {@link ServerUrlTemplateValidator} instance.
     */
    static final ServerUrlTemplateValidator INSTANCE = new ServerUrlTemplateValidator();

    @Override
    public void setInitializationData(final IConfigurationElement config, final String propertyName, final Object data)
        throws CoreException {
        try {
            setExamplesFrom(config, false);
        } catch (final Throwable throwable) {
            LOGGER.error("The examples of an URL template validator could not be set in the extension: "
                + config.getDeclaringExtension().getNamespaceIdentifier(), throwable);
        }
    }

    @Override
    public Map<String, String> getConditionVariables() {
        return VARIABLES;
    }

    @Override
    public Map<String, String> getTokenVariables() {
        return VARIABLES;
    }

    @Override
    public Optional<String> validate(final CharSequence urlTemplate) {
        final String template = urlTemplate.toString().toLowerCase();
        if (!template.startsWith("jdbc:spark:") && !template.startsWith("jdbc:hive2:")) {
            return Optional.of("Template must start with 'jdbc:spark:' or 'jdbc:hive2:'.");
        }

        try {
            validateDriverUrlTemplate(urlTemplate, VARIABLE_NAMES, VARIABLE_NAMES);
        } catch (final NestedTokenException exception) {
            final String token = exception.getToken();
            return Optional
                .of("Illegally nested variable, or illegally nested token in the content of the condition: \""
                    + (token == null ? "" : token) + '"');
        } catch (final NoSuchTokenException exception) {
            final String token = exception.getToken();
            return Optional.of("Invalid variable: \"" + (token == null ? "" : token) + '"');
        } catch (final StringTokenException exception) {
            final String message = exception.getMessage();
            return Optional.of(message == null ? "Erroneous template." : message);
        } catch (final Throwable throwable) {
            LOGGER.error("An error occurred during URL template validation.", throwable);
        }
        return Optional.empty();
    }
}
