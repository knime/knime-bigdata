/*
 * ------------------------------------------------------------------------
 *
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
 * ---------------------------------------------------------------------
 *
 * History
 *   Nov 7, 2016 (Sascha Wolke, KNIME.com): created
 */
package com.knime.bigdata.testing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.core.runtime.Platform;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.util.FileUtil;
import org.knime.testing.core.TestrunJanitor;

/**
 * Provides big data flow variables based on a CSV file. CSV contains string key/value pairs and a header line.
 *
 * If FLOWVARS ENV is not set, this janitor searches in {@link BigDataExtensionJanitor#DEFAULT_PATH_WORKFLOW} or
 * {@link #DEFAULT_PATH_WORKSPACE} for flowvariables.csv.
 *
 * @author Sascha Wolke, KNIME.com
 */
public class BigDataExtensionJanitor extends TestrunJanitor {

    /** Default CSV file path if FLOWVARS is not set in ENV. */
    public final String DEFAULT_PATH_WORKFLOW = "knime://knime.workflow/../flowvariables.csv";

    /** Default flowvariables.csv path in workspace. */
    public final String DEFAULT_PATH_WORKSPACE = Platform.getLocation().append("/flowvariables.csv").toString();

    private final List<FlowVariable> m_flowVariables;

    /** Default constructor. */
    public BigDataExtensionJanitor() {
        m_flowVariables = new ArrayList<>();
    }

    @Override
    public List<FlowVariable> getFlowVariables() {
        return m_flowVariables;
    }

    /**
     * Searches for flowvariables.csv in FLOWVARS ENV, {@link #DEFAULT_PATH_WORKFLOW} or {@link #DEFAULT_PATH_WORKSPACE}.
     * @return File if found
     * @throws FileNotFoundException - If no location contains CSV file.
     * @throws MalformedURLException - If {@link #DEFAULT_PATH_WORKFLOW} has wrong format.
     */
    private File getInputFile() throws FileNotFoundException, MalformedURLException {
        if (System.getenv("FLOWVARS") != null) {
            final File env = new File(System.getenv("FLOWVARS"));
            if (env.exists() && env.canRead()) {
                return env;
            }
        }

        final File workflow = FileUtil.getFileFromURL(new URL(DEFAULT_PATH_WORKFLOW));
        if (workflow.exists() && workflow.canRead()) {
            return workflow;
        }

        final File workspace = new File(DEFAULT_PATH_WORKSPACE);
        if (workspace.exists() && workspace.canRead()) {
            return workspace;
        }

        throw new FileNotFoundException("Unable to locate flowvariables.csv");
    }

    @Override
    public void before() throws Exception {
        try (final BufferedReader reader = new BufferedReader(new FileReader(getInputFile()));) {

            m_flowVariables.clear();
            String line = reader.readLine(); // header

            // Read lines into string flow variables, ignore variables with empty values.
            while ((line = reader.readLine()) != null) {
                final String kv[] = line.split(",", 2);

                if (kv.length == 2 && !StringUtils.isBlank(kv[1])) {
                    m_flowVariables.add(new FlowVariable(kv[0], kv[1]));
                }
            }
        }
    }

    @Override
    public void after() throws Exception {
    	//nothing to do
    }

    @Override
    public String getName() {
        return "Big Data Extension flowvariables.csv ";
    }

    @Override
    public String getID() {
        return "com.knime.bigdata.spark.testing.BigDataJanitor";
    }

    @Override
    public String getDescription() {
        return "Loads flowvariables.csv from path provided by environment FLOWVARS variable, relativ to current workflow or in root workspace directory.";
    }
}
