package org.knime.bigdata.testing;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactoryProvider;
import org.knime.bigdata.spark.core.version.AllVersionCompatibilityChecker;
import org.knime.bigdata.testing.node.create.CreateTestEnvironmentNodeFactory;

/**
 * Node factory provider for big data testing nodes.
 * 
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class BigDataTestingNodeFactory extends DefaultSparkNodeFactoryProvider {

    /**
     * Default constructor.
     */
    public BigDataTestingNodeFactory() {
        super(AllVersionCompatibilityChecker.INSTANCE, new CreateTestEnvironmentNodeFactory());
    }

}
