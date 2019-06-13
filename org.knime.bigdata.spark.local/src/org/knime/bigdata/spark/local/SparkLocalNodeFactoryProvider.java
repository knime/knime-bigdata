/**
 * 
 */
package org.knime.bigdata.spark.local;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactoryProvider;
import org.knime.bigdata.spark.core.version.AllVersionCompatibilityChecker;
import org.knime.bigdata.spark.local.node.create.LocalEnvironmentCreatorNodeFactory;
import org.knime.bigdata.spark.local.node.create.LocalEnvironmentCreatorNodeFactory2;

/**
 * Node factory provider for Spark local.
 * 
 * @author Oleg Yasnev, KNIME GmbH
 */
public class SparkLocalNodeFactoryProvider extends DefaultSparkNodeFactoryProvider {

	/**
	 * Default constructor.
	 */
    public SparkLocalNodeFactoryProvider() {
        super(AllVersionCompatibilityChecker.INSTANCE, new LocalEnvironmentCreatorNodeFactory(), new LocalEnvironmentCreatorNodeFactory2());
    }

}
