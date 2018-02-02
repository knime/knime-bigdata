package org.knime.bigdata.spark.local.wrapper;

import java.util.Map;
import java.util.Set;

import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;

@SparkClass
public interface LocalSparkWrapper {

	void openSparkContext(String name, int workerThreads, Map<String, String> sparkConf, boolean enableHiveSupport, boolean startThriftserver,
			String hiveDataFolder) throws KNIMESparkException;

	void destroy() throws KNIMESparkException;

	Map<String, Object> runJob(Map<String, Object> localSparkInputMap);

	Set<String> getNamedObjects() throws KNIMESparkException;

	void deleteNamedObjects(final Set<String> namedObjects) throws KNIMESparkException;

	String getSparkWebUIUrl();
	
	int getHiveserverPort();
}