@Library('knime-pipeline@master') _

node {
	def upstreamParams = defaultProperties('org.knime.update.analytics-platform', 'com.knime.update.pmml.compilation',
		'com.knime.update.productivity')
	
	stage('Clean workspace') {
		cleanWorkspace()
		sh 'rm -rf git/knime-bigdata/com.knime.features.bigdata.externals/maven2osgi/target'
	}
	
	checkoutSources (
		defaultBranch: 'master',
		credentialsId: 'bitbucket-jenkins',
		repos: [
			[name : 'knime-bigdata', branch: BRANCH_NAME],
			[name : 'knime-config'],
			[name : 'knime-jenkins']
		]
	)
	
	stage('Maven-to-OSGi') {
		withMaven(maven: 'Maven 3.2') {
			sh 'mvn -f "$WORKSPACE"/git/knime-bigdata/com.knime.features.bigdata.externals/maven2osgi/pom.xml p2:site'
		}
		
		sh '''
			source "git/knime-jenkins/org.knime.build.config/hudson-product-install.inc"

			rm -f git/knime-bigdata/com.knime.features.bigdata.externals/maven2osgi/target/repository/{artifacts,content}.{jar,xml}
			# Remove Guava package from Spark Core 1.2.2 (see BD-240)
			zip git/knime-bigdata/com.knime.features.bigdata.externals/maven2osgi/target/repository/plugins/org.apache.spark.core_2.10_1.2.*.jar -d 'com*'
		
			# Recompute artifacts.jar because the hash sums have changed
			p2-publisher -metadataRepository "file:$WORKSPACE/git/knime-bigdata/com.knime.features.bigdata.externals/maven2osgi/target/repository" \
				-artifactRepository "file:$WORKSPACE/git/knime-bigdata/com.knime.features.bigdata.externals/maven2osgi/target/repository" \
				-compress \
				-source "$WORKSPACE/git/knime-bigdata/com.knime.features.bigdata.externals/maven2osgi/target/repository"
		'''
		buckminster (
			commands: '''
				setpref org.eclipse.buckminster.pde.targetPlatformPath='$WORKSPACE/buckminster.targetPlatform'
				import 'git/knime-bigdata/com.knime.update.bigdata.externals/com.knime.update.bigdata.externals.cquery'
				build -c
				perform -D site.signing=false com.knime.update.bigdata.externals#site.p2
			'''
		)
		sh '''
			mv buckminster.output/com.knime.update.bigdata.externals_*/site.p2 com.knime.update.bigdata.externals
			rm -rf .metadata buckminster.*
		'''
	}
	
	stage('Build update site') {
		buckminster (
			component: 'com.knime.update.bigdata',
			baseline: [file: 'git/knime-config/org.knime.config/API-Baseline.target', name: 'Release 3.3'],
			repos: [
				"$JENKINS_URL/jobs/${upstreamParams['org.knime.update.analytics-platform'].p2}",
				"$JENKINS_URL/jobs/${upstreamParams['com.knime.update.pmml.compilation'].p2}",
				"$JENKINS_URL/jobs/${upstreamParams['com.knime.update.productivity'].p2}",
				"file:///${WORKSPACE.replace('\\', '/')}/com.knime.update.bigdata.externals/"
			]
		)

		finalizeP2Repository()
	}
	
	stage('Archive artifacts') {
		archive()
		archiveArtifacts artifacts: "com.knime.update.bigdata.externals/**", fingerprint: true
		step([$class: 'Mailer', notifyEveryUnstableBuild: true, recipients: 'testing@knime.com', sendToIndividuals: true])
	}
}
