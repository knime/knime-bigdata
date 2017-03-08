@Library('knime-pipeline@releases/2016-12') _

node {
	def upstreamParams = defaultProperties('org.knime.update.analytics-platform@releases/3.3', 'com.knime.update.pmml.compilation@releases/3.3',
		'com.knime.update.productivity@releases/3.3')
	
	stage('Clean workspace') {
		cleanWorkspace()
		try {
			sh 'rm -rf git/knime-bigdata/com.knime.features.bigdata.externals/maven2osgi/target com.knime.update.bigdata.externals'
		} catch (ex) {
			currentBuild.result = 'FAILED'
			emailext (
				subject: "FAILURE in ${env.JOB_NAME} [${env.BUILD_NUMBER}]",
				body: "Failure while cleaning workspace. Check console output at ${env.BUILD_URL}/console",
				recipientProviders: [[$class: 'DevelopersRecipientProvider'], [$class: 'RequesterRecipientProvider']]
			)
			throw ex
		}

	}
	
	stage('Checkout sources') {
		checkoutSources (
			defaultBranch: 'releases/3.3',
			credentialsId: 'bitbucket-jenkins',
			repos: [
				[name : 'knime-bigdata', branch: BRANCH_NAME],
				[name : 'knime-config'],
				[name : 'knime-jenkins']
			]
		)
	}
	
	stage('Maven-to-OSGi') {
		try {
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
		} catch (ex) {
			if (currentBuild.result == null) {
				currentBuild.result = 'FAILED'
				emailext (
					subject: "FAILURE in ${env.JOB_NAME} [${env.BUILD_NUMBER}]",
					body: "Failure while building target platform. Check console output at ${env.BUILD_URL}/console",
					recipientProviders: [[$class: 'DevelopersRecipientProvider'], [$class: 'RequesterRecipientProvider']]
				)
			}
			throw ex
		}
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
		try {
			archiveArtifacts artifacts: "com.knime.update.bigdata.externals/**", fingerprint: true
		} catch (ex) {
			currentBuild.result = 'FAILED'
			emailext (
				subject: "FAILURE in ${env.JOB_NAME} [${env.BUILD_NUMBER}]",
				body: "Failure while archiving artifacts. Check console output at ${env.BUILD_URL}/console",
				recipientProviders: [[$class: 'DevelopersRecipientProvider'], [$class: 'RequesterRecipientProvider']]
			)
			throw ex
		}
	}
}
