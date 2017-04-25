package com.knime.tpbuilder.osgi

import com.knime.tpbuilder.MavenInvokerUtil

object P2SiteMaker {

  def createSite() = {
    val result = MavenInvokerUtil.run(Seq("org.eclipse.tycho.extras:tycho-p2-extras-plugin:1.0.0:publish-features-and-bundles"))
    require(result.getExitCode == 0, "Failed to create p2 update site")
  }
}