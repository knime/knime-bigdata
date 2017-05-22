package com.knime.tpbuilder.aether

import collection.JavaConverters._
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import scala.collection.mutable.Set
import org.eclipse.aether.resolution.ArtifactRequest
import com.knime.tpbuilder.Artifact
import org.eclipse.aether.resolution.ArtifactResult
import org.eclipse.aether.RepositorySystemSession
import org.eclipse.aether.repository.RemoteRepository
import scala.collection.mutable.HashSet
import org.eclipse.aether.resolution.ArtifactResolutionException
import com.knime.tpbuilder.osgi.OsgiUtil
import com.knime.tpbuilder.TPConfigReader.TPConfig
import org.eclipse.aether.util.artifact.SubArtifact
import org.eclipse.aether.artifact.{ Artifact => AetherArtifact }
import java.io.File

object DepGraphResolver {

  def resolveDepGraph(depGraph: Map[Artifact, Set[Artifact]], config: TPConfig): Map[Artifact, Set[Artifact]] = {
    val oldGraph = HashMap[Artifact, Set[Artifact]]()
    oldGraph ++= depGraph

    depGraph.clear()

    // val remoteRepos = Seq(AetherUtils.mavenCentral).asJava
    val session = AetherUtils.repoSession

    val resolvedArtifactCache = HashMap[String, Artifact]()
    val artsWithSource = TPConfig.getArtifactCoordsWithSource(config)
    val resolve = doResolve(session, config, resolvedArtifactCache, artsWithSource)(_)

    for (oldArt <- oldGraph.keys) {
      depGraph += (resolve(oldArt) -> oldGraph(oldArt).map(resolve))
    }

    depGraph
  }

  private def doResolve(session: RepositorySystemSession, config: TPConfig, resolvedArtifactCache: Map[String, Artifact], artsWithSource: Set[String])(art: Artifact): Artifact = {

    val artCoord = art.mvnCoordinate

    resolvedArtifactCache.get(artCoord) match {
      case Some(resolvedArt) => resolvedArt
      case None => {
        println(s"  Resolving ${artCoord}")

        val remoteRepos = AetherUtils.remoteArtifactRepos(artCoord).toSeq

        val aetherArt = AetherUtils.artToAetherArt(art)
        val result = AetherUtils.repoSys.resolveArtifact(session,
          new ArtifactRequest(aetherArt, remoteRepos.asJava, null))

        val resolvedArt = OsgiUtil.withBundleAndVersion(AetherUtils.aetherArtToArt(result.getArtifact), None, config)
        println(s"    Bundle-SymbolicName ${resolvedArt.bundle.get.bundleSymbolicName} / Bundle-Version: ${resolvedArt.bundle.get.bundleVersion}")

        // only grab the source for jars where this is explicitly requested or if the jar is already prebundled 
        val sourceFile = if (config.source.get && artsWithSource(artCoord)) {
              trySourceResolution(aetherArt, remoteRepos, session)
            println(s"    Bundle-SymbolicName ${resolvedArt.bundle.get.bundleSymbolicName}.source / Bundle-Version: ${resolvedArt.bundle.get.bundleVersion}")
            trySourceResolution(aetherArt, remoteRepos, session)
          } else None

        val resolvedArtWithOptionalSource = OsgiUtil.withBundle(resolvedArt, resolvedArt.bundle.get, resolvedArt.file, sourceFile)

        resolvedArtifactCache += (artCoord -> resolvedArtWithOptionalSource)

        resolvedArtWithOptionalSource
      }
    }
  }

  private def trySourceResolution(aetherArt: AetherArtifact, remoteRepos: Seq[RemoteRepository], session: RepositorySystemSession): Option[File] = {

    try {
      val sourceAetherart = new SubArtifact(aetherArt, "*-sources", "jar")
      val sourceResult = AetherUtils.repoSys.resolveArtifact(session, new ArtifactRequest(sourceAetherart, remoteRepos.asJava, null))
      return Some(sourceResult.getArtifact.getFile)
    } catch {
      case e: ArtifactResolutionException => {} // do nothing for now
    }

    try {
      val sourceAetherart = new SubArtifact(aetherArt, "*-sources", "src")
      val sourceResult = AetherUtils.repoSys.resolveArtifact(session, new ArtifactRequest(sourceAetherart, remoteRepos.asJava, null))
      return Some(sourceResult.getArtifact.getFile)
    } catch {
      case e: ArtifactResolutionException => {
        System.err.println(s"    Could not resolve source for: ${AetherUtils.aetherArtToArt(aetherArt).mvnCoordinate}")
        return None
      }
    }
  }
}
