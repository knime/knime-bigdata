package com.knime.tpbuilder.aether

import java.nio.file.Paths

import scala.collection.mutable.HashMap
import scala.collection.Set
import scala.collection.mutable.{Set => MutableSet}

import org.apache.maven.repository.internal.MavenRepositorySystemUtils
import org.eclipse.aether.DefaultRepositorySystemSession
import org.eclipse.aether.RepositorySystem
import org.eclipse.aether.artifact.{ Artifact => AetherArtifact }
import org.eclipse.aether.artifact.DefaultArtifact
import org.eclipse.aether.connector.basic.BasicRepositoryConnectorFactory
import org.eclipse.aether.graph.Dependency
import org.eclipse.aether.repository.LocalRepository
import org.eclipse.aether.repository.RemoteRepository
import org.eclipse.aether.spi.connector.RepositoryConnectorFactory
import org.eclipse.aether.spi.connector.transport.TransporterFactory
import org.eclipse.aether.transport.file.FileTransporterFactory
import org.eclipse.aether.transport.http.HttpTransporterFactory

import com.knime.tpbuilder.Artifact
import com.knime.tpbuilder.osgi.OsgiUtil
import java.io.File
import com.knime.tpbuilder.TPConfigReader.TPConfig

object AetherUtils {
  val localRepo = new LocalRepository(Paths.get(System.getProperty("user.home"), ".m2", "repository").toFile())

  private val mavenCentral = new RemoteRepository.Builder("maven-central", "default", "http://repo1.maven.org/maven2/").build()
  
  private val locator = MavenRepositorySystemUtils.newServiceLocator()

  val repoSys = {
    locator.addService(classOf[RepositoryConnectorFactory], classOf[BasicRepositoryConnectorFactory])
    locator.addService(classOf[TransporterFactory], classOf[FileTransporterFactory])
    locator.addService(classOf[TransporterFactory], classOf[HttpTransporterFactory])

    locator.getService(classOf[RepositorySystem])
  }

  val repoSession = {
    val session = MavenRepositorySystemUtils.newSession().asInstanceOf[DefaultRepositorySystemSession]
    session.setLocalRepositoryManager(repoSys.newLocalRepositoryManager(session, AetherUtils.localRepo))
    session
  }

  /**
   * Collects information which remote repositories were used to resolve the metadata
   * of a maven artifact.
   */
  val remoteArtifactRepos = HashMap[String, Set[RemoteRepository]]()
  
  val defaultRemoteArtifactRepos: Set[RemoteRepository] = MutableSet(mavenCentral)
  
  def initDefaultRemoteArtifactRepos(config: TPConfig) = {
    for (repo <- config.mavenRepositories) {
      defaultRemoteArtifactRepos
        .asInstanceOf[MutableSet[RemoteRepository]]
        .add(new RemoteRepository.Builder(repo.id, "default", repo.url).build())
    }
  }

  def depToArt(dep: Dependency): Artifact = {
    val aetherArt = dep.getArtifact
    aetherArtToArt(aetherArt)
  }

  def artToAetherArt(art: Artifact): AetherArtifact = {
    new DefaultArtifact(
      art.group,
      art.artifact,
      art.classifier.getOrElse(""),
      "jar",
      art.version)
  }
  
  def artToDep(art: Artifact): Dependency = {
    new Dependency(new DefaultArtifact(
      art.group,
      art.artifact,
      art.classifier.getOrElse(""),
      "jar",
      art.version),
      "runtime")
  }


  def aetherArtToArt(aetherArt: AetherArtifact): Artifact = {
    val art = Artifact(group = aetherArt.getGroupId,
      artifact = aetherArt.getArtifactId,
      version = aetherArt.getVersion,
      classifier =
        if (aetherArt.getClassifier.length() > 0)
          Some(aetherArt.getClassifier)
        else None,
      file = Option(aetherArt.getFile))
    art
  }
}