package com.knime.tpbuilder

import java.io.PrintStream
import org.apache.maven.shared.invoker.InvokerLogger
import org.apache.maven.shared.invoker.DefaultInvoker
import java.io.FileOutputStream
import org.apache.maven.shared.invoker.DefaultInvocationRequest
import org.apache.maven.shared.invoker.PrintStreamLogger
import java.io.File

import collection.JavaConverters._
import org.apache.maven.shared.invoker.InvocationOutputHandler
import org.apache.maven.shared.invoker.InvocationResult
import java.util.Properties

object MavenInvokerUtil {

  private class OutputHandlerAdapater(logStream: PrintStream, lineHandler: String => Unit) extends InvocationOutputHandler {
    override def consumeLine(line: String) = {
      logStream.println(line)
      lineHandler(line)
    }
  }

  def run(goals: Seq[String],
    lineHandler: String => Unit = (_) => {},
    pomFile: Option[File] = None,
    systemProperties: Map[String, String] = Map()): InvocationResult = {

    val request = new DefaultInvocationRequest()
    pomFile match {
      case Some(file) => request.setPomFile(file)
      case None => {}
    }
    
    request.setGoals(goals.asJava)
    
    val sysProps = Option(request.getProperties).getOrElse(new Properties())
    for (propKey <- systemProperties.keys) {
      sysProps.setProperty(propKey, systemProperties(propKey))
    }
    request.setProperties(sysProps)

    val invoker = new DefaultInvoker();

    val logOutputStream = new PrintStream(new FileOutputStream(new File(new File("target"), "maven-invoker.log"), true))
    invoker.setLogger(new PrintStreamLogger(logOutputStream, InvokerLogger.INFO))

    invoker.setOutputHandler(new OutputHandlerAdapater(logOutputStream, lineHandler))

    try
      invoker.execute(request)
    finally logOutputStream.close()
  }
}