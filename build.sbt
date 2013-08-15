// -*- mode: scala -*-
organization := "org.whym"

name    := "revdiffsearch"

version := "0.2"

scalaVersion := "2.10.2"

scalacOptions ++= Seq("-deprecation",
                      "-unchecked",
                      "-optimise",
                      "-explaintypes",
                      "-g:line")

javacOptions ++= Seq("-Xlint:unchecked")

libraryDependencies ++= Seq(
  "org.json4s"        %% "json4s-native" % "3.2.4",
  "org.scalatest"     %% "scalatest"     % "2.0.M5b" % "test",
  "com.typesafe"      %% "scalalogging-slf4j" % "1.0.1",
  "com.typesafe"       % "config"        % "1.0.0",
  "org.slf4j"          % "slf4j-api"     % "1.7.1",
  "ch.qos.logback"     % "logback-classic" % "1.0.7",
  "org.mockito"        % "mockito-core"  % "1.9.0" % "test",
  "org.apache.commons" % "commons-lang3" % "3.1",
  "commons-codec"      % "commons-codec" % "1.6",
  "org.apache.lucene"  % "lucene-core"             % "4.4.0",
  "org.apache.lucene"  % "lucene-analyzers-common" % "4.4.0",
  "org.apache.lucene"  % "lucene-queryparser"      % "4.4.0",
  "org.jboss.netty"    % "netty"         % "3.2.7.Final",
  "org.json"           % "json"          % "20090211",
  "junit"              % "junit"         % "4.10" % "test",
   "com.novocode"      % "junit-interface" % "0.8" % "test->default"
)

seq(com.github.retronym.SbtOneJar.oneJarSettings: _*)

mainClass in (Compile, run) := Some("org.wikimedia.revdiffsearch.Indexer")

mainClass in oneJar := Some("org.wikimedia.revdiffsearch.Indexer")

publishMavenStyle := true

resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"

publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))
