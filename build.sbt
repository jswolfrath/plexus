name := "plexus"
version := "1.0"

scalaVersion := "2.12.15"

libraryDependencies += "com.typesafe" % "config" % "1.4.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.0" % "provided"
libraryDependencies += "org.scalanlp" %% "breeze" % "0.13"
libraryDependencies += "org.apache.commons" % "commons-math3" % "3.3"
libraryDependencies += "org.apache.kafka" %% "kafka" % "3.2.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.2.0"
libraryDependencies += "org.slf4j" % "slf4j-simple" % "2.0.0" % Test
libraryDependencies += "com.hierynomus" % "sshj" % "0.34.0"
libraryDependencies += "net.i2p.crypto" % "eddsa" % "0.3.0"
//libraryDependencies += "org.bouncycastle" % "bcpkix-jdk15on" % "1.67"
//libraryDependencies += "org.bouncycastle" % "bcprov-jdk15on" % "1.67"
libraryDependencies += "commons-net" % "commons-net" % "3.9.0"

scalacOptions := Seq("-unchecked", "-deprecation")

// Just tolerate the warning for now
//mainClass in (Compile, packageBin) := Some("ClientMain")
