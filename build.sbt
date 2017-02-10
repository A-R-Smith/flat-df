name := "flat-dataframe"

version := "1.0"

scalaVersion := "2.11.7"

crossScalaVersions := Seq("2.10.4", "2.11.7")

excludeFilter in unmanagedSources := {
	CrossVersion.partialVersion(scalaVersion.value) match {
		case Some((2, 11))  => ""
		case Some((2, 10))  => "FlatDataFrameLoader.scala" 
  }
}


libraryDependencies := {
	CrossVersion.partialVersion(scalaVersion.value) match {
	case Some((2, 11))  =>
	  libraryDependencies.value ++ Seq(
			"org.apache.spark" %% "spark-core" % "2.1.0",
			"org.apache.spark" %% "spark-hive" % "2.1.0",
			"org.apache.spark" %% "spark-catalyst" % "2.1.0",
			"org.apache.spark" %% "spark-sql" % "2.1.0")	
	case Some((2, 10))  =>
	  libraryDependencies.value ++ Seq(
			"org.apache.spark" %% "spark-core" % "1.5.1",
			"org.apache.spark" %% "spark-hive" % "1.5.1",
			"org.apache.spark" %% "spark-catalyst" % "1.5.1",
			"org.apache.spark" %% "spark-sql" % "1.5.1")
  }
}


