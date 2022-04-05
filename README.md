impala
======

Interactive shell for Hadoop

* Maven:

~~~~~ xml
<dependency>
  <groupId>org.springframework.data.hadoop.impala</groupId>
  <artifactId>plugin-hdfs</artifactId>
  <version>${version}</version>
</dependency>
<dependency>
  <groupId>org.springframework.data.hadoop.impala</groupId>
  <artifactId>plugin-mapreduce</artifactId>
  <version>${version}</version>
</dependency> 

<build>
	<plugins>
		<plugin>
			<groupId>org.apache.maven.plugins</groupId>
			<artifactId>maven-dependency-plugin</artifactId>
			<executions>
				<execution>
					<id>copy-dependencies</id>
					<phase>prepare-package</phase>
					<goals>
						<goal>copy-dependencies</goal>
					</goals>
					<configuration>
						<outputDirectory>${project.build.directory}/dependency</outputDirectory>
						<overWriteReleases>true</overWriteReleases>
						<overWriteSnapshots>true</overWriteSnapshots>
						<overWriteIfNewer>true</overWriteIfNewer>
					</configuration>
				</execution>
			</executions>
		</plugin>
		<plugin>
			<groupId>org.apache.maven.plugins</groupId>
			<artifactId>maven-jar-plugin</artifactId>
			<configuration>
				<archive>
					<manifest>
						<addClasspath>true</addClasspath>
						<useUniqueVersions>false</useUniqueVersions>
						<classpathPrefix>dependency/</classpathPrefix>
						<mainClass>${jar.mainclass}</mainClass>
					</manifest>
					<manifestEntries>
						<version>${project.version}</version>
					</manifestEntries>
				</archive>
			</configuration>
		</plugin>
	</plugins>
</build>

<!-- used for nightly builds -->
<repository>
  <id>spring-maven-snapshot</id>
  <snapshots><enabled>true</enabled></snapshots>
  <name>Springframework Maven SNAPSHOT Repository</name>
  <url>https://maven.springframework.org/snapshot</url>
</repository> 

<!-- used for milestone/rc releases -->
<repository>
  <id>spring-maven-milestone</id>
  <name>Springframework Maven Milestone Repository</name>
  <url>https://maven.springframework.org/milestone</url>
</repository>  
~~~~~

* Gradle: 

~~~~~ groovy
repositories {
   mavenRepo name: "spring-snapshot", urls: "https://maven.springframework.org/snapshot"
}

dependencies {
   compile "org.springframework.data.hadoop.impala:plugin-hdfs:${version}"
   compile "org.springframework.data.hadoop.impala:plugin-mapreduce:${version}"
}


task copyDependency(type: Copy) {
	into "$buildDir/libs/dependency"
	from configurations.runtime	
}

project.ext.springShellJar = configurations.runtime.find { file -> file.name.contains("spring-shell")}
//println("spring shell name is:" + project.ext.springShellJar.name)

project.ext.manifestClasspath = "dependency/" + springShellJar.name + " "
project.ext.manifestClasspath = project.ext.manifestClasspath + configurations.runtime.collect{ File file -> "dependency/"+file.name}.join(' ')

jar {
	baseName "simple-cli"
	manifest {
		attributes 'Main-Class' : 'org.springframework.shell.Bootstrap', "Class-Path" : project.ext.manifestClasspath
	}	
}


eclipse {
	project {
		name = "simple-cli"
	}
}


defaultTasks 'clean', 'build', 'copyDependency'
~~~~~
current version is 1.0.0.BUILD-SNAPSHOT

     
# Running Example

    cd samples/simple
	../../gradlew
	java -jar build/libs/build/libs/simple-cli-1.0.0.BUILD-SNAPSHOT.jar

	
	