KDBigMemory
===========

Simple MapReduce Kernel Density aggregation using Hadoop and [BigMemoryMAX](http://terracotta.org/products/bigmemory).

The Hadoop job output is an _in-memory_ collection set in BigMemory rather than a file in HDFS.
This project is inpsired from [Hadoop BigMemory](http://blog.terracotta.org/2013/04/02/hadoop-bigmemory-run-elephant-run/).

## Build and package

After [downloading](http://terracotta.org/downloads) BigMemoryMAX, execute the following maven commands to populate you local maven repo with the dependency jars:

    mvn install:install-file \
     -Dfile=./common/lib/bigmemory-4.0.0.jar\
     -DgroupId=org.terracotta\
     -DartifactId=bigmemory\
     -Dversion=4.0.0\
     -Dpackaging=jar\
     -DgeneratePom=true

    mvn install:install-file \
     -Dfile=./apis/toolkit/lib/terracotta-toolkit-runtime-ee-4.0.0.jar\
     -DgroupId=org.terracotta\
     -DartifactId=terracotta-toolkit-runtime-ee\
     -Dversion=4.0.0\
     -Dpackaging=jar\
     -DgeneratePom=true

Build and package:

    $ mvn install

The default maven profile is based on CDH3. You can package the jar for CDH4 as follows:

    $ mvn -P chd4 clean install

## Setup

Check out [this](http://terracotta.org/documentation/4.0/bigmemorymax/get-started/quick-start) quick tutorial to start BigMemory.

The following is my xml configuration file:

    <?xml version="1.0" encoding="UTF-8"?>
    <con:tc-config xmlns:con="http://www.terracotta.org/config">
        <servers>
            <mirror-group group-name="group1">
                <server host="localhost" bind="localhost" name="server1">
                    <offheap>
                        <enabled>false</enabled>
                        <maxDataSize>256M</maxDataSize>
                    </offheap>
                    <tsa-port bind="localhost">9510</tsa-port>
                    <jmx-port bind="localhost">9520</jmx-port>
                    <tsa-group-port bind="localhost">9530</tsa-group-port>
                    <data>terracotta/server1-data</data>
                    <logs>terracotta/server1-logs</logs>
                    <data-backup>terracotta/server1-backups</data-backup>
                </server>
            </mirror-group>
            <update-check>
                <enabled>false</enabled>
            </update-check>
            <garbage-collection>
                <enabled>false</enabled>
            </garbage-collection>
            <restartable enabled="false"/>
        </servers>
        <clients>
            <logs>terracotta/client-logs</logs>
        </clients>
    </con:tc-config>

Start the server as follows:

    $ start-tc-server.sh -f ~/tc-config.xml -n server1

## Run hadoop job

The job reads from HDFS a tab delimted text file where the last two fields in each input line are a latitude and longitude values respectively.
Rather than putting the result back into HDFS, the output is sent to BigMemory in the form of a collection set.
The collection set is composed of items containing the kernel density cell location in a hash format and the number of inputs in that cell.

The job arguments are:

- HDFS input path
- collection name prefix
- bounding box horizontal lower limit
- bounding box vertical lower limit
- bounding box horizontal upper limit
- bounding box vertical upper limit
- cell size

Here is a sample job run:

    $ hadoop jar target/KDBigMemory-1.0-SNAPSHOT-job.jar /user/mraad_admin/InfoUSA/InfoUSA.txt infousa -180 -90 180 90 1

The arguments are saved in a collection map for future retreival and association with the output collection set.

## Dump BigMemory content

    $ mvn -q exec:java -Dexec.mainClass=com.esri.KDDump -Dexec.args="infousa"
