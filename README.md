KDBigMemory
===========

Simple MapReduce Kernel Density aggregation using Hadoop and with [BigMemory](http://terracotta.org/products/bigmemory).

The Hadoop job output is a collection set in BigMemory rather than a file in HDFS.
The job map function expects each input text line to be tab separated and the last two fields content is a latitude value and a longitude value.

Build and package
-----------------

    $ mvn install

Run hadoop job
-----------------
The job expects two arguments, the first argument is an HDFS input path and the second argument is the BigMemory set and map prefix name.

Here is a sample execution:

    $ hadoop jar target/KDBigMemory-1.0-SNAPSHOT-job.jar /user/mraad_admin/InfoUSA/InfoUSA.txt infousa

The job will create a collection map that contains the bounding box of the kernel density and the number of units per kernel density cell.

Dump BigMemory content
----------------------

    $ mvn -q exec:java -Dexec.mainClass=com.esri.KDDump -Dexec.args="infousa"
