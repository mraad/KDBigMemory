KDBigMemory
===========

Kernel Density using Hadoop with [BigMemory](http://terracotta.org/products/bigmemory)

Build and package
-----------------

    $ mvn install

Run hadoop job
-----------------

    $ hadoop jar target/KDBigMemory-1.0-SNAPSHOT-job.jar /user/mraad_admin/InfoUSA/InfoUSA.txt infousa

Dump BigMemory content
----------------------

    $ mvn -q exec:java -Dexec.mainClass=com.esri.KDDump -Dexec.args="infousa"
