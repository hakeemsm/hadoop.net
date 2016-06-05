<b>Disclaimer: The bits are super raw and the solution doesn't even compile, much less run. This is a WIP</b>
<p>
This is a .NET port of <a href="https://hadoop.apache.org/index.html">The Apache Hadoop project v2.7.2</a> It is targeted for .NET 4.6.1 using C# 6 but the ultimate goal is to use .NET core for xplat. Please note that this is only the core Hadoop platform and as such it doesn't include the other components in the ecosystem such as Hive, Pig, Oozie, Flume etc. Below is a plan of action to get it up & running
<p>
1. Hadoop.Common has the core code for IO, HDFS, Yarn and Mapreduce. Clean this up first and make all tests pass
<p>
2. Hadoop.HDFS is the filesystem port. This needs to be taken up next
<p>
3. Get Hadoop.Yarn working followed by Hadoop.MapReduce
<p>
4. Work on porting ZooKeeper so it can be used as the orchestrator
<p>
5. Work on Hadoop.Tools to get the framework running on aws, openstack etc