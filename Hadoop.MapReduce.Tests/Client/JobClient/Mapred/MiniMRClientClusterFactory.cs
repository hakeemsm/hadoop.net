using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>A MiniMRCluster factory.</summary>
	/// <remarks>
	/// A MiniMRCluster factory. In MR2, it provides a wrapper MiniMRClientCluster
	/// interface around the MiniMRYarnCluster. While in MR1, it provides such
	/// wrapper around MiniMRCluster. This factory should be used in tests to provide
	/// an easy migration of tests across MR1 and MR2.
	/// </remarks>
	public class MiniMRClientClusterFactory
	{
		/// <exception cref="System.IO.IOException"/>
		public static MiniMRClientCluster Create(Type caller, int noOfNMs, Configuration 
			conf)
		{
			return Create(caller, caller.Name, noOfNMs, conf);
		}

		/// <exception cref="System.IO.IOException"/>
		public static MiniMRClientCluster Create(Type caller, string identifier, int noOfNMs
			, Configuration conf)
		{
			if (conf == null)
			{
				conf = new Configuration();
			}
			FileSystem fs = FileSystem.Get(conf);
			Path testRootDir = new Path("target", identifier + "-tmpDir").MakeQualified(fs);
			Path appJar = new Path(testRootDir, "MRAppJar.jar");
			// Copy MRAppJar and make it private.
			Path appMasterJar = new Path(MiniMRYarnCluster.Appjar);
			fs.CopyFromLocalFile(appMasterJar, appJar);
			fs.SetPermission(appJar, new FsPermission("744"));
			Job job = Job.GetInstance(conf);
			job.AddFileToClassPath(appJar);
			Path callerJar = new Path(JarFinder.GetJar(caller));
			Path remoteCallerJar = new Path(testRootDir, callerJar.GetName());
			fs.CopyFromLocalFile(callerJar, remoteCallerJar);
			fs.SetPermission(remoteCallerJar, new FsPermission("744"));
			job.AddFileToClassPath(remoteCallerJar);
			MiniMRYarnCluster miniMRYarnCluster = new MiniMRYarnCluster(identifier, noOfNMs);
			job.GetConfiguration().Set("minimrclientcluster.caller.name", identifier);
			job.GetConfiguration().SetInt("minimrclientcluster.nodemanagers.number", noOfNMs);
			miniMRYarnCluster.Init(job.GetConfiguration());
			miniMRYarnCluster.Start();
			return new MiniMRYarnClusterAdapter(miniMRYarnCluster);
		}
	}
}
