using System;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// Abstract Test case class to run MR in local or cluster mode and in local FS
	/// or DFS.
	/// </summary>
	/// <remarks>
	/// Abstract Test case class to run MR in local or cluster mode and in local FS
	/// or DFS.
	/// The Hadoop instance is started and stopped on each test method.
	/// If using DFS the filesystem is reformated at each start (test method).
	/// Job Configurations should be created using a configuration returned by the
	/// 'createJobConf()' method.
	/// </remarks>
	public abstract class HadoopTestCase : TestCase
	{
		public const int LocalMr = 1;

		public const int ClusterMr = 2;

		public const int LocalFs = 4;

		public const int DfsFs = 8;

		private bool localMR;

		private bool localFS;

		private int taskTrackers;

		private int dataNodes;

		/// <summary>Creates a testcase for local or cluster MR using DFS.</summary>
		/// <remarks>
		/// Creates a testcase for local or cluster MR using DFS.
		/// The DFS will be formatted regardless if there was one or not before in the
		/// given location.
		/// </remarks>
		/// <param name="mrMode">
		/// indicates if the MR should be local (LOCAL_MR) or cluster
		/// (CLUSTER_MR)
		/// </param>
		/// <param name="fsMode">
		/// indicates if the FS should be local (LOCAL_FS) or DFS (DFS_FS)
		/// local FS when using relative PATHs)
		/// </param>
		/// <param name="taskTrackers">number of task trackers to start when using cluster</param>
		/// <param name="dataNodes">number of data nodes to start when using DFS</param>
		/// <exception cref="System.IO.IOException">thrown if the base directory cannot be set.
		/// 	</exception>
		public HadoopTestCase(int mrMode, int fsMode, int taskTrackers, int dataNodes)
		{
			if (mrMode != LocalMr && mrMode != ClusterMr)
			{
				throw new ArgumentException("Invalid MapRed mode, must be LOCAL_MR or CLUSTER_MR"
					);
			}
			if (fsMode != LocalFs && fsMode != DfsFs)
			{
				throw new ArgumentException("Invalid FileSystem mode, must be LOCAL_FS or DFS_FS"
					);
			}
			if (taskTrackers < 1)
			{
				throw new ArgumentException("Invalid taskTrackers value, must be greater than 0");
			}
			if (dataNodes < 1)
			{
				throw new ArgumentException("Invalid dataNodes value, must be greater than 0");
			}
			localMR = (mrMode == LocalMr);
			localFS = (fsMode == LocalFs);
			/*
			JobConf conf = new JobConf();
			fsRoot = conf.get("hadoop.tmp.dir");
			
			if (fsRoot == null) {
			throw new IllegalArgumentException(
			"hadoop.tmp.dir is not defined");
			}
			
			fsRoot = fsRoot.replace(' ', '+') + "/fs";
			
			File file = new File(fsRoot);
			if (!file.exists()) {
			if (!file.mkdirs()) {
			throw new RuntimeException("Could not create FS base path: " + file);
			}
			}
			*/
			this.taskTrackers = taskTrackers;
			this.dataNodes = dataNodes;
		}

		/// <summary>Indicates if the MR is running in local or cluster mode.</summary>
		/// <returns>
		/// returns TRUE if the MR is running locally, FALSE if running in
		/// cluster mode.
		/// </returns>
		public virtual bool IsLocalMR()
		{
			return localMR;
		}

		/// <summary>Indicates if the filesystem is local or DFS.</summary>
		/// <returns>returns TRUE if the filesystem is local, FALSE if it is DFS.</returns>
		public virtual bool IsLocalFS()
		{
			return localFS;
		}

		private MiniDFSCluster dfsCluster = null;

		private MiniMRCluster mrCluster = null;

		private FileSystem fileSystem = null;

		/// <summary>
		/// Creates Hadoop instance based on constructor configuration before
		/// a test case is run.
		/// </summary>
		/// <exception cref="System.Exception"/>
		protected override void SetUp()
		{
			base.SetUp();
			if (localFS)
			{
				fileSystem = FileSystem.GetLocal(new JobConf());
			}
			else
			{
				dfsCluster = new MiniDFSCluster.Builder(new JobConf()).NumDataNodes(dataNodes).Build
					();
				fileSystem = dfsCluster.GetFileSystem();
			}
			if (localMR)
			{
			}
			else
			{
				//noinspection deprecation
				mrCluster = new MiniMRCluster(taskTrackers, fileSystem.GetUri().ToString(), 1);
			}
		}

		/// <summary>
		/// Destroys Hadoop instance based on constructor configuration after
		/// a test case is run.
		/// </summary>
		/// <exception cref="System.Exception"/>
		protected override void TearDown()
		{
			try
			{
				if (mrCluster != null)
				{
					mrCluster.Shutdown();
				}
			}
			catch (Exception ex)
			{
				System.Console.Out.WriteLine(ex);
			}
			try
			{
				if (dfsCluster != null)
				{
					dfsCluster.Shutdown();
				}
			}
			catch (Exception ex)
			{
				System.Console.Out.WriteLine(ex);
			}
			base.TearDown();
		}

		/// <summary>Returns the Filesystem in use.</summary>
		/// <remarks>
		/// Returns the Filesystem in use.
		/// TestCases should use this Filesystem as it
		/// is properly configured with the workingDir for relative PATHs.
		/// </remarks>
		/// <returns>the filesystem used by Hadoop.</returns>
		protected internal virtual FileSystem GetFileSystem()
		{
			return fileSystem;
		}

		/// <summary>
		/// Returns a job configuration preconfigured to run against the Hadoop
		/// managed by the testcase.
		/// </summary>
		/// <returns>configuration that works on the testcase Hadoop instance</returns>
		protected internal virtual JobConf CreateJobConf()
		{
			if (localMR)
			{
				JobConf conf = new JobConf();
				conf.Set(MRConfig.FrameworkName, MRConfig.LocalFrameworkName);
				return conf;
			}
			else
			{
				return mrCluster.CreateJobConf();
			}
		}
	}
}
