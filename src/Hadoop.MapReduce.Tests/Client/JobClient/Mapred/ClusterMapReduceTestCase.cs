using System.Collections;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>Test case to run a MapReduce job.</summary>
	/// <remarks>
	/// Test case to run a MapReduce job.
	/// <p/>
	/// It runs a 2 node cluster Hadoop with a 2 node DFS.
	/// <p/>
	/// The JobConf to use must be obtained via the creatJobConf() method.
	/// <p/>
	/// It creates a temporary directory -accessible via getTestRootDir()-
	/// for both input and output.
	/// <p/>
	/// The input directory is accesible via getInputDir() and the output
	/// directory via getOutputDir()
	/// <p/>
	/// The DFS filesystem is formated before the testcase starts and after it ends.
	/// </remarks>
	public abstract class ClusterMapReduceTestCase : TestCase
	{
		private MiniDFSCluster dfsCluster = null;

		private MiniMRCluster mrCluster = null;

		/// <summary>Creates Hadoop Cluster and DFS before a test case is run.</summary>
		/// <exception cref="System.Exception"/>
		protected override void SetUp()
		{
			base.SetUp();
			StartCluster(true, null);
		}

		/// <summary>Starts the cluster within a testcase.</summary>
		/// <remarks>
		/// Starts the cluster within a testcase.
		/// <p/>
		/// Note that the cluster is already started when the testcase method
		/// is invoked. This method is useful if as part of the testcase the
		/// cluster has to be shutdown and restarted again.
		/// <p/>
		/// If the cluster is already running this method does nothing.
		/// </remarks>
		/// <param name="reformatDFS">indicates if DFS has to be reformated</param>
		/// <param name="props">configuration properties to inject to the mini cluster</param>
		/// <exception cref="System.Exception">if the cluster could not be started</exception>
		protected internal virtual void StartCluster(bool reformatDFS, Properties props)
		{
			lock (this)
			{
				if (dfsCluster == null)
				{
					JobConf conf = new JobConf();
					if (props != null)
					{
						foreach (DictionaryEntry entry in props)
						{
							conf.Set((string)entry.Key, (string)entry.Value);
						}
					}
					dfsCluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Format(reformatDFS)
						.Racks(null).Build();
					ClusterMapReduceTestCase.ConfigurableMiniMRCluster.SetConfiguration(props);
					//noinspection deprecation
					mrCluster = new ClusterMapReduceTestCase.ConfigurableMiniMRCluster(2, GetFileSystem
						().GetUri().ToString(), 1, conf);
				}
			}
		}

		private class ConfigurableMiniMRCluster : MiniMRCluster
		{
			private static Properties config;

			public static void SetConfiguration(Properties props)
			{
				config = props;
			}

			/// <exception cref="System.Exception"/>
			public ConfigurableMiniMRCluster(int numTaskTrackers, string namenode, int numDir
				, JobConf conf)
				: base(0, 0, numTaskTrackers, namenode, numDir, null, null, null, conf)
			{
			}

			public override JobConf CreateJobConf()
			{
				JobConf conf = base.CreateJobConf();
				if (config != null)
				{
					foreach (DictionaryEntry entry in config)
					{
						conf.Set((string)entry.Key, (string)entry.Value);
					}
				}
				return conf;
			}
		}

		/// <summary>Stops the cluster within a testcase.</summary>
		/// <remarks>
		/// Stops the cluster within a testcase.
		/// <p/>
		/// Note that the cluster is already started when the testcase method
		/// is invoked. This method is useful if as part of the testcase the
		/// cluster has to be shutdown.
		/// <p/>
		/// If the cluster is already stopped this method does nothing.
		/// </remarks>
		/// <exception cref="System.Exception">if the cluster could not be stopped</exception>
		protected internal virtual void StopCluster()
		{
			if (mrCluster != null)
			{
				mrCluster.Shutdown();
				mrCluster = null;
			}
			if (dfsCluster != null)
			{
				dfsCluster.Shutdown();
				dfsCluster = null;
			}
		}

		/// <summary>Destroys Hadoop Cluster and DFS after a test case is run.</summary>
		/// <exception cref="System.Exception"/>
		protected override void TearDown()
		{
			StopCluster();
			base.TearDown();
		}

		/// <summary>
		/// Returns a preconfigured Filesystem instance for test cases to read and
		/// write files to it.
		/// </summary>
		/// <remarks>
		/// Returns a preconfigured Filesystem instance for test cases to read and
		/// write files to it.
		/// <p/>
		/// TestCases should use this Filesystem instance.
		/// </remarks>
		/// <returns>the filesystem used by Hadoop.</returns>
		/// <exception cref="System.IO.IOException"></exception>
		protected internal virtual FileSystem GetFileSystem()
		{
			return dfsCluster.GetFileSystem();
		}

		protected internal virtual MiniMRCluster GetMRCluster()
		{
			return mrCluster;
		}

		/// <summary>Returns the path to the root directory for the testcase.</summary>
		/// <returns>path to the root directory for the testcase.</returns>
		protected internal virtual Path GetTestRootDir()
		{
			return new Path("x").GetParent();
		}

		/// <summary>Returns a path to the input directory for the testcase.</summary>
		/// <returns>path to the input directory for the tescase.</returns>
		protected internal virtual Path GetInputDir()
		{
			return new Path("target/input");
		}

		/// <summary>Returns a path to the output directory for the testcase.</summary>
		/// <returns>path to the output directory for the tescase.</returns>
		protected internal virtual Path GetOutputDir()
		{
			return new Path("target/output");
		}

		/// <summary>
		/// Returns a job configuration preconfigured to run against the Hadoop
		/// managed by the testcase.
		/// </summary>
		/// <returns>configuration that works on the testcase Hadoop instance</returns>
		protected internal virtual JobConf CreateJobConf()
		{
			return mrCluster.CreateJobConf();
		}
	}
}
