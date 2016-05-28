using System.IO;
using System.Text;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred.Lib;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Server.Jobtracker;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>This test checks whether the task caches are created and used properly.</summary>
	public class TestMultipleLevelCaching : TestCase
	{
		private const int MaxLevel = 5;

		internal readonly Path inDir = new Path("/cachetesting");

		internal readonly Path outputPath = new Path("/output");

		/// <summary>
		/// Returns a string representing a rack with level + 1 nodes in the topology
		/// for the rack.
		/// </summary>
		/// <remarks>
		/// Returns a string representing a rack with level + 1 nodes in the topology
		/// for the rack.
		/// For id = 2, level = 2 we get /a/b2/c2
		/// id = 1, level = 3 we get /a/b1/c1/d1
		/// NOTE There should always be one shared node i.e /a
		/// </remarks>
		/// <param name="id">Unique Id for the rack</param>
		/// <param name="level">The level in the topology where the separation starts</param>
		private static string GetRack(int id, int level)
		{
			StringBuilder rack = new StringBuilder();
			char alpha = 'a';
			int length = level + 1;
			while (length > level)
			{
				rack.Append("/");
				rack.Append(alpha);
				++alpha;
				--length;
			}
			while (length > 0)
			{
				rack.Append("/");
				rack.Append(alpha);
				rack.Append(id);
				++alpha;
				--length;
			}
			return rack.ToString();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMultiLevelCaching()
		{
			for (int i = 1; i <= MaxLevel; ++i)
			{
				TestCachingAtLevel(i);
			}
		}

		/// <exception cref="System.Exception"/>
		private void TestCachingAtLevel(int level)
		{
			string namenode = null;
			MiniDFSCluster dfs = null;
			MiniMRCluster mr = null;
			FileSystem fileSys = null;
			string testName = "TestMultiLevelCaching";
			try
			{
				int taskTrackers = 1;
				// generate the racks
				// use rack1 for data node
				string rack1 = GetRack(0, level);
				// use rack2 for task tracker
				string rack2 = GetRack(1, level);
				Configuration conf = new Configuration();
				// Run a datanode on host1 under /a/b/c/..../d1/e1/f1
				dfs = new MiniDFSCluster.Builder(conf).Racks(new string[] { rack1 }).Hosts(new string
					[] { "host1.com" }).Build();
				dfs.WaitActive();
				fileSys = dfs.GetFileSystem();
				if (!fileSys.Mkdirs(inDir))
				{
					throw new IOException("Mkdirs failed to create " + inDir.ToString());
				}
				UtilsForTests.WriteFile(dfs.GetNameNode(), conf, new Path(inDir + "/file"), (short
					)1);
				namenode = (dfs.GetFileSystem()).GetUri().GetHost() + ":" + (dfs.GetFileSystem())
					.GetUri().GetPort();
				// Run a job with the (only)tasktracker on host2 under diff topology
				// e.g /a/b/c/..../d2/e2/f2. 
				JobConf jc = new JobConf();
				// cache-level = level (unshared levels) + 1(topmost shared node i.e /a) 
				//               + 1 (for host)
				jc.SetInt(JTConfig.JtTaskcacheLevels, level + 2);
				mr = new MiniMRCluster(taskTrackers, namenode, 1, new string[] { rack2 }, new string
					[] { "host2.com" }, jc);
				/* The job is configured with 1 map for one (non-splittable) file.
				* Since the datanode is running under different subtree, there is no
				* node-level data locality but there should be topological locality.
				*/
				LaunchJobAndTestCounters(testName, mr, fileSys, inDir, outputPath, 1, 1, 0, 0);
				mr.Shutdown();
			}
			finally
			{
				if (null != fileSys)
				{
					// inDir, outputPath only exist if fileSys is valid.
					fileSys.Delete(inDir, true);
					fileSys.Delete(outputPath, true);
				}
				if (dfs != null)
				{
					dfs.Shutdown();
				}
			}
		}

		/// <summary>Launches a MR job and tests the job counters against the expected values.
		/// 	</summary>
		/// <param name="testName">The name for the job</param>
		/// <param name="mr">The MR cluster</param>
		/// <param name="fileSys">The FileSystem</param>
		/// <param name="in">Input path</param>
		/// <param name="out">Output path</param>
		/// <param name="numMaps">Number of maps</param>
		/// <param name="otherLocalMaps">Expected value of other local maps</param>
		/// <param name="datalocalMaps">Expected value of data(node) local maps</param>
		/// <param name="racklocalMaps">Expected value of rack local maps</param>
		/// <exception cref="System.IO.IOException"/>
		internal static void LaunchJobAndTestCounters(string jobName, MiniMRCluster mr, FileSystem
			 fileSys, Path @in, Path @out, int numMaps, int otherLocalMaps, int dataLocalMaps
			, int rackLocalMaps)
		{
			JobConf jobConf = mr.CreateJobConf();
			if (fileSys.Exists(@out))
			{
				fileSys.Delete(@out, true);
			}
			RunningJob job = LaunchJob(jobConf, @in, @out, numMaps, jobName);
			Counters counters = job.GetCounters();
			NUnit.Framework.Assert.AreEqual("Number of local maps", counters.GetCounter(JobCounter
				.OtherLocalMaps), otherLocalMaps);
			NUnit.Framework.Assert.AreEqual("Number of Data-local maps", counters.GetCounter(
				JobCounter.DataLocalMaps), dataLocalMaps);
			NUnit.Framework.Assert.AreEqual("Number of Rack-local maps", counters.GetCounter(
				JobCounter.RackLocalMaps), rackLocalMaps);
			mr.WaitUntilIdle();
			mr.Shutdown();
		}

		/// <exception cref="System.IO.IOException"/>
		internal static RunningJob LaunchJob(JobConf jobConf, Path inDir, Path outputPath
			, int numMaps, string jobName)
		{
			jobConf.SetJobName(jobName);
			jobConf.SetInputFormat(typeof(SortValidator.RecordStatsChecker.NonSplitableSequenceFileInputFormat
				));
			jobConf.SetOutputFormat(typeof(SequenceFileOutputFormat));
			FileInputFormat.SetInputPaths(jobConf, inDir);
			FileOutputFormat.SetOutputPath(jobConf, outputPath);
			jobConf.SetMapperClass(typeof(IdentityMapper));
			jobConf.SetReducerClass(typeof(IdentityReducer));
			jobConf.SetOutputKeyClass(typeof(BytesWritable));
			jobConf.SetOutputValueClass(typeof(BytesWritable));
			jobConf.SetNumMapTasks(numMaps);
			jobConf.SetNumReduceTasks(0);
			jobConf.SetJar("build/test/mapred/testjar/testjob.jar");
			return JobClient.RunJob(jobConf);
		}
	}
}
