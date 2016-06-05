using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapred.Lib;
using Org.Apache.Hadoop.Mapreduce.Filecache;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2
{
	public class TestMRAppWithCombiner
	{
		protected internal static MiniMRYarnCluster mrCluster;

		private static Configuration conf = new Configuration();

		private static FileSystem localFs;

		private static readonly Log Log = LogFactory.GetLog(typeof(TestMRAppWithCombiner)
			);

		static TestMRAppWithCombiner()
		{
			try
			{
				localFs = FileSystem.GetLocal(conf);
			}
			catch (IOException io)
			{
				throw new RuntimeException("problem getting local fs", io);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void Setup()
		{
			if (!(new FilePath(MiniMRYarnCluster.Appjar)).Exists())
			{
				Log.Info("MRAppJar " + MiniMRYarnCluster.Appjar + " not found. Not running test."
					);
				return;
			}
			if (mrCluster == null)
			{
				mrCluster = new MiniMRYarnCluster(typeof(TestMRJobs).FullName, 3);
				Configuration conf = new Configuration();
				mrCluster.Init(conf);
				mrCluster.Start();
			}
			// Copy MRAppJar and make it private. TODO: FIXME. This is a hack to
			// workaround the absent public discache.
			localFs.CopyFromLocalFile(new Path(MiniMRYarnCluster.Appjar), TestMRJobs.AppJar);
			localFs.SetPermission(TestMRJobs.AppJar, new FsPermission("700"));
		}

		[AfterClass]
		public static void TearDown()
		{
			if (mrCluster != null)
			{
				mrCluster.Stop();
				mrCluster = null;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCombinerShouldUpdateTheReporter()
		{
			JobConf conf = new JobConf(mrCluster.GetConfig());
			int numMaps = 5;
			int numReds = 2;
			Path @in = new Path(mrCluster.GetTestWorkDir().GetAbsolutePath(), "testCombinerShouldUpdateTheReporter-in"
				);
			Path @out = new Path(mrCluster.GetTestWorkDir().GetAbsolutePath(), "testCombinerShouldUpdateTheReporter-out"
				);
			CreateInputOutPutFolder(@in, @out, numMaps);
			conf.SetJobName("test-job-with-combiner");
			conf.SetMapperClass(typeof(IdentityMapper));
			conf.SetCombinerClass(typeof(TestMRAppWithCombiner.MyCombinerToCheckReporter));
			//conf.setJarByClass(MyCombinerToCheckReporter.class);
			conf.SetReducerClass(typeof(IdentityReducer));
			DistributedCache.AddFileToClassPath(TestMRJobs.AppJar, conf);
			conf.SetOutputCommitter(typeof(CustomOutputCommitter));
			conf.SetInputFormat(typeof(TextInputFormat));
			conf.SetOutputKeyClass(typeof(LongWritable));
			conf.SetOutputValueClass(typeof(Text));
			FileInputFormat.SetInputPaths(conf, @in);
			FileOutputFormat.SetOutputPath(conf, @out);
			conf.SetNumMapTasks(numMaps);
			conf.SetNumReduceTasks(numReds);
			RunJob(conf);
		}

		/// <exception cref="System.Exception"/>
		internal static void CreateInputOutPutFolder(Path inDir, Path outDir, int numMaps
			)
		{
			FileSystem fs = FileSystem.Get(conf);
			if (fs.Exists(outDir))
			{
				fs.Delete(outDir, true);
			}
			if (!fs.Exists(inDir))
			{
				fs.Mkdirs(inDir);
			}
			string input = "The quick brown fox\n" + "has many silly\n" + "red fox sox\n";
			for (int i = 0; i < numMaps; ++i)
			{
				DataOutputStream file = fs.Create(new Path(inDir, "part-" + i));
				file.WriteBytes(input);
				file.Close();
			}
		}

		/// <exception cref="System.Exception"/>
		internal static bool RunJob(JobConf conf)
		{
			JobClient jobClient = new JobClient(conf);
			RunningJob job = jobClient.SubmitJob(conf);
			return jobClient.MonitorAndPrintJob(conf, job);
		}

		internal class MyCombinerToCheckReporter<K, V> : IdentityReducer<K, V>
		{
			/// <exception cref="System.IO.IOException"/>
			public override void Reduce(K key, IEnumerator<V> values, OutputCollector<K, V> output
				, Reporter reporter)
			{
				if (Reporter.Null == reporter)
				{
					NUnit.Framework.Assert.Fail("A valid Reporter should have been used but, Reporter.NULL is used"
						);
				}
			}

			internal MyCombinerToCheckReporter(TestMRAppWithCombiner _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestMRAppWithCombiner _enclosing;
		}
	}
}
