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
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Filecache;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2
{
	public class TestMROldApiJobs
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestMROldApiJobs));

		protected internal static MiniMRYarnCluster mrCluster;

		private static Configuration conf = new Configuration();

		private static FileSystem localFs;

		static TestMROldApiJobs()
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
				mrCluster = new MiniMRYarnCluster(typeof(TestMROldApiJobs).FullName);
				mrCluster.Init(new Configuration());
				mrCluster.Start();
			}
			// TestMRJobs is for testing non-uberized operation only; see TestUberAM
			// for corresponding uberized tests.
			mrCluster.GetConfig().SetBoolean(MRJobConfig.JobUbertaskEnable, false);
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

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		[NUnit.Framework.Test]
		public virtual void TestJobSucceed()
		{
			Log.Info("\n\n\nStarting testJobSucceed().");
			if (!(new FilePath(MiniMRYarnCluster.Appjar)).Exists())
			{
				Log.Info("MRAppJar " + MiniMRYarnCluster.Appjar + " not found. Not running test."
					);
				return;
			}
			JobConf conf = new JobConf(mrCluster.GetConfig());
			Path @in = new Path(mrCluster.GetTestWorkDir().GetAbsolutePath(), "in");
			Path @out = new Path(mrCluster.GetTestWorkDir().GetAbsolutePath(), "out");
			RunJobSucceed(conf, @in, @out);
			FileSystem fs = FileSystem.Get(conf);
			NUnit.Framework.Assert.IsTrue(fs.Exists(new Path(@out, CustomOutputCommitter.JobSetupFileName
				)));
			NUnit.Framework.Assert.IsFalse(fs.Exists(new Path(@out, CustomOutputCommitter.JobAbortFileName
				)));
			NUnit.Framework.Assert.IsTrue(fs.Exists(new Path(@out, CustomOutputCommitter.JobCommitFileName
				)));
			NUnit.Framework.Assert.IsTrue(fs.Exists(new Path(@out, CustomOutputCommitter.TaskSetupFileName
				)));
			NUnit.Framework.Assert.IsFalse(fs.Exists(new Path(@out, CustomOutputCommitter.TaskAbortFileName
				)));
			NUnit.Framework.Assert.IsTrue(fs.Exists(new Path(@out, CustomOutputCommitter.TaskCommitFileName
				)));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		[NUnit.Framework.Test]
		public virtual void TestJobFail()
		{
			Log.Info("\n\n\nStarting testJobFail().");
			if (!(new FilePath(MiniMRYarnCluster.Appjar)).Exists())
			{
				Log.Info("MRAppJar " + MiniMRYarnCluster.Appjar + " not found. Not running test."
					);
				return;
			}
			JobConf conf = new JobConf(mrCluster.GetConfig());
			Path @in = new Path(mrCluster.GetTestWorkDir().GetAbsolutePath(), "fail-in");
			Path @out = new Path(mrCluster.GetTestWorkDir().GetAbsolutePath(), "fail-out");
			RunJobFail(conf, @in, @out);
			FileSystem fs = FileSystem.Get(conf);
			NUnit.Framework.Assert.IsTrue(fs.Exists(new Path(@out, CustomOutputCommitter.JobSetupFileName
				)));
			NUnit.Framework.Assert.IsTrue(fs.Exists(new Path(@out, CustomOutputCommitter.JobAbortFileName
				)));
			NUnit.Framework.Assert.IsFalse(fs.Exists(new Path(@out, CustomOutputCommitter.JobCommitFileName
				)));
			NUnit.Framework.Assert.IsTrue(fs.Exists(new Path(@out, CustomOutputCommitter.TaskSetupFileName
				)));
			NUnit.Framework.Assert.IsTrue(fs.Exists(new Path(@out, CustomOutputCommitter.TaskAbortFileName
				)));
			NUnit.Framework.Assert.IsFalse(fs.Exists(new Path(@out, CustomOutputCommitter.TaskCommitFileName
				)));
		}

		//Run a job that will be failed and wait until it completes
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public static void RunJobFail(JobConf conf, Path inDir, Path outDir)
		{
			conf.SetJobName("test-job-fail");
			conf.SetMapperClass(typeof(FailMapper));
			conf.SetJarByClass(typeof(FailMapper));
			conf.SetReducerClass(typeof(IdentityReducer));
			conf.SetMaxMapAttempts(1);
			bool success = RunJob(conf, inDir, outDir, 1, 0);
			NUnit.Framework.Assert.IsFalse("Job expected to fail succeeded", success);
		}

		//Run a job that will be succeeded and wait until it completes
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public static void RunJobSucceed(JobConf conf, Path inDir, Path outDir)
		{
			conf.SetJobName("test-job-succeed");
			conf.SetMapperClass(typeof(IdentityMapper));
			//conf.setJar(new File(MiniMRYarnCluster.APPJAR).getAbsolutePath());
			conf.SetReducerClass(typeof(IdentityReducer));
			bool success = RunJob(conf, inDir, outDir, 1, 1);
			NUnit.Framework.Assert.IsTrue("Job expected to succeed failed", success);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		internal static bool RunJob(JobConf conf, Path inDir, Path outDir, int numMaps, int
			 numReds)
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
			DistributedCache.AddFileToClassPath(TestMRJobs.AppJar, conf, fs);
			conf.SetOutputCommitter(typeof(CustomOutputCommitter));
			conf.SetInputFormat(typeof(TextInputFormat));
			conf.SetOutputKeyClass(typeof(LongWritable));
			conf.SetOutputValueClass(typeof(Text));
			FileInputFormat.SetInputPaths(conf, inDir);
			FileOutputFormat.SetOutputPath(conf, outDir);
			conf.SetNumMapTasks(numMaps);
			conf.SetNumReduceTasks(numReds);
			JobClient jobClient = new JobClient(conf);
			RunningJob job = jobClient.SubmitJob(conf);
			return jobClient.MonitorAndPrintJob(conf, job);
		}
	}
}
