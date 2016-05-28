using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2
{
	public class TestMRAMWithNonNormalizedCapabilities
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestMRAMWithNonNormalizedCapabilities
			));

		private static FileSystem localFs;

		protected internal static MiniMRYarnCluster mrCluster = null;

		private static Configuration conf = new Configuration();

		static TestMRAMWithNonNormalizedCapabilities()
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

		private static Path TestRootDir = new Path("target", typeof(TestMRAMWithNonNormalizedCapabilities
			).FullName + "-tmpDir").MakeQualified(localFs.GetUri(), localFs.GetWorkingDirectory
			());

		internal static Path AppJar = new Path(TestRootDir, "MRAppJar.jar");

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			if (!(new FilePath(MiniMRYarnCluster.Appjar)).Exists())
			{
				Log.Info("MRAppJar " + MiniMRYarnCluster.Appjar + " not found. Not running test."
					);
				return;
			}
			if (mrCluster == null)
			{
				mrCluster = new MiniMRYarnCluster(GetType().Name);
				mrCluster.Init(new Configuration());
				mrCluster.Start();
			}
			// Copy MRAppJar and make it private. TODO: FIXME. This is a hack to
			// workaround the absent public discache.
			localFs.CopyFromLocalFile(new Path(MiniMRYarnCluster.Appjar), AppJar);
			localFs.SetPermission(AppJar, new FsPermission("700"));
		}

		/// <summary>
		/// To ensure nothing broken after we removed normalization
		/// from the MRAM side
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobWithNonNormalizedCapabilities()
		{
			if (!(new FilePath(MiniMRYarnCluster.Appjar)).Exists())
			{
				Log.Info("MRAppJar " + MiniMRYarnCluster.Appjar + " not found. Not running test."
					);
				return;
			}
			JobConf jobConf = new JobConf(mrCluster.GetConfig());
			jobConf.SetInt("mapreduce.map.memory.mb", 700);
			jobConf.SetInt("mapred.reduce.memory.mb", 1500);
			SleepJob sleepJob = new SleepJob();
			sleepJob.SetConf(jobConf);
			Job job = sleepJob.CreateJob(3, 2, 1000, 1, 500, 1);
			job.SetJarByClass(typeof(SleepJob));
			job.AddFileToClassPath(AppJar);
			// The AppMaster jar itself.
			job.Submit();
			bool completed = job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue("Job should be completed", completed);
			NUnit.Framework.Assert.AreEqual("Job should be finished successfully", JobStatus.State
				.Succeeded, job.GetJobState());
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (!(new FilePath(MiniMRYarnCluster.Appjar)).Exists())
			{
				Log.Info("MRAppJar " + MiniMRYarnCluster.Appjar + " not found. Not running test."
					);
				return;
			}
			if (mrCluster != null)
			{
				mrCluster.Stop();
			}
		}
	}
}
