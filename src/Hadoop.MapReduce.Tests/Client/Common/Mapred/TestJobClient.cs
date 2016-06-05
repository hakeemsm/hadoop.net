using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Server.Jobtracker;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestJobClient
	{
		internal static readonly string TestDir = new FilePath("target", typeof(TestJobClient
			).Name).GetAbsolutePath();

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			FileUtil.FullyDelete(new FilePath(TestDir));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetClusterStatusWithLocalJobRunner()
		{
			Configuration conf = new Configuration();
			conf.Set(JTConfig.JtIpcAddress, MRConfig.LocalFrameworkName);
			conf.Set(MRConfig.FrameworkName, MRConfig.LocalFrameworkName);
			JobClient client = new JobClient(conf);
			ClusterStatus clusterStatus = client.GetClusterStatus(true);
			ICollection<string> activeTrackerNames = clusterStatus.GetActiveTrackerNames();
			NUnit.Framework.Assert.AreEqual(0, activeTrackerNames.Count);
			int blacklistedTrackers = clusterStatus.GetBlacklistedTrackers();
			NUnit.Framework.Assert.AreEqual(0, blacklistedTrackers);
			ICollection<ClusterStatus.BlackListInfo> blackListedTrackersInfo = clusterStatus.
				GetBlackListedTrackersInfo();
			NUnit.Framework.Assert.AreEqual(0, blackListedTrackersInfo.Count);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestIsJobDirValid()
		{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.GetLocal(conf);
			Path testDir = new Path(TestDir);
			fs.Mkdirs(testDir);
			NUnit.Framework.Assert.IsFalse(JobClient.IsJobDirValid(testDir, fs));
			Path jobconf = new Path(testDir, "job.xml");
			Path jobsplit = new Path(testDir, "job.split");
			fs.Create(jobconf);
			fs.Create(jobsplit);
			NUnit.Framework.Assert.IsTrue(JobClient.IsJobDirValid(testDir, fs));
			fs.Delete(jobconf, true);
			fs.Delete(jobsplit, true);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestGetStagingAreaDir()
		{
			Configuration conf = new Configuration();
			JobClient client = new JobClient(conf);
			NUnit.Framework.Assert.IsTrue("Mismatch in paths", client.GetClusterHandle().GetStagingAreaDir
				().ToString().Equals(client.GetStagingAreaDir().ToString()));
		}
	}
}
