using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Client;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestHdfsAdmin
	{
		private static readonly Path TestPath = new Path("/test");

		private readonly Configuration conf = new Configuration();

		private MiniDFSCluster cluster;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void SetUpCluster()
		{
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
		}

		[TearDown]
		public virtual void ShutDownCluster()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Test that we can set and clear quotas via
		/// <see cref="Org.Apache.Hadoop.Hdfs.Client.HdfsAdmin"/>
		/// .
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHdfsAdminSetQuota()
		{
			HdfsAdmin dfsAdmin = new HdfsAdmin(FileSystem.GetDefaultUri(conf), conf);
			FileSystem fs = null;
			try
			{
				fs = FileSystem.Get(conf);
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(TestPath));
				NUnit.Framework.Assert.AreEqual(-1, fs.GetContentSummary(TestPath).GetQuota());
				NUnit.Framework.Assert.AreEqual(-1, fs.GetContentSummary(TestPath).GetSpaceQuota(
					));
				dfsAdmin.SetSpaceQuota(TestPath, 10);
				NUnit.Framework.Assert.AreEqual(-1, fs.GetContentSummary(TestPath).GetQuota());
				NUnit.Framework.Assert.AreEqual(10, fs.GetContentSummary(TestPath).GetSpaceQuota(
					));
				dfsAdmin.SetQuota(TestPath, 10);
				NUnit.Framework.Assert.AreEqual(10, fs.GetContentSummary(TestPath).GetQuota());
				NUnit.Framework.Assert.AreEqual(10, fs.GetContentSummary(TestPath).GetSpaceQuota(
					));
				dfsAdmin.ClearSpaceQuota(TestPath);
				NUnit.Framework.Assert.AreEqual(10, fs.GetContentSummary(TestPath).GetQuota());
				NUnit.Framework.Assert.AreEqual(-1, fs.GetContentSummary(TestPath).GetSpaceQuota(
					));
				dfsAdmin.ClearQuota(TestPath);
				NUnit.Framework.Assert.AreEqual(-1, fs.GetContentSummary(TestPath).GetQuota());
				NUnit.Framework.Assert.AreEqual(-1, fs.GetContentSummary(TestPath).GetSpaceQuota(
					));
			}
			finally
			{
				if (fs != null)
				{
					fs.Close();
				}
			}
		}

		/// <summary>Make sure that a non-HDFS URI throws a helpful error.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		public virtual void TestHdfsAdminWithBadUri()
		{
			new HdfsAdmin(new URI("file:///bad-scheme"), conf);
		}
	}
}
