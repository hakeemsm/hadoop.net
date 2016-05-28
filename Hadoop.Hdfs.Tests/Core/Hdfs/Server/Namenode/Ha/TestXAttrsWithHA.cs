using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	/// <summary>Tests interaction of XAttrs with HA failover.</summary>
	public class TestXAttrsWithHA
	{
		private static readonly Path path = new Path("/file");

		protected internal const string name1 = "user.a1";

		protected internal static readonly byte[] value1 = new byte[] { unchecked((int)(0x31
			)), unchecked((int)(0x32)), unchecked((int)(0x33)) };

		protected internal static readonly byte[] newValue1 = new byte[] { unchecked((int
			)(0x31)), unchecked((int)(0x31)), unchecked((int)(0x31)) };

		protected internal const string name2 = "user.a2";

		protected internal static readonly byte[] value2 = new byte[] { unchecked((int)(0x37
			)), unchecked((int)(0x38)), unchecked((int)(0x39)) };

		protected internal const string name3 = "user.a3";

		private MiniDFSCluster cluster;

		private NameNode nn0;

		private NameNode nn1;

		private FileSystem fs;

		// XAttrs
		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void SetupCluster()
		{
			Configuration conf = new Configuration();
			conf.SetInt(DFSConfigKeys.DfsHaTaileditsPeriodKey, 1);
			HAUtil.SetAllowStandbyReads(conf, true);
			cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleHATopology
				()).NumDataNodes(1).WaitSafeMode(false).Build();
			cluster.WaitActive();
			nn0 = cluster.GetNameNode(0);
			nn1 = cluster.GetNameNode(1);
			fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
			cluster.TransitionToActive(0);
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void ShutdownCluster()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Test that xattrs are properly tracked by the standby</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestXAttrsTrackedOnStandby()
		{
			fs.Create(path).Close();
			fs.SetXAttr(path, name1, value1, EnumSet.Of(XAttrSetFlag.Create));
			fs.SetXAttr(path, name2, value2, EnumSet.Of(XAttrSetFlag.Create));
			HATestUtil.WaitForStandbyToCatchUp(nn0, nn1);
			IList<XAttr> xAttrs = nn1.GetRpcServer().GetXAttrs("/file", null);
			NUnit.Framework.Assert.AreEqual(2, xAttrs.Count);
			cluster.ShutdownNameNode(0);
			// Failover the current standby to active.
			cluster.ShutdownNameNode(0);
			cluster.TransitionToActive(1);
			IDictionary<string, byte[]> xattrs = fs.GetXAttrs(path);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 2);
			Assert.AssertArrayEquals(value1, xattrs[name1]);
			Assert.AssertArrayEquals(value2, xattrs[name2]);
			fs.Delete(path, true);
		}
	}
}
