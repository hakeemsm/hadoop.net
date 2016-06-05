using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// 1) save xattrs, restart NN, assert xattrs reloaded from edit log,
	/// 2) save xattrs, create new checkpoint, restart NN, assert xattrs
	/// reloaded from fsimage
	/// </summary>
	public class TestFSImageWithXAttr
	{
		private static Configuration conf;

		private static MiniDFSCluster cluster;

		private const string name1 = "user.a1";

		private static readonly byte[] value1 = new byte[] { unchecked((int)(0x31)), unchecked(
			(int)(0x32)), unchecked((int)(0x33)) };

		private static readonly byte[] newValue1 = new byte[] { unchecked((int)(0x31)), unchecked(
			(int)(0x31)), unchecked((int)(0x31)) };

		private const string name2 = "user.a2";

		private static readonly byte[] value2 = new byte[] { unchecked((int)(0x37)), unchecked(
			(int)(0x38)), unchecked((int)(0x39)) };

		private const string name3 = "user.a3";

		private static readonly byte[] value3 = new byte[] {  };

		//xattrs
		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void SetUp()
		{
			conf = new Configuration();
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeXattrsEnabledKey, true);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.WaitActive();
		}

		[AfterClass]
		public static void TearDown()
		{
			cluster.Shutdown();
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestXAttr(bool persistNamespace)
		{
			Path path = new Path("/p");
			DistributedFileSystem fs = cluster.GetFileSystem();
			fs.Create(path).Close();
			fs.SetXAttr(path, name1, value1, EnumSet.Of(XAttrSetFlag.Create));
			fs.SetXAttr(path, name2, value2, EnumSet.Of(XAttrSetFlag.Create));
			fs.SetXAttr(path, name3, null, EnumSet.Of(XAttrSetFlag.Create));
			Restart(fs, persistNamespace);
			IDictionary<string, byte[]> xattrs = fs.GetXAttrs(path);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 3);
			Assert.AssertArrayEquals(value1, xattrs[name1]);
			Assert.AssertArrayEquals(value2, xattrs[name2]);
			Assert.AssertArrayEquals(value3, xattrs[name3]);
			fs.SetXAttr(path, name1, newValue1, EnumSet.Of(XAttrSetFlag.Replace));
			Restart(fs, persistNamespace);
			xattrs = fs.GetXAttrs(path);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 3);
			Assert.AssertArrayEquals(newValue1, xattrs[name1]);
			Assert.AssertArrayEquals(value2, xattrs[name2]);
			Assert.AssertArrayEquals(value3, xattrs[name3]);
			fs.RemoveXAttr(path, name1);
			fs.RemoveXAttr(path, name2);
			fs.RemoveXAttr(path, name3);
			Restart(fs, persistNamespace);
			xattrs = fs.GetXAttrs(path);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 0);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestPersistXAttr()
		{
			TestXAttr(true);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestXAttrEditLog()
		{
			TestXAttr(false);
		}

		/// <summary>Restart the NameNode, optionally saving a new checkpoint.</summary>
		/// <param name="fs">DistributedFileSystem used for saving namespace</param>
		/// <param name="persistNamespace">boolean true to save a new checkpoint</param>
		/// <exception cref="System.IO.IOException">if restart fails</exception>
		private void Restart(DistributedFileSystem fs, bool persistNamespace)
		{
			if (persistNamespace)
			{
				fs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
				fs.SaveNamespace();
				fs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave);
			}
			cluster.RestartNameNode();
			cluster.WaitActive();
		}
	}
}
