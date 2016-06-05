using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Viewfs
{
	/// <summary>Verify XAttrs through ViewFs functionality.</summary>
	public class TestViewFsWithXAttrs
	{
		private static MiniDFSCluster cluster;

		private static Configuration clusterConf = new Configuration();

		private static FileContext fc;

		private static FileContext fc2;

		private FileContext fcView;

		private FileContext fcTarget;

		private FileContext fcTarget2;

		private Configuration fsViewConf;

		private Path targetTestRoot;

		private Path targetTestRoot2;

		private Path mountOnNn1;

		private Path mountOnNn2;

		private FileContextTestHelper fileContextTestHelper = new FileContextTestHelper("/tmp/TestViewFsWithXAttrs"
			);

		protected internal const string name1 = "user.a1";

		protected internal static readonly byte[] value1 = new byte[] { unchecked((int)(0x31
			)), unchecked((int)(0x32)), unchecked((int)(0x33)) };

		protected internal const string name2 = "user.a2";

		protected internal static readonly byte[] value2 = new byte[] { unchecked((int)(0x37
			)), unchecked((int)(0x38)), unchecked((int)(0x39)) };

		// XAttrs
		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void ClusterSetupAtBeginning()
		{
			cluster = new MiniDFSCluster.Builder(clusterConf).NnTopology(MiniDFSNNTopology.SimpleFederatedTopology
				(2)).NumDataNodes(2).Build();
			cluster.WaitClusterUp();
			fc = FileContext.GetFileContext(cluster.GetURI(0), clusterConf);
			fc2 = FileContext.GetFileContext(cluster.GetURI(1), clusterConf);
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void ClusterShutdownAtEnd()
		{
			cluster.Shutdown();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			fcTarget = fc;
			fcTarget2 = fc2;
			targetTestRoot = fileContextTestHelper.GetAbsoluteTestRootPath(fc);
			targetTestRoot2 = fileContextTestHelper.GetAbsoluteTestRootPath(fc2);
			fcTarget.Delete(targetTestRoot, true);
			fcTarget2.Delete(targetTestRoot2, true);
			fcTarget.Mkdir(targetTestRoot, new FsPermission((short)0x1e8), true);
			fcTarget2.Mkdir(targetTestRoot2, new FsPermission((short)0x1e8), true);
			fsViewConf = ViewFileSystemTestSetup.CreateConfig();
			SetupMountPoints();
			fcView = FileContext.GetFileContext(FsConstants.ViewfsUri, fsViewConf);
		}

		private void SetupMountPoints()
		{
			mountOnNn1 = new Path("/mountOnNn1");
			mountOnNn2 = new Path("/mountOnNn2");
			ConfigUtil.AddLink(fsViewConf, mountOnNn1.ToString(), targetTestRoot.ToUri());
			ConfigUtil.AddLink(fsViewConf, mountOnNn2.ToString(), targetTestRoot2.ToUri());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			fcTarget.Delete(fileContextTestHelper.GetTestRootPath(fcTarget), true);
			fcTarget2.Delete(fileContextTestHelper.GetTestRootPath(fcTarget2), true);
		}

		/// <summary>
		/// Verify a ViewFs wrapped over multiple federated NameNodes will
		/// dispatch the XAttr operations to the correct NameNode.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestXAttrOnMountEntry()
		{
			// Set XAttrs on the first namespace and verify they are correct
			fcView.SetXAttr(mountOnNn1, name1, value1);
			fcView.SetXAttr(mountOnNn1, name2, value2);
			NUnit.Framework.Assert.AreEqual(2, fcView.GetXAttrs(mountOnNn1).Count);
			Assert.AssertArrayEquals(value1, fcView.GetXAttr(mountOnNn1, name1));
			Assert.AssertArrayEquals(value2, fcView.GetXAttr(mountOnNn1, name2));
			// Double-check by getting the XAttrs using FileSystem
			// instead of ViewFs
			Assert.AssertArrayEquals(value1, fc.GetXAttr(targetTestRoot, name1));
			Assert.AssertArrayEquals(value2, fc.GetXAttr(targetTestRoot, name2));
			// Paranoid check: verify the other namespace does not
			// have XAttrs set on the same path.
			NUnit.Framework.Assert.AreEqual(0, fcView.GetXAttrs(mountOnNn2).Count);
			NUnit.Framework.Assert.AreEqual(0, fc2.GetXAttrs(targetTestRoot2).Count);
			// Remove the XAttr entries on the first namespace
			fcView.RemoveXAttr(mountOnNn1, name1);
			fcView.RemoveXAttr(mountOnNn1, name2);
			NUnit.Framework.Assert.AreEqual(0, fcView.GetXAttrs(mountOnNn1).Count);
			NUnit.Framework.Assert.AreEqual(0, fc.GetXAttrs(targetTestRoot).Count);
			// Now set XAttrs on the second namespace
			fcView.SetXAttr(mountOnNn2, name1, value1);
			fcView.SetXAttr(mountOnNn2, name2, value2);
			NUnit.Framework.Assert.AreEqual(2, fcView.GetXAttrs(mountOnNn2).Count);
			Assert.AssertArrayEquals(value1, fcView.GetXAttr(mountOnNn2, name1));
			Assert.AssertArrayEquals(value2, fcView.GetXAttr(mountOnNn2, name2));
			Assert.AssertArrayEquals(value1, fc2.GetXAttr(targetTestRoot2, name1));
			Assert.AssertArrayEquals(value2, fc2.GetXAttr(targetTestRoot2, name2));
			fcView.RemoveXAttr(mountOnNn2, name1);
			fcView.RemoveXAttr(mountOnNn2, name2);
			NUnit.Framework.Assert.AreEqual(0, fcView.GetXAttrs(mountOnNn2).Count);
			NUnit.Framework.Assert.AreEqual(0, fc2.GetXAttrs(targetTestRoot2).Count);
		}
	}
}
