using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Viewfs
{
	/// <summary>Verify XAttrs through ViewFileSystem functionality.</summary>
	public class TestViewFileSystemWithXAttrs
	{
		private static MiniDFSCluster cluster;

		private static Configuration clusterConf = new Configuration();

		private static FileSystem fHdfs;

		private static FileSystem fHdfs2;

		private FileSystem fsView;

		private Configuration fsViewConf;

		private FileSystem fsTarget;

		private FileSystem fsTarget2;

		private Path targetTestRoot;

		private Path targetTestRoot2;

		private Path mountOnNn1;

		private Path mountOnNn2;

		private FileSystemTestHelper fileSystemTestHelper = new FileSystemTestHelper("/tmp/TestViewFileSystemWithXAttrs"
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
			fHdfs = cluster.GetFileSystem(0);
			fHdfs2 = cluster.GetFileSystem(1);
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
			fsTarget = fHdfs;
			fsTarget2 = fHdfs2;
			targetTestRoot = fileSystemTestHelper.GetAbsoluteTestRootPath(fsTarget);
			targetTestRoot2 = fileSystemTestHelper.GetAbsoluteTestRootPath(fsTarget2);
			fsTarget.Delete(targetTestRoot, true);
			fsTarget2.Delete(targetTestRoot2, true);
			fsTarget.Mkdirs(targetTestRoot);
			fsTarget2.Mkdirs(targetTestRoot2);
			fsViewConf = ViewFileSystemTestSetup.CreateConfig();
			SetupMountPoints();
			fsView = FileSystem.Get(FsConstants.ViewfsUri, fsViewConf);
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
			fsTarget.Delete(fileSystemTestHelper.GetTestRootPath(fsTarget), true);
			fsTarget2.Delete(fileSystemTestHelper.GetTestRootPath(fsTarget2), true);
		}

		/// <summary>
		/// Verify a ViewFileSystem wrapped over multiple federated NameNodes will
		/// dispatch the XAttr operations to the correct NameNode.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestXAttrOnMountEntry()
		{
			// Set XAttrs on the first namespace and verify they are correct
			fsView.SetXAttr(mountOnNn1, name1, value1);
			fsView.SetXAttr(mountOnNn1, name2, value2);
			NUnit.Framework.Assert.AreEqual(2, fsView.GetXAttrs(mountOnNn1).Count);
			Assert.AssertArrayEquals(value1, fsView.GetXAttr(mountOnNn1, name1));
			Assert.AssertArrayEquals(value2, fsView.GetXAttr(mountOnNn1, name2));
			// Double-check by getting the XAttrs using FileSystem
			// instead of ViewFileSystem
			Assert.AssertArrayEquals(value1, fHdfs.GetXAttr(targetTestRoot, name1));
			Assert.AssertArrayEquals(value2, fHdfs.GetXAttr(targetTestRoot, name2));
			// Paranoid check: verify the other namespace does not
			// have XAttrs set on the same path.
			NUnit.Framework.Assert.AreEqual(0, fsView.GetXAttrs(mountOnNn2).Count);
			NUnit.Framework.Assert.AreEqual(0, fHdfs2.GetXAttrs(targetTestRoot2).Count);
			// Remove the XAttr entries on the first namespace
			fsView.RemoveXAttr(mountOnNn1, name1);
			fsView.RemoveXAttr(mountOnNn1, name2);
			NUnit.Framework.Assert.AreEqual(0, fsView.GetXAttrs(mountOnNn1).Count);
			NUnit.Framework.Assert.AreEqual(0, fHdfs.GetXAttrs(targetTestRoot).Count);
			// Now set XAttrs on the second namespace
			fsView.SetXAttr(mountOnNn2, name1, value1);
			fsView.SetXAttr(mountOnNn2, name2, value2);
			NUnit.Framework.Assert.AreEqual(2, fsView.GetXAttrs(mountOnNn2).Count);
			Assert.AssertArrayEquals(value1, fsView.GetXAttr(mountOnNn2, name1));
			Assert.AssertArrayEquals(value2, fsView.GetXAttr(mountOnNn2, name2));
			Assert.AssertArrayEquals(value1, fHdfs2.GetXAttr(targetTestRoot2, name1));
			Assert.AssertArrayEquals(value2, fHdfs2.GetXAttr(targetTestRoot2, name2));
			fsView.RemoveXAttr(mountOnNn2, name1);
			fsView.RemoveXAttr(mountOnNn2, name2);
			NUnit.Framework.Assert.AreEqual(0, fsView.GetXAttrs(mountOnNn2).Count);
			NUnit.Framework.Assert.AreEqual(0, fHdfs2.GetXAttrs(targetTestRoot2).Count);
		}
	}
}
