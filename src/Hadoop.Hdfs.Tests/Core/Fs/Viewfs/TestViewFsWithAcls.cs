using System.Collections.Generic;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Viewfs
{
	/// <summary>Verify ACL through ViewFs functionality.</summary>
	public class TestViewFsWithAcls
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

		private FileContextTestHelper fileContextTestHelper = new FileContextTestHelper("/tmp/TestViewFsWithAcls"
			);

		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void ClusterSetupAtBeginning()
		{
			clusterConf.SetBoolean(DFSConfigKeys.DfsNamenodeAclsEnabledKey, true);
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
		/// dispatch the ACL operations to the correct NameNode.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAclOnMountEntry()
		{
			// Set ACLs on the first namespace and verify they are correct
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "foo", FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None));
			fcView.SetAcl(mountOnNn1, aclSpec);
			AclEntry[] expected = new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "foo", FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.Read) };
			Assert.AssertArrayEquals(expected, AclEntryArray(fcView.GetAclStatus(mountOnNn1))
				);
			// Double-check by getting ACL status using FileSystem
			// instead of ViewFs
			Assert.AssertArrayEquals(expected, AclEntryArray(fc.GetAclStatus(targetTestRoot))
				);
			// Modify the ACL entries on the first namespace
			aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.User, "foo", FsAction.Read));
			fcView.ModifyAclEntries(mountOnNn1, aclSpec);
			expected = new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, "foo", FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.User, FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.User, "foo", FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.Group, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.Mask, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.Other, FsAction.None) };
			Assert.AssertArrayEquals(expected, AclEntryArray(fcView.GetAclStatus(mountOnNn1))
				);
			fcView.RemoveDefaultAcl(mountOnNn1);
			expected = new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, "foo", FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read) };
			Assert.AssertArrayEquals(expected, AclEntryArray(fcView.GetAclStatus(mountOnNn1))
				);
			Assert.AssertArrayEquals(expected, AclEntryArray(fc.GetAclStatus(targetTestRoot))
				);
			// Paranoid check: verify the other namespace does not
			// have ACLs set on the same path.
			NUnit.Framework.Assert.AreEqual(0, fcView.GetAclStatus(mountOnNn2).GetEntries().Count
				);
			NUnit.Framework.Assert.AreEqual(0, fc2.GetAclStatus(targetTestRoot2).GetEntries()
				.Count);
			// Remove the ACL entries on the first namespace
			fcView.RemoveAcl(mountOnNn1);
			NUnit.Framework.Assert.AreEqual(0, fcView.GetAclStatus(mountOnNn1).GetEntries().Count
				);
			NUnit.Framework.Assert.AreEqual(0, fc.GetAclStatus(targetTestRoot).GetEntries().Count
				);
			// Now set ACLs on the second namespace
			aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, "bar", FsAction.Read));
			fcView.ModifyAclEntries(mountOnNn2, aclSpec);
			expected = new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, "bar", FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.ReadExecute) };
			Assert.AssertArrayEquals(expected, AclEntryArray(fcView.GetAclStatus(mountOnNn2))
				);
			Assert.AssertArrayEquals(expected, AclEntryArray(fc2.GetAclStatus(targetTestRoot2
				)));
			// Remove the ACL entries on the second namespace
			fcView.RemoveAclEntries(mountOnNn2, Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "bar", FsAction.Read)));
			expected = new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.ReadExecute) };
			Assert.AssertArrayEquals(expected, AclEntryArray(fc2.GetAclStatus(targetTestRoot2
				)));
			fcView.RemoveAcl(mountOnNn2);
			NUnit.Framework.Assert.AreEqual(0, fcView.GetAclStatus(mountOnNn2).GetEntries().Count
				);
			NUnit.Framework.Assert.AreEqual(0, fc2.GetAclStatus(targetTestRoot2).GetEntries()
				.Count);
		}

		private AclEntry[] AclEntryArray(AclStatus aclStatus)
		{
			return Sharpen.Collections.ToArray(aclStatus.GetEntries(), new AclEntry[0]);
		}
	}
}
