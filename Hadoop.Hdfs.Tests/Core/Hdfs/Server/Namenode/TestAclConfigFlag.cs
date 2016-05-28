using Com.Google.Common.Collect;
using NUnit.Framework;
using NUnit.Framework.Rules;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// Tests that the configuration flag that controls support for ACLs is off by
	/// default and causes all attempted operations related to ACLs to fail.
	/// </summary>
	/// <remarks>
	/// Tests that the configuration flag that controls support for ACLs is off by
	/// default and causes all attempted operations related to ACLs to fail.  The
	/// NameNode can still load ACLs from fsimage or edits.
	/// </remarks>
	public class TestAclConfigFlag
	{
		private static readonly Path Path = new Path("/path");

		private MiniDFSCluster cluster;

		private DistributedFileSystem fs;

		[Rule]
		public ExpectedException exception = ExpectedException.None();

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void Shutdown()
		{
			IOUtils.Cleanup(null, fs);
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestModifyAclEntries()
		{
			InitCluster(true, false);
			fs.Mkdirs(Path);
			ExpectException();
			fs.ModifyAclEntries(Path, Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.ReadWrite)));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveAclEntries()
		{
			InitCluster(true, false);
			fs.Mkdirs(Path);
			ExpectException();
			fs.RemoveAclEntries(Path, Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.ReadWrite)));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveDefaultAcl()
		{
			InitCluster(true, false);
			fs.Mkdirs(Path);
			ExpectException();
			fs.RemoveAclEntries(Path, Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.ReadWrite)));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveAcl()
		{
			InitCluster(true, false);
			fs.Mkdirs(Path);
			ExpectException();
			fs.RemoveAcl(Path);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSetAcl()
		{
			InitCluster(true, false);
			fs.Mkdirs(Path);
			ExpectException();
			fs.SetAcl(Path, Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope.Default, 
				AclEntryType.User, "foo", FsAction.ReadWrite)));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetAclStatus()
		{
			InitCluster(true, false);
			fs.Mkdirs(Path);
			ExpectException();
			fs.GetAclStatus(Path);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEditLog()
		{
			// With ACLs enabled, set an ACL.
			InitCluster(true, true);
			fs.Mkdirs(Path);
			fs.SetAcl(Path, Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope.Default, 
				AclEntryType.User, "foo", FsAction.ReadWrite)));
			// Restart with ACLs disabled.  Expect successful restart.
			Restart(false, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFsImage()
		{
			// With ACLs enabled, set an ACL.
			InitCluster(true, true);
			fs.Mkdirs(Path);
			fs.SetAcl(Path, Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope.Default, 
				AclEntryType.User, "foo", FsAction.ReadWrite)));
			// Save a new checkpoint and restart with ACLs still enabled.
			Restart(true, true);
			// Restart with ACLs disabled.  Expect successful restart.
			Restart(false, false);
		}

		/// <summary>
		/// We expect an AclException, and we want the exception text to state the
		/// configuration key that controls ACL support.
		/// </summary>
		private void ExpectException()
		{
			exception.Expect(typeof(AclException));
			exception.ExpectMessage(DFSConfigKeys.DfsNamenodeAclsEnabledKey);
		}

		/// <summary>Initialize the cluster, wait for it to become active, and get FileSystem.
		/// 	</summary>
		/// <param name="format">if true, format the NameNode and DataNodes before starting up
		/// 	</param>
		/// <param name="aclsEnabled">if true, ACL support is enabled</param>
		/// <exception cref="System.Exception">if any step fails</exception>
		private void InitCluster(bool format, bool aclsEnabled)
		{
			Configuration conf = new Configuration();
			// not explicitly setting to false, should be false by default
			if (aclsEnabled)
			{
				conf.SetBoolean(DFSConfigKeys.DfsNamenodeAclsEnabledKey, true);
			}
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Format(format).Build();
			cluster.WaitActive();
			fs = cluster.GetFileSystem();
		}

		/// <summary>Restart the cluster, optionally saving a new checkpoint.</summary>
		/// <param name="checkpoint">boolean true to save a new checkpoint</param>
		/// <param name="aclsEnabled">if true, ACL support is enabled</param>
		/// <exception cref="System.Exception">if restart fails</exception>
		private void Restart(bool checkpoint, bool aclsEnabled)
		{
			NameNode nameNode = cluster.GetNameNode();
			if (checkpoint)
			{
				NameNodeAdapter.EnterSafeMode(nameNode, false);
				NameNodeAdapter.SaveNamespace(nameNode);
			}
			Shutdown();
			InitCluster(false, aclsEnabled);
		}
	}
}
