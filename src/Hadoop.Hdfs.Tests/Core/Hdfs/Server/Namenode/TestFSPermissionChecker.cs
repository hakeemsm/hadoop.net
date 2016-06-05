using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Security;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Unit tests covering FSPermissionChecker.</summary>
	/// <remarks>
	/// Unit tests covering FSPermissionChecker.  All tests in this suite have been
	/// cross-validated against Linux setfacl/getfacl to check for consistency of the
	/// HDFS implementation.
	/// </remarks>
	public class TestFSPermissionChecker
	{
		private const long PreferredBlockSize = 128 * 1024 * 1024;

		private const short Replication = 3;

		private const string Supergroup = "supergroup";

		private const string Superuser = "superuser";

		private static readonly UserGroupInformation Bruce = UserGroupInformation.CreateUserForTesting
			("bruce", new string[] {  });

		private static readonly UserGroupInformation Diana = UserGroupInformation.CreateUserForTesting
			("diana", new string[] { "sales" });

		private static readonly UserGroupInformation Clark = UserGroupInformation.CreateUserForTesting
			("clark", new string[] { "execs" });

		private FSDirectory dir;

		private INodeDirectory inodeRoot;

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			Configuration conf = new Configuration();
			FSNamesystem fsn = Org.Mockito.Mockito.Mock<FSNamesystem>();
			Org.Mockito.Mockito.DoAnswer(new _Answer_81()).When(fsn).CreateFsOwnerPermissions
				(Matchers.Any<FsPermission>());
			dir = new FSDirectory(fsn, conf);
			inodeRoot = dir.GetRoot();
		}

		private sealed class _Answer_81 : Answer
		{
			public _Answer_81()
			{
			}

			/// <exception cref="System.Exception"/>
			public object Answer(InvocationOnMock invocation)
			{
				object[] args = invocation.GetArguments();
				FsPermission perm = (FsPermission)args[0];
				return new PermissionStatus(TestFSPermissionChecker.Superuser, TestFSPermissionChecker
					.Supergroup, perm);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAclOwner()
		{
			INodeFile inodeFile = CreateINodeFile(inodeRoot, "file1", "bruce", "execs", (short
				)0x1a0);
			AddAcl(inodeFile, AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Mask, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.None));
			AssertPermissionGranted(Bruce, "/file1", FsAction.Read);
			AssertPermissionGranted(Bruce, "/file1", FsAction.Write);
			AssertPermissionGranted(Bruce, "/file1", FsAction.ReadWrite);
			AssertPermissionDenied(Bruce, "/file1", FsAction.Execute);
			AssertPermissionDenied(Diana, "/file1", FsAction.Read);
			AssertPermissionDenied(Diana, "/file1", FsAction.Write);
			AssertPermissionDenied(Diana, "/file1", FsAction.Execute);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAclNamedUser()
		{
			INodeFile inodeFile = CreateINodeFile(inodeRoot, "file1", "bruce", "execs", (short
				)0x1a0);
			AddAcl(inodeFile, AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, "diana", FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Mask, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.None));
			AssertPermissionGranted(Diana, "/file1", FsAction.Read);
			AssertPermissionDenied(Diana, "/file1", FsAction.Write);
			AssertPermissionDenied(Diana, "/file1", FsAction.Execute);
			AssertPermissionDenied(Diana, "/file1", FsAction.ReadWrite);
			AssertPermissionDenied(Diana, "/file1", FsAction.ReadExecute);
			AssertPermissionDenied(Diana, "/file1", FsAction.WriteExecute);
			AssertPermissionDenied(Diana, "/file1", FsAction.All);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAclNamedUserDeny()
		{
			INodeFile inodeFile = CreateINodeFile(inodeRoot, "file1", "bruce", "execs", (short
				)0x1a4);
			AddAcl(inodeFile, AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, "diana", FsAction.None), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Mask, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.Read));
			AssertPermissionGranted(Bruce, "/file1", FsAction.ReadWrite);
			AssertPermissionGranted(Clark, "/file1", FsAction.Read);
			AssertPermissionDenied(Diana, "/file1", FsAction.Read);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAclNamedUserTraverseDeny()
		{
			INodeDirectory inodeDir = CreateINodeDirectory(inodeRoot, "dir1", "bruce", "execs"
				, (short)0x1ed);
			INodeFile inodeFile = CreateINodeFile(inodeDir, "file1", "bruce", "execs", (short
				)0x1a4);
			AddAcl(inodeDir, AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User, 
				FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User, 
				"diana", FsAction.None), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Mask, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.ReadExecute));
			AssertPermissionGranted(Bruce, "/dir1/file1", FsAction.ReadWrite);
			AssertPermissionGranted(Clark, "/dir1/file1", FsAction.Read);
			AssertPermissionDenied(Diana, "/dir1/file1", FsAction.Read);
			AssertPermissionDenied(Diana, "/dir1/file1", FsAction.Write);
			AssertPermissionDenied(Diana, "/dir1/file1", FsAction.Execute);
			AssertPermissionDenied(Diana, "/dir1/file1", FsAction.ReadWrite);
			AssertPermissionDenied(Diana, "/dir1/file1", FsAction.ReadExecute);
			AssertPermissionDenied(Diana, "/dir1/file1", FsAction.WriteExecute);
			AssertPermissionDenied(Diana, "/dir1/file1", FsAction.All);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAclNamedUserMask()
		{
			INodeFile inodeFile = CreateINodeFile(inodeRoot, "file1", "bruce", "execs", (short
				)0x190);
			AddAcl(inodeFile, AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, "diana", FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Mask, FsAction.Write), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.None));
			AssertPermissionDenied(Diana, "/file1", FsAction.Read);
			AssertPermissionDenied(Diana, "/file1", FsAction.Write);
			AssertPermissionDenied(Diana, "/file1", FsAction.Execute);
			AssertPermissionDenied(Diana, "/file1", FsAction.ReadWrite);
			AssertPermissionDenied(Diana, "/file1", FsAction.ReadExecute);
			AssertPermissionDenied(Diana, "/file1", FsAction.WriteExecute);
			AssertPermissionDenied(Diana, "/file1", FsAction.All);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAclGroup()
		{
			INodeFile inodeFile = CreateINodeFile(inodeRoot, "file1", "bruce", "execs", (short
				)0x1a0);
			AddAcl(inodeFile, AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Mask, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.None));
			AssertPermissionGranted(Clark, "/file1", FsAction.Read);
			AssertPermissionDenied(Clark, "/file1", FsAction.Write);
			AssertPermissionDenied(Clark, "/file1", FsAction.Execute);
			AssertPermissionDenied(Clark, "/file1", FsAction.ReadWrite);
			AssertPermissionDenied(Clark, "/file1", FsAction.ReadExecute);
			AssertPermissionDenied(Clark, "/file1", FsAction.WriteExecute);
			AssertPermissionDenied(Clark, "/file1", FsAction.All);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAclGroupDeny()
		{
			INodeFile inodeFile = CreateINodeFile(inodeRoot, "file1", "bruce", "sales", (short
				)0x184);
			AddAcl(inodeFile, AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.None), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Mask, FsAction.None), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.Read));
			AssertPermissionGranted(Bruce, "/file1", FsAction.ReadWrite);
			AssertPermissionGranted(Clark, "/file1", FsAction.Read);
			AssertPermissionDenied(Diana, "/file1", FsAction.Read);
			AssertPermissionDenied(Diana, "/file1", FsAction.Write);
			AssertPermissionDenied(Diana, "/file1", FsAction.Execute);
			AssertPermissionDenied(Diana, "/file1", FsAction.ReadWrite);
			AssertPermissionDenied(Diana, "/file1", FsAction.ReadExecute);
			AssertPermissionDenied(Diana, "/file1", FsAction.WriteExecute);
			AssertPermissionDenied(Diana, "/file1", FsAction.All);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAclGroupTraverseDeny()
		{
			INodeDirectory inodeDir = CreateINodeDirectory(inodeRoot, "dir1", "bruce", "execs"
				, (short)0x1ed);
			INodeFile inodeFile = CreateINodeFile(inodeDir, "file1", "bruce", "execs", (short
				)0x1a4);
			AddAcl(inodeDir, AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User, 
				FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.Group, 
				FsAction.None), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.Mask, 
				FsAction.None), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.Other
				, FsAction.ReadExecute));
			AssertPermissionGranted(Bruce, "/dir1/file1", FsAction.ReadWrite);
			AssertPermissionGranted(Diana, "/dir1/file1", FsAction.Read);
			AssertPermissionDenied(Clark, "/dir1/file1", FsAction.Read);
			AssertPermissionDenied(Clark, "/dir1/file1", FsAction.Write);
			AssertPermissionDenied(Clark, "/dir1/file1", FsAction.Execute);
			AssertPermissionDenied(Clark, "/dir1/file1", FsAction.ReadWrite);
			AssertPermissionDenied(Clark, "/dir1/file1", FsAction.ReadExecute);
			AssertPermissionDenied(Clark, "/dir1/file1", FsAction.WriteExecute);
			AssertPermissionDenied(Clark, "/dir1/file1", FsAction.All);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAclGroupTraverseDenyOnlyDefaultEntries()
		{
			INodeDirectory inodeDir = CreateINodeDirectory(inodeRoot, "dir1", "bruce", "execs"
				, (short)0x1ed);
			INodeFile inodeFile = CreateINodeFile(inodeDir, "file1", "bruce", "execs", (short
				)0x1a4);
			AddAcl(inodeDir, AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User, 
				FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.Group, 
				FsAction.None), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.Other
				, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.Group, "sales", FsAction.None), AclTestHelpers.AclEntry(AclEntryScope.Default, 
				AclEntryType.Group, FsAction.None), AclTestHelpers.AclEntry(AclEntryScope.Default
				, AclEntryType.Other, FsAction.ReadExecute));
			AssertPermissionGranted(Bruce, "/dir1/file1", FsAction.ReadWrite);
			AssertPermissionGranted(Diana, "/dir1/file1", FsAction.Read);
			AssertPermissionDenied(Clark, "/dir1/file1", FsAction.Read);
			AssertPermissionDenied(Clark, "/dir1/file1", FsAction.Write);
			AssertPermissionDenied(Clark, "/dir1/file1", FsAction.Execute);
			AssertPermissionDenied(Clark, "/dir1/file1", FsAction.ReadWrite);
			AssertPermissionDenied(Clark, "/dir1/file1", FsAction.ReadExecute);
			AssertPermissionDenied(Clark, "/dir1/file1", FsAction.WriteExecute);
			AssertPermissionDenied(Clark, "/dir1/file1", FsAction.All);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAclGroupMask()
		{
			INodeFile inodeFile = CreateINodeFile(inodeRoot, "file1", "bruce", "execs", (short
				)0x1a4);
			AddAcl(inodeFile, AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Mask, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.Read));
			AssertPermissionGranted(Bruce, "/file1", FsAction.ReadWrite);
			AssertPermissionGranted(Clark, "/file1", FsAction.Read);
			AssertPermissionDenied(Clark, "/file1", FsAction.Write);
			AssertPermissionDenied(Clark, "/file1", FsAction.Execute);
			AssertPermissionDenied(Clark, "/file1", FsAction.ReadWrite);
			AssertPermissionDenied(Clark, "/file1", FsAction.ReadExecute);
			AssertPermissionDenied(Clark, "/file1", FsAction.WriteExecute);
			AssertPermissionDenied(Clark, "/file1", FsAction.All);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAclNamedGroup()
		{
			INodeFile inodeFile = CreateINodeFile(inodeRoot, "file1", "bruce", "execs", (short
				)0x1a0);
			AddAcl(inodeFile, AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, "sales", FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Mask, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.None));
			AssertPermissionGranted(Bruce, "/file1", FsAction.ReadWrite);
			AssertPermissionGranted(Clark, "/file1", FsAction.Read);
			AssertPermissionGranted(Diana, "/file1", FsAction.Read);
			AssertPermissionDenied(Diana, "/file1", FsAction.Write);
			AssertPermissionDenied(Diana, "/file1", FsAction.Execute);
			AssertPermissionDenied(Diana, "/file1", FsAction.ReadWrite);
			AssertPermissionDenied(Diana, "/file1", FsAction.ReadExecute);
			AssertPermissionDenied(Diana, "/file1", FsAction.All);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAclNamedGroupDeny()
		{
			INodeFile inodeFile = CreateINodeFile(inodeRoot, "file1", "bruce", "sales", (short
				)0x1a4);
			AddAcl(inodeFile, AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, "execs", FsAction.None), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Mask, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.Read));
			AssertPermissionGranted(Bruce, "/file1", FsAction.ReadWrite);
			AssertPermissionGranted(Diana, "/file1", FsAction.Read);
			AssertPermissionDenied(Clark, "/file1", FsAction.Read);
			AssertPermissionDenied(Clark, "/file1", FsAction.Write);
			AssertPermissionDenied(Clark, "/file1", FsAction.Execute);
			AssertPermissionDenied(Clark, "/file1", FsAction.ReadWrite);
			AssertPermissionDenied(Clark, "/file1", FsAction.ReadExecute);
			AssertPermissionDenied(Clark, "/file1", FsAction.WriteExecute);
			AssertPermissionDenied(Clark, "/file1", FsAction.All);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAclNamedGroupTraverseDeny()
		{
			INodeDirectory inodeDir = CreateINodeDirectory(inodeRoot, "dir1", "bruce", "execs"
				, (short)0x1ed);
			INodeFile inodeFile = CreateINodeFile(inodeDir, "file1", "bruce", "execs", (short
				)0x1a4);
			AddAcl(inodeDir, AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User, 
				FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.Group, 
				FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, "sales", FsAction.None), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Mask, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.ReadExecute));
			AssertPermissionGranted(Bruce, "/dir1/file1", FsAction.ReadWrite);
			AssertPermissionGranted(Clark, "/dir1/file1", FsAction.Read);
			AssertPermissionDenied(Diana, "/dir1/file1", FsAction.Read);
			AssertPermissionDenied(Diana, "/dir1/file1", FsAction.Write);
			AssertPermissionDenied(Diana, "/dir1/file1", FsAction.Execute);
			AssertPermissionDenied(Diana, "/dir1/file1", FsAction.ReadWrite);
			AssertPermissionDenied(Diana, "/dir1/file1", FsAction.ReadExecute);
			AssertPermissionDenied(Diana, "/dir1/file1", FsAction.WriteExecute);
			AssertPermissionDenied(Diana, "/dir1/file1", FsAction.All);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAclNamedGroupMask()
		{
			INodeFile inodeFile = CreateINodeFile(inodeRoot, "file1", "bruce", "execs", (short
				)0x1a4);
			AddAcl(inodeFile, AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, "sales", FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.Mask, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.Other, FsAction.Read));
			AssertPermissionGranted(Bruce, "/file1", FsAction.ReadWrite);
			AssertPermissionGranted(Clark, "/file1", FsAction.Read);
			AssertPermissionGranted(Diana, "/file1", FsAction.Read);
			AssertPermissionDenied(Diana, "/file1", FsAction.Write);
			AssertPermissionDenied(Diana, "/file1", FsAction.Execute);
			AssertPermissionDenied(Diana, "/file1", FsAction.ReadWrite);
			AssertPermissionDenied(Diana, "/file1", FsAction.ReadExecute);
			AssertPermissionDenied(Diana, "/file1", FsAction.WriteExecute);
			AssertPermissionDenied(Diana, "/file1", FsAction.All);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAclOther()
		{
			INodeFile inodeFile = CreateINodeFile(inodeRoot, "file1", "bruce", "sales", (short
				)0x1fc);
			AddAcl(inodeFile, AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User
				, "diana", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Mask, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.Read));
			AssertPermissionGranted(Bruce, "/file1", FsAction.All);
			AssertPermissionGranted(Diana, "/file1", FsAction.All);
			AssertPermissionGranted(Clark, "/file1", FsAction.Read);
			AssertPermissionDenied(Clark, "/file1", FsAction.Write);
			AssertPermissionDenied(Clark, "/file1", FsAction.Execute);
			AssertPermissionDenied(Clark, "/file1", FsAction.ReadWrite);
			AssertPermissionDenied(Clark, "/file1", FsAction.ReadExecute);
			AssertPermissionDenied(Clark, "/file1", FsAction.WriteExecute);
			AssertPermissionDenied(Clark, "/file1", FsAction.All);
		}

		/// <exception cref="System.IO.IOException"/>
		private void AddAcl(INodeWithAdditionalFields inode, params AclEntry[] acl)
		{
			AclStorage.UpdateINodeAcl(inode, Arrays.AsList(acl), Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId);
		}

		/// <exception cref="System.IO.IOException"/>
		private void AssertPermissionGranted(UserGroupInformation user, string path, FsAction
			 access)
		{
			INodesInPath iip = dir.GetINodesInPath(path, true);
			dir.GetPermissionChecker(Superuser, Supergroup, user).CheckPermission(iip, false, 
				null, null, access, null, false);
		}

		/// <exception cref="System.IO.IOException"/>
		private void AssertPermissionDenied(UserGroupInformation user, string path, FsAction
			 access)
		{
			try
			{
				INodesInPath iip = dir.GetINodesInPath(path, true);
				dir.GetPermissionChecker(Superuser, Supergroup, user).CheckPermission(iip, false, 
					null, null, access, null, false);
				NUnit.Framework.Assert.Fail("expected AccessControlException for user + " + user 
					+ ", path = " + path + ", access = " + access);
			}
			catch (AccessControlException e)
			{
				NUnit.Framework.Assert.IsTrue("Permission denied messages must carry the username"
					, e.Message.Contains(user.GetUserName().ToString()));
				NUnit.Framework.Assert.IsTrue("Permission denied messages must carry the path parent"
					, e.Message.Contains(new Path(path).GetParent().ToUri().GetPath()));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static INodeDirectory CreateINodeDirectory(INodeDirectory parent, string 
			name, string owner, string group, short perm)
		{
			PermissionStatus permStatus = PermissionStatus.CreateImmutable(owner, group, FsPermission
				.CreateImmutable(perm));
			INodeDirectory inodeDirectory = new INodeDirectory(INodeId.GrandfatherInodeId, Sharpen.Runtime.GetBytesForString
				(name, "UTF-8"), permStatus, 0L);
			parent.AddChild(inodeDirectory);
			return inodeDirectory;
		}

		/// <exception cref="System.IO.IOException"/>
		private static INodeFile CreateINodeFile(INodeDirectory parent, string name, string
			 owner, string group, short perm)
		{
			PermissionStatus permStatus = PermissionStatus.CreateImmutable(owner, group, FsPermission
				.CreateImmutable(perm));
			INodeFile inodeFile = new INodeFile(INodeId.GrandfatherInodeId, Sharpen.Runtime.GetBytesForString
				(name, "UTF-8"), permStatus, 0L, 0L, null, Replication, PreferredBlockSize, unchecked(
				(byte)0));
			parent.AddChild(inodeFile);
			return inodeFile;
		}
	}
}
