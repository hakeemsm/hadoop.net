using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>Unit tests for permission</summary>
	public class TestDFSPermission
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestDFSPermission));

		private static readonly Configuration conf = new HdfsConfiguration();

		private const string Group1Name = "group1";

		private const string Group2Name = "group2";

		private const string Group3Name = "group3";

		private const string Group4Name = "group4";

		private const string User1Name = "user1";

		private const string User2Name = "user2";

		private const string User3Name = "user3";

		private static readonly UserGroupInformation Superuser;

		private static readonly UserGroupInformation User1;

		private static readonly UserGroupInformation User2;

		private static readonly UserGroupInformation User3;

		private const short MaxPermission = 511;

		private const short DefaultUmask = 0x12;

		private static readonly FsPermission DefaultPermission = FsPermission.CreateImmutable
			((short)0x1ff);

		private static readonly int NumTestPermissions = conf.GetInt("test.dfs.permission.num"
			, 10) * (MaxPermission + 1) / 100;

		private const string PathName = "xx";

		private static readonly Path FileDirPath = new Path("/", PathName);

		private static readonly Path NonExistentPath = new Path("/parent", PathName);

		private static readonly Path NonExistentFile = new Path("/NonExistentFile");

		private FileSystem fs;

		private MiniDFSCluster cluster;

		private static readonly Random r;

		static TestDFSPermission()
		{
			createVerifier = new TestDFSPermission.CreatePermissionVerifier(this);
			openVerifier = new TestDFSPermission.OpenPermissionVerifier(this);
			replicatorVerifier = new TestDFSPermission.SetReplicationPermissionVerifier(this);
			timesVerifier = new TestDFSPermission.SetTimesPermissionVerifier(this);
			statsVerifier = new TestDFSPermission.StatsPermissionVerifier(this);
			listVerifier = new TestDFSPermission.ListPermissionVerifier(this);
			renameVerifier = new TestDFSPermission.RenamePermissionVerifier(this);
			fileDeletionVerifier = new TestDFSPermission.DeletePermissionVerifier(this);
			dirDeletionVerifier = new TestDFSPermission.DeleteDirPermissionVerifier(this);
			emptyDirDeletionVerifier = new TestDFSPermission.DeleteEmptyDirPermissionVerifier
				(this);
			try
			{
				// Initiate the random number generator and logging the seed
				long seed = Time.Now();
				r = new Random(seed);
				Log.Info("Random number generator uses seed " + seed);
				Log.Info("NUM_TEST_PERMISSIONS=" + NumTestPermissions);
				// explicitly turn on permission checking
				conf.SetBoolean(DFSConfigKeys.DfsPermissionsEnabledKey, true);
				// create fake mapping for the groups
				IDictionary<string, string[]> u2g_map = new Dictionary<string, string[]>(3);
				u2g_map[User1Name] = new string[] { Group1Name, Group2Name };
				u2g_map[User2Name] = new string[] { Group2Name, Group3Name };
				u2g_map[User3Name] = new string[] { Group3Name, Group4Name };
				DFSTestUtil.UpdateConfWithFakeGroupMapping(conf, u2g_map);
				// Initiate all four users
				Superuser = UserGroupInformation.GetCurrentUser();
				User1 = UserGroupInformation.CreateUserForTesting(User1Name, new string[] { Group1Name
					, Group2Name });
				User2 = UserGroupInformation.CreateUserForTesting(User2Name, new string[] { Group2Name
					, Group3Name });
				User3 = UserGroupInformation.CreateUserForTesting(User3Name, new string[] { Group3Name
					, Group4Name });
			}
			catch (IOException e)
			{
				throw new RuntimeException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
			cluster.WaitActive();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// This tests if permission setting in create, mkdir, and
		/// setPermission works correctly
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPermissionSetting()
		{
			TestPermissionSetting(TestDFSPermission.OpType.Create);
			// test file creation
			TestPermissionSetting(TestDFSPermission.OpType.Mkdirs);
		}

		// test directory creation
		/// <exception cref="System.Exception"/>
		private void InitFileSystem(short umask)
		{
			// set umask in configuration, converting to padded octal
			conf.Set(FsPermission.UmaskLabel, string.Format("%1$03o", umask));
			fs = FileSystem.Get(conf);
		}

		/// <exception cref="System.Exception"/>
		private void CloseFileSystem()
		{
			fs.Close();
		}

		/* check permission setting works correctly for file or directory */
		/// <exception cref="System.Exception"/>
		private void TestPermissionSetting(TestDFSPermission.OpType op)
		{
			short uMask = DefaultUmask;
			// case 1: use default permission but all possible umasks
			TestDFSPermission.PermissionGenerator generator = new TestDFSPermission.PermissionGenerator
				(r);
			FsPermission permission = new FsPermission(DefaultPermission);
			for (short i = 0; i < NumTestPermissions; i++)
			{
				uMask = generator.Next();
				InitFileSystem(uMask);
				CreateAndCheckPermission(op, FileDirPath, uMask, permission, true);
				CloseFileSystem();
			}
			// case 2: use permission 0643 and the default umask
			uMask = DefaultUmask;
			InitFileSystem(uMask);
			CreateAndCheckPermission(op, FileDirPath, uMask, new FsPermission((short)0x1a3), 
				true);
			CloseFileSystem();
			// case 3: use permission 0643 and umask 0222
			uMask = (short)0x92;
			InitFileSystem(uMask);
			CreateAndCheckPermission(op, FileDirPath, uMask, new FsPermission((short)0x1a3), 
				false);
			CloseFileSystem();
			// case 4: set permission
			uMask = (short)0x49;
			InitFileSystem(uMask);
			fs.SetPermission(FileDirPath, new FsPermission(uMask));
			short expectedPermission = (short)0x49;
			CheckPermission(FileDirPath, expectedPermission, true);
			CloseFileSystem();
			// case 5: test non-existent parent directory
			uMask = DefaultUmask;
			InitFileSystem(uMask);
			NUnit.Framework.Assert.IsFalse("File shouldn't exists", fs.Exists(NonExistentPath
				));
			CreateAndCheckPermission(op, NonExistentPath, uMask, new FsPermission(DefaultPermission
				), false);
			Path parent = NonExistentPath.GetParent();
			CheckPermission(parent, GetPermission(parent.GetParent()), true);
			CloseFileSystem();
		}

		/* get the permission of a file/directory */
		/// <exception cref="System.IO.IOException"/>
		private short GetPermission(Path path)
		{
			return fs.GetFileStatus(path).GetPermission().ToShort();
		}

		/* create a file/directory with the default umask and permission */
		/// <exception cref="System.IO.IOException"/>
		private void Create(TestDFSPermission.OpType op, Path name)
		{
			Create(op, name, DefaultUmask, new FsPermission(DefaultPermission));
		}

		/* create a file/directory with the given umask and permission */
		/// <exception cref="System.IO.IOException"/>
		private void Create(TestDFSPermission.OpType op, Path name, short umask, FsPermission
			 permission)
		{
			// set umask in configuration, converting to padded octal
			conf.Set(FsPermission.UmaskLabel, string.Format("%1$03o", umask));
			switch (op)
			{
				case TestDFSPermission.OpType.Create:
				{
					// create the file/directory
					FSDataOutputStream @out = fs.Create(name, permission, true, conf.GetInt(CommonConfigurationKeys
						.IoFileBufferSizeKey, 4096), fs.GetDefaultReplication(name), fs.GetDefaultBlockSize
						(name), null);
					@out.Close();
					break;
				}

				case TestDFSPermission.OpType.Mkdirs:
				{
					fs.Mkdirs(name, permission);
					break;
				}

				default:
				{
					throw new IOException("Unsupported operation: " + op);
				}
			}
		}

		/* create file/directory with the provided umask and permission; then it
		* checks if the permission is set correctly;
		* If the delete flag is true, delete the file afterwards; otherwise leave
		* it in the file system.
		*/
		/// <exception cref="System.Exception"/>
		private void CreateAndCheckPermission(TestDFSPermission.OpType op, Path name, short
			 umask, FsPermission permission, bool delete)
		{
			// create the file/directory
			Create(op, name, umask, permission);
			// get the short form of the permission
			short permissionNum = (DefaultPermission.Equals(permission)) ? MaxPermission : permission
				.ToShort();
			// get the expected permission
			short expectedPermission = (op == TestDFSPermission.OpType.Create) ? (short)(~umask
				 & permissionNum) : (short)(~umask & permissionNum);
			// check if permission is correctly set
			CheckPermission(name, expectedPermission, delete);
		}

		/* Check if the permission of a file/directory is the same as the
		* expected permission; If the delete flag is true, delete the
		* file/directory afterwards.
		*/
		/// <exception cref="System.IO.IOException"/>
		private void CheckPermission(Path name, short expectedPermission, bool delete)
		{
			try
			{
				// check its permission
				NUnit.Framework.Assert.AreEqual(GetPermission(name), expectedPermission);
			}
			finally
			{
				// delete the file
				if (delete)
				{
					fs.Delete(name, true);
				}
			}
		}

		/// <summary>
		/// check that ImmutableFsPermission can be used as the argument
		/// to setPermission
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestImmutableFsPermission()
		{
			fs = FileSystem.Get(conf);
			// set the permission of the root to be world-wide rwx
			fs.SetPermission(new Path("/"), FsPermission.CreateImmutable((short)0x1ff));
		}

		/* check if the ownership of a file/directory is set correctly */
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOwnership()
		{
			TestOwnership(TestDFSPermission.OpType.Create);
			// test file creation
			TestOwnership(TestDFSPermission.OpType.Mkdirs);
		}

		// test directory creation
		/* change a file/directory's owner and group.
		* if expectDeny is set, expect an AccessControlException.
		*/
		/// <exception cref="System.IO.IOException"/>
		private void SetOwner(Path path, string owner, string group, bool expectDeny)
		{
			try
			{
				string expectedOwner = (owner == null) ? GetOwner(path) : owner;
				string expectedGroup = (group == null) ? GetGroup(path) : group;
				fs.SetOwner(path, owner, group);
				CheckOwnership(path, expectedOwner, expectedGroup);
				NUnit.Framework.Assert.IsFalse(expectDeny);
			}
			catch (AccessControlException)
			{
				NUnit.Framework.Assert.IsTrue(expectDeny);
			}
		}

		/* check ownership is set correctly for a file or directory */
		/// <exception cref="System.Exception"/>
		private void TestOwnership(TestDFSPermission.OpType op)
		{
			// case 1: superuser create a file/directory
			fs = FileSystem.Get(conf);
			Create(op, FileDirPath, DefaultUmask, new FsPermission(DefaultPermission));
			CheckOwnership(FileDirPath, Superuser.GetShortUserName(), GetGroup(FileDirPath.GetParent
				()));
			// case 2: superuser changes FILE_DIR_PATH's owner to be <user1, group3>
			SetOwner(FileDirPath, User1.GetShortUserName(), Group3Name, false);
			// case 3: user1 changes FILE_DIR_PATH's owner to be user2
			Login(User1);
			SetOwner(FileDirPath, User2.GetShortUserName(), null, true);
			// case 4: user1 changes FILE_DIR_PATH's group to be group1 which it belongs
			// to
			SetOwner(FileDirPath, null, Group1Name, false);
			// case 5: user1 changes FILE_DIR_PATH's group to be group3
			// which it does not belong to
			SetOwner(FileDirPath, null, Group3Name, true);
			// case 6: user2 (non-owner) changes FILE_DIR_PATH's group to be group3
			Login(User2);
			SetOwner(FileDirPath, null, Group3Name, true);
			// case 7: user2 (non-owner) changes FILE_DIR_PATH's user to be user2
			SetOwner(FileDirPath, User2.GetShortUserName(), null, true);
			// delete the file/directory
			Login(Superuser);
			fs.Delete(FileDirPath, true);
		}

		/* Return the group owner of the file/directory */
		/// <exception cref="System.IO.IOException"/>
		private string GetGroup(Path path)
		{
			return fs.GetFileStatus(path).GetGroup();
		}

		/* Return the file owner of the file/directory */
		/// <exception cref="System.IO.IOException"/>
		private string GetOwner(Path path)
		{
			return fs.GetFileStatus(path).GetOwner();
		}

		/* check if ownership is set correctly */
		/// <exception cref="System.IO.IOException"/>
		private void CheckOwnership(Path name, string expectedOwner, string expectedGroup
			)
		{
			// check its owner and group
			FileStatus status = fs.GetFileStatus(name);
			NUnit.Framework.Assert.AreEqual(status.GetOwner(), expectedOwner);
			NUnit.Framework.Assert.AreEqual(status.GetGroup(), expectedGroup);
		}

		private const string AncestorName = "/ancestor";

		private const string ParentName = "parent";

		private const string FileName = "file";

		private const string DirName = "dir";

		private const string FileDirName = "filedir";

		private enum OpType
		{
			Create,
			Mkdirs,
			Open,
			SetReplication,
			GetFileinfo,
			IsDir,
			Exists,
			GetContentLength,
			List,
			Rename,
			Delete
		}

		/* Check if namenode performs permission checking correctly for
		* superuser, file owner, group owner, and other users */
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPermissionChecking()
		{
			try
			{
				fs = FileSystem.Get(conf);
				// set the permission of the root to be world-wide rwx
				fs.SetPermission(new Path("/"), new FsPermission((short)0x1ff));
				// create a directory hierarchy and sets random permission for each inode
				TestDFSPermission.PermissionGenerator ancestorPermissionGenerator = new TestDFSPermission.PermissionGenerator
					(r);
				TestDFSPermission.PermissionGenerator dirPermissionGenerator = new TestDFSPermission.PermissionGenerator
					(r);
				TestDFSPermission.PermissionGenerator filePermissionGenerator = new TestDFSPermission.PermissionGenerator
					(r);
				short[] ancestorPermissions = new short[NumTestPermissions];
				short[] parentPermissions = new short[NumTestPermissions];
				short[] permissions = new short[NumTestPermissions];
				Path[] ancestorPaths = new Path[NumTestPermissions];
				Path[] parentPaths = new Path[NumTestPermissions];
				Path[] filePaths = new Path[NumTestPermissions];
				Path[] dirPaths = new Path[NumTestPermissions];
				for (int i = 0; i < NumTestPermissions; i++)
				{
					// create ancestor directory
					ancestorPaths[i] = new Path(AncestorName + i);
					Create(TestDFSPermission.OpType.Mkdirs, ancestorPaths[i]);
					fs.SetOwner(ancestorPaths[i], User1Name, Group2Name);
					// create parent directory
					parentPaths[i] = new Path(ancestorPaths[i], ParentName + i);
					Create(TestDFSPermission.OpType.Mkdirs, parentPaths[i]);
					// change parent directory's ownership to be user1
					fs.SetOwner(parentPaths[i], User1Name, Group2Name);
					filePaths[i] = new Path(parentPaths[i], FileName + i);
					dirPaths[i] = new Path(parentPaths[i], DirName + i);
					// makes sure that each inode at the same level 
					// has a different permission
					ancestorPermissions[i] = ancestorPermissionGenerator.Next();
					parentPermissions[i] = dirPermissionGenerator.Next();
					permissions[i] = filePermissionGenerator.Next();
					fs.SetPermission(ancestorPaths[i], new FsPermission(ancestorPermissions[i]));
					fs.SetPermission(parentPaths[i], new FsPermission(parentPermissions[i]));
				}
				/* file owner */
				TestPermissionCheckingPerUser(User1, ancestorPermissions, parentPermissions, permissions
					, parentPaths, filePaths, dirPaths);
				/* group owner */
				TestPermissionCheckingPerUser(User2, ancestorPermissions, parentPermissions, permissions
					, parentPaths, filePaths, dirPaths);
				/* other owner */
				TestPermissionCheckingPerUser(User3, ancestorPermissions, parentPermissions, permissions
					, parentPaths, filePaths, dirPaths);
				/* super owner */
				TestPermissionCheckingPerUser(Superuser, ancestorPermissions, parentPermissions, 
					permissions, parentPaths, filePaths, dirPaths);
			}
			finally
			{
				fs.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAccessOwner()
		{
			FileSystem rootFs = FileSystem.Get(conf);
			Path p1 = new Path("/p1");
			rootFs.Mkdirs(p1);
			rootFs.SetOwner(p1, User1Name, Group1Name);
			fs = User1.DoAs(new _PrivilegedExceptionAction_434());
			fs.SetPermission(p1, new FsPermission((short)0x124));
			fs.Access(p1, FsAction.Read);
			try
			{
				fs.Access(p1, FsAction.Write);
				NUnit.Framework.Assert.Fail("The access call should have failed.");
			}
			catch (AccessControlException e)
			{
				NUnit.Framework.Assert.IsTrue("Permission denied messages must carry the username"
					, e.Message.Contains(User1Name));
				NUnit.Framework.Assert.IsTrue("Permission denied messages must carry the path parent"
					, e.Message.Contains(p1.GetParent().ToUri().GetPath()));
			}
			Path badPath = new Path("/bad/bad");
			try
			{
				fs.Access(badPath, FsAction.Read);
				NUnit.Framework.Assert.Fail("The access call should have failed");
			}
			catch (FileNotFoundException)
			{
			}
		}

		private sealed class _PrivilegedExceptionAction_434 : PrivilegedExceptionAction<FileSystem
			>
		{
			public _PrivilegedExceptionAction_434()
			{
			}

			/// <exception cref="System.Exception"/>
			public FileSystem Run()
			{
				return FileSystem.Get(TestDFSPermission.conf);
			}
		}

		// expected
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAccessGroupMember()
		{
			FileSystem rootFs = FileSystem.Get(conf);
			Path p2 = new Path("/p2");
			rootFs.Mkdirs(p2);
			rootFs.SetOwner(p2, UserGroupInformation.GetCurrentUser().GetShortUserName(), Group1Name
				);
			rootFs.SetPermission(p2, new FsPermission((short)0x1e0));
			fs = User1.DoAs(new _PrivilegedExceptionAction_469());
			fs.Access(p2, FsAction.Read);
			try
			{
				fs.Access(p2, FsAction.Execute);
				NUnit.Framework.Assert.Fail("The access call should have failed.");
			}
			catch (AccessControlException e)
			{
				NUnit.Framework.Assert.IsTrue("Permission denied messages must carry the username"
					, e.Message.Contains(User1Name));
				NUnit.Framework.Assert.IsTrue("Permission denied messages must carry the path parent"
					, e.Message.Contains(p2.GetParent().ToUri().GetPath()));
			}
		}

		private sealed class _PrivilegedExceptionAction_469 : PrivilegedExceptionAction<FileSystem
			>
		{
			public _PrivilegedExceptionAction_469()
			{
			}

			/// <exception cref="System.Exception"/>
			public FileSystem Run()
			{
				return FileSystem.Get(TestDFSPermission.conf);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAccessOthers()
		{
			FileSystem rootFs = FileSystem.Get(conf);
			Path p3 = new Path("/p3");
			rootFs.Mkdirs(p3);
			rootFs.SetPermission(p3, new FsPermission((short)0x1fc));
			fs = User1.DoAs(new _PrivilegedExceptionAction_494());
			fs.Access(p3, FsAction.Read);
			try
			{
				fs.Access(p3, FsAction.ReadWrite);
				NUnit.Framework.Assert.Fail("The access call should have failed.");
			}
			catch (AccessControlException e)
			{
				NUnit.Framework.Assert.IsTrue("Permission denied messages must carry the username"
					, e.Message.Contains(User1Name));
				NUnit.Framework.Assert.IsTrue("Permission denied messages must carry the path parent"
					, e.Message.Contains(p3.GetParent().ToUri().GetPath()));
			}
		}

		private sealed class _PrivilegedExceptionAction_494 : PrivilegedExceptionAction<FileSystem
			>
		{
			public _PrivilegedExceptionAction_494()
			{
			}

			/// <exception cref="System.Exception"/>
			public FileSystem Run()
			{
				return FileSystem.Get(TestDFSPermission.conf);
			}
		}

		/* Check if namenode performs permission checking correctly
		* for the given user for operations mkdir, open, setReplication,
		* getFileInfo, isDirectory, exists, getContentLength, list, rename,
		* and delete */
		/// <exception cref="System.Exception"/>
		private void TestPermissionCheckingPerUser(UserGroupInformation ugi, short[] ancestorPermission
			, short[] parentPermission, short[] filePermission, Path[] parentDirs, Path[] files
			, Path[] dirs)
		{
			bool[] isDirEmpty = new bool[NumTestPermissions];
			Login(Superuser);
			for (int i = 0; i < NumTestPermissions; i++)
			{
				Create(TestDFSPermission.OpType.Create, files[i]);
				Create(TestDFSPermission.OpType.Mkdirs, dirs[i]);
				fs.SetOwner(files[i], User1Name, Group2Name);
				fs.SetOwner(dirs[i], User1Name, Group2Name);
				CheckOwnership(dirs[i], User1Name, Group2Name);
				CheckOwnership(files[i], User1Name, Group2Name);
				FsPermission fsPermission = new FsPermission(filePermission[i]);
				fs.SetPermission(files[i], fsPermission);
				fs.SetPermission(dirs[i], fsPermission);
				isDirEmpty[i] = (fs.ListStatus(dirs[i]).Length == 0);
			}
			Login(ugi);
			for (int i_1 = 0; i_1 < NumTestPermissions; i_1++)
			{
				TestCreateMkdirs(ugi, new Path(parentDirs[i_1], FileDirName), ancestorPermission[
					i_1], parentPermission[i_1]);
				TestOpen(ugi, files[i_1], ancestorPermission[i_1], parentPermission[i_1], filePermission
					[i_1]);
				TestSetReplication(ugi, files[i_1], ancestorPermission[i_1], parentPermission[i_1
					], filePermission[i_1]);
				TestSetTimes(ugi, files[i_1], ancestorPermission[i_1], parentPermission[i_1], filePermission
					[i_1]);
				TestStats(ugi, files[i_1], ancestorPermission[i_1], parentPermission[i_1]);
				TestList(ugi, files[i_1], dirs[i_1], ancestorPermission[i_1], parentPermission[i_1
					], filePermission[i_1]);
				int next = i_1 == NumTestPermissions - 1 ? 0 : i_1 + 1;
				TestRename(ugi, files[i_1], files[next], ancestorPermission[i_1], parentPermission
					[i_1], ancestorPermission[next], parentPermission[next]);
				TestDeleteFile(ugi, files[i_1], ancestorPermission[i_1], parentPermission[i_1]);
				TestDeleteDir(ugi, dirs[i_1], ancestorPermission[i_1], parentPermission[i_1], filePermission
					[i_1], null, isDirEmpty[i_1]);
			}
			// test non existent file
			CheckNonExistentFile();
		}

		private class PermissionGenerator
		{
			private readonly Random r;

			private readonly short[] permissions = new short[MaxPermission + 1];

			private int numLeft = MaxPermission + 1;

			internal PermissionGenerator(Random r)
			{
				/* A random permission generator that guarantees that each permission
				* value is generated only once.
				*/
				this.r = r;
				for (int i = 0; i <= MaxPermission; i++)
				{
					permissions[i] = (short)i;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual short Next()
			{
				if (numLeft == 0)
				{
					throw new IOException("No more permission is avaialbe");
				}
				int index = r.Next(numLeft);
				// choose which permission to return
				numLeft--;
				// decrement the counter
				// swap the chosen permission with last available permission in the array
				short temp = permissions[numLeft];
				permissions[numLeft] = permissions[index];
				permissions[index] = temp;
				return permissions[numLeft];
			}
		}

		internal abstract class PermissionVerifier
		{
			protected internal Path path;

			protected internal short ancestorPermission;

			protected internal short parentPermission;

			private short permission;

			protected internal short requiredAncestorPermission;

			protected internal short requiredParentPermission;

			protected internal short requiredPermission;

			protected internal const short opAncestorPermission = TestDFSPermission.SearchMask;

			protected internal short opParentPermission;

			protected internal short opPermission;

			protected internal UserGroupInformation ugi;

			/* A base class that verifies the permission checking is correct
			* for an operation */
			/* initialize */
			protected internal virtual void Set(Path path, short ancestorPermission, short parentPermission
				, short permission)
			{
				this.path = path;
				this.ancestorPermission = ancestorPermission;
				this.parentPermission = parentPermission;
				this.permission = permission;
				this.SetOpPermission();
				this.ugi = null;
			}

			/* Perform an operation and verify if the permission checking is correct */
			/// <exception cref="System.IO.IOException"/>
			internal virtual void VerifyPermission(UserGroupInformation ugi)
			{
				if (this.ugi != ugi)
				{
					this.SetRequiredPermissions(ugi);
					this.ugi = ugi;
				}
				try
				{
					try
					{
						this.Call();
						NUnit.Framework.Assert.IsFalse(this.ExpectPermissionDeny());
					}
					catch (AccessControlException)
					{
						NUnit.Framework.Assert.IsTrue(this.ExpectPermissionDeny());
					}
				}
				catch (Exception ae)
				{
					this.LogPermissions();
					throw;
				}
			}

			/// <summary>Log the permissions and required permissions</summary>
			protected internal virtual void LogPermissions()
			{
				TestDFSPermission.Log.Info("required ancestor permission:" + Sharpen.Extensions.ToOctalString
					(this.requiredAncestorPermission));
				TestDFSPermission.Log.Info("ancestor permission: " + Sharpen.Extensions.ToOctalString
					(this.ancestorPermission));
				TestDFSPermission.Log.Info("required parent permission:" + Sharpen.Extensions.ToOctalString
					(this.requiredParentPermission));
				TestDFSPermission.Log.Info("parent permission: " + Sharpen.Extensions.ToOctalString
					(this.parentPermission));
				TestDFSPermission.Log.Info("required permission:" + Sharpen.Extensions.ToOctalString
					(this.requiredPermission));
				TestDFSPermission.Log.Info("permission: " + Sharpen.Extensions.ToOctalString(this
					.permission));
			}

			/* Return true if an AccessControlException is expected */
			protected internal virtual bool ExpectPermissionDeny()
			{
				return (this.requiredPermission & this.permission) != this.requiredPermission || 
					(this.requiredParentPermission & this.parentPermission) != this.requiredParentPermission
					 || (this.requiredAncestorPermission & this.ancestorPermission) != this.requiredAncestorPermission;
			}

			/* Set the permissions required to pass the permission checking */
			protected internal virtual void SetRequiredPermissions(UserGroupInformation ugi)
			{
				if (TestDFSPermission.Superuser.Equals(ugi))
				{
					this.requiredAncestorPermission = TestDFSPermission.SuperMask;
					this.requiredParentPermission = TestDFSPermission.SuperMask;
					this.requiredPermission = TestDFSPermission.SuperMask;
				}
				else
				{
					if (TestDFSPermission.User1.Equals(ugi))
					{
						this.requiredAncestorPermission = (short)(TestDFSPermission.PermissionVerifier.opAncestorPermission
							 & TestDFSPermission.OwnerMask);
						this.requiredParentPermission = (short)(this.opParentPermission & TestDFSPermission
							.OwnerMask);
						this.requiredPermission = (short)(this.opPermission & TestDFSPermission.OwnerMask
							);
					}
					else
					{
						if (TestDFSPermission.User2.Equals(ugi))
						{
							this.requiredAncestorPermission = (short)(TestDFSPermission.PermissionVerifier.opAncestorPermission
								 & TestDFSPermission.GroupMask);
							this.requiredParentPermission = (short)(this.opParentPermission & TestDFSPermission
								.GroupMask);
							this.requiredPermission = (short)(this.opPermission & TestDFSPermission.GroupMask
								);
						}
						else
						{
							if (TestDFSPermission.User3.Equals(ugi))
							{
								this.requiredAncestorPermission = (short)(TestDFSPermission.PermissionVerifier.opAncestorPermission
									 & TestDFSPermission.OtherMask);
								this.requiredParentPermission = (short)(this.opParentPermission & TestDFSPermission
									.OtherMask);
								this.requiredPermission = (short)(this.opPermission & TestDFSPermission.OtherMask
									);
							}
							else
							{
								throw new ArgumentException("Non-supported user: " + ugi);
							}
						}
					}
				}
			}

			/* Set the rwx permissions required for the operation */
			internal abstract void SetOpPermission();

			/* Perform the operation */
			/// <exception cref="System.IO.IOException"/>
			internal abstract void Call();

			internal PermissionVerifier(TestDFSPermission _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestDFSPermission _enclosing;
		}

		private const short SuperMask = 0;

		private const short ReadMask = 0x124;

		private const short WriteMask = 0x92;

		private const short SearchMask = 0x49;

		private const short NullMask = 0;

		private const short OwnerMask = 0x1c0;

		private const short GroupMask = 0x38;

		private const short OtherMask = 0x7;

		private class CreatePermissionVerifier : TestDFSPermission.PermissionVerifier
		{
			private TestDFSPermission.OpType opType;

			private bool cleanup = true;

			/* A class that verifies the permission checking is correct for create/mkdir*/
			/* initialize */
			protected internal virtual void Set(Path path, TestDFSPermission.OpType opType, short
				 ancestorPermission, short parentPermission)
			{
				base.Set(path, ancestorPermission, parentPermission, TestDFSPermission.NullMask);
				this.SetOpType(opType);
			}

			internal virtual void SetCleanup(bool cleanup)
			{
				this.cleanup = cleanup;
			}

			/* set if the operation mkdir/create */
			internal virtual void SetOpType(TestDFSPermission.OpType opType)
			{
				this.opType = opType;
			}

			internal override void SetOpPermission()
			{
				this.opParentPermission = TestDFSPermission.SearchMask | TestDFSPermission.WriteMask;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void Call()
			{
				this._enclosing.Create(this.opType, this.path);
				if (this.cleanup)
				{
					this._enclosing.fs.Delete(this.path, true);
				}
			}

			internal CreatePermissionVerifier(TestDFSPermission _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestDFSPermission _enclosing;
		}

		private readonly TestDFSPermission.CreatePermissionVerifier createVerifier;

		/* test if the permission checking of create/mkdir is correct */
		/// <exception cref="System.Exception"/>
		private void TestCreateMkdirs(UserGroupInformation ugi, Path path, short ancestorPermission
			, short parentPermission)
		{
			createVerifier.Set(path, TestDFSPermission.OpType.Mkdirs, ancestorPermission, parentPermission
				);
			createVerifier.VerifyPermission(ugi);
			createVerifier.SetOpType(TestDFSPermission.OpType.Create);
			createVerifier.SetCleanup(false);
			createVerifier.VerifyPermission(ugi);
			createVerifier.SetCleanup(true);
			createVerifier.VerifyPermission(ugi);
		}

		private class OpenPermissionVerifier : TestDFSPermission.PermissionVerifier
		{
			// test overWritten
			/* A class that verifies the permission checking is correct for open */
			internal override void SetOpPermission()
			{
				this.opParentPermission = TestDFSPermission.SearchMask;
				this.opPermission = TestDFSPermission.ReadMask;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void Call()
			{
				FSDataInputStream @in = this._enclosing.fs.Open(this.path);
				@in.Close();
			}

			internal OpenPermissionVerifier(TestDFSPermission _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestDFSPermission _enclosing;
		}

		private readonly TestDFSPermission.OpenPermissionVerifier openVerifier;

		/* test if the permission checking of open is correct */
		/// <exception cref="System.Exception"/>
		private void TestOpen(UserGroupInformation ugi, Path path, short ancestorPermission
			, short parentPermission, short filePermission)
		{
			openVerifier.Set(path, ancestorPermission, parentPermission, filePermission);
			openVerifier.VerifyPermission(ugi);
		}

		private class SetReplicationPermissionVerifier : TestDFSPermission.PermissionVerifier
		{
			/* A class that verifies the permission checking is correct for
			* setReplication */
			internal override void SetOpPermission()
			{
				this.opParentPermission = TestDFSPermission.SearchMask;
				this.opPermission = TestDFSPermission.WriteMask;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void Call()
			{
				this._enclosing.fs.SetReplication(this.path, (short)1);
			}

			internal SetReplicationPermissionVerifier(TestDFSPermission _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestDFSPermission _enclosing;
		}

		private readonly TestDFSPermission.SetReplicationPermissionVerifier replicatorVerifier;

		/* test if the permission checking of setReplication is correct */
		/// <exception cref="System.Exception"/>
		private void TestSetReplication(UserGroupInformation ugi, Path path, short ancestorPermission
			, short parentPermission, short filePermission)
		{
			replicatorVerifier.Set(path, ancestorPermission, parentPermission, filePermission
				);
			replicatorVerifier.VerifyPermission(ugi);
		}

		private class SetTimesPermissionVerifier : TestDFSPermission.PermissionVerifier
		{
			/* A class that verifies the permission checking is correct for
			* setTimes */
			internal override void SetOpPermission()
			{
				this.opParentPermission = TestDFSPermission.SearchMask;
				this.opPermission = TestDFSPermission.WriteMask;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void Call()
			{
				this._enclosing.fs.SetTimes(this.path, 100, 100);
				this._enclosing.fs.SetTimes(this.path, -1, 100);
				this._enclosing.fs.SetTimes(this.path, 100, -1);
			}

			internal SetTimesPermissionVerifier(TestDFSPermission _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestDFSPermission _enclosing;
		}

		private readonly TestDFSPermission.SetTimesPermissionVerifier timesVerifier;

		/* test if the permission checking of setReplication is correct */
		/// <exception cref="System.Exception"/>
		private void TestSetTimes(UserGroupInformation ugi, Path path, short ancestorPermission
			, short parentPermission, short filePermission)
		{
			timesVerifier.Set(path, ancestorPermission, parentPermission, filePermission);
			timesVerifier.VerifyPermission(ugi);
		}

		private class StatsPermissionVerifier : TestDFSPermission.PermissionVerifier
		{
			internal TestDFSPermission.OpType opType;

			/* A class that verifies the permission checking is correct for isDirectory,
			* exist,  getFileInfo, getContentSummary */
			/* initialize */
			internal virtual void Set(Path path, TestDFSPermission.OpType opType, short ancestorPermission
				, short parentPermission)
			{
				base.Set(path, ancestorPermission, parentPermission, TestDFSPermission.NullMask);
				this.SetOpType(opType);
			}

			/* set if operation is getFileInfo, isDirectory, exist, getContenSummary */
			internal virtual void SetOpType(TestDFSPermission.OpType opType)
			{
				this.opType = opType;
			}

			internal override void SetOpPermission()
			{
				this.opParentPermission = TestDFSPermission.SearchMask;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void Call()
			{
				switch (this.opType)
				{
					case TestDFSPermission.OpType.GetFileinfo:
					{
						this._enclosing.fs.GetFileStatus(this.path);
						break;
					}

					case TestDFSPermission.OpType.IsDir:
					{
						this._enclosing.fs.IsDirectory(this.path);
						break;
					}

					case TestDFSPermission.OpType.Exists:
					{
						this._enclosing.fs.Exists(this.path);
						break;
					}

					case TestDFSPermission.OpType.GetContentLength:
					{
						this._enclosing.fs.GetContentSummary(this.path).GetLength();
						break;
					}

					default:
					{
						throw new ArgumentException("Unexpected operation type: " + this.opType);
					}
				}
			}

			internal StatsPermissionVerifier(TestDFSPermission _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestDFSPermission _enclosing;
		}

		private readonly TestDFSPermission.StatsPermissionVerifier statsVerifier;

		/* test if the permission checking of isDirectory, exist,
		* getFileInfo, getContentSummary is correct */
		/// <exception cref="System.Exception"/>
		private void TestStats(UserGroupInformation ugi, Path path, short ancestorPermission
			, short parentPermission)
		{
			statsVerifier.Set(path, TestDFSPermission.OpType.GetFileinfo, ancestorPermission, 
				parentPermission);
			statsVerifier.VerifyPermission(ugi);
			statsVerifier.SetOpType(TestDFSPermission.OpType.IsDir);
			statsVerifier.VerifyPermission(ugi);
			statsVerifier.SetOpType(TestDFSPermission.OpType.Exists);
			statsVerifier.VerifyPermission(ugi);
			statsVerifier.SetOpType(TestDFSPermission.OpType.GetContentLength);
			statsVerifier.VerifyPermission(ugi);
		}

		private enum InodeType
		{
			File,
			Dir
		}

		private class ListPermissionVerifier : TestDFSPermission.PermissionVerifier
		{
			private TestDFSPermission.InodeType inodeType;

			/* A class that verifies the permission checking is correct for list */
			/* initialize */
			internal virtual void Set(Path path, TestDFSPermission.InodeType inodeType, short
				 ancestorPermission, short parentPermission, short permission)
			{
				this.inodeType = inodeType;
				base.Set(path, ancestorPermission, parentPermission, permission);
			}

			/* set if the given path is a file/directory */
			internal virtual void SetInodeType(Path path, TestDFSPermission.InodeType inodeType
				)
			{
				this.path = path;
				this.inodeType = inodeType;
				this.SetOpPermission();
				this.ugi = null;
			}

			internal override void SetOpPermission()
			{
				this.opParentPermission = TestDFSPermission.SearchMask;
				switch (this.inodeType)
				{
					case TestDFSPermission.InodeType.File:
					{
						this.opPermission = 0;
						break;
					}

					case TestDFSPermission.InodeType.Dir:
					{
						this.opPermission = TestDFSPermission.ReadMask | TestDFSPermission.SearchMask;
						break;
					}

					default:
					{
						throw new ArgumentException("Illegal inode type: " + this.inodeType);
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void Call()
			{
				this._enclosing.fs.ListStatus(this.path);
			}

			internal ListPermissionVerifier(TestDFSPermission _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestDFSPermission _enclosing;
		}

		internal readonly TestDFSPermission.ListPermissionVerifier listVerifier;

		/* test if the permission checking of list is correct */
		/// <exception cref="System.Exception"/>
		private void TestList(UserGroupInformation ugi, Path file, Path dir, short ancestorPermission
			, short parentPermission, short filePermission)
		{
			listVerifier.Set(file, TestDFSPermission.InodeType.File, ancestorPermission, parentPermission
				, filePermission);
			listVerifier.VerifyPermission(ugi);
			listVerifier.SetInodeType(dir, TestDFSPermission.InodeType.Dir);
			listVerifier.VerifyPermission(ugi);
		}

		private class RenamePermissionVerifier : TestDFSPermission.PermissionVerifier
		{
			private Path dst;

			private short dstAncestorPermission;

			private short dstParentPermission;

			/* A class that verifies the permission checking is correct for rename */
			/* initialize */
			internal virtual void Set(Path src, short srcAncestorPermission, short srcParentPermission
				, Path dst, short dstAncestorPermission, short dstParentPermission)
			{
				base.Set(src, srcAncestorPermission, srcParentPermission, TestDFSPermission.NullMask
					);
				this.dst = dst;
				this.dstAncestorPermission = dstAncestorPermission;
				this.dstParentPermission = dstParentPermission;
			}

			internal override void SetOpPermission()
			{
				this.opParentPermission = TestDFSPermission.SearchMask | TestDFSPermission.WriteMask;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void Call()
			{
				this._enclosing.fs.Rename(this.path, this.dst);
			}

			protected internal override bool ExpectPermissionDeny()
			{
				return base.ExpectPermissionDeny() || (this.requiredParentPermission & this.dstParentPermission
					) != this.requiredParentPermission || (this.requiredAncestorPermission & this.dstAncestorPermission
					) != this.requiredAncestorPermission;
			}

			protected internal override void LogPermissions()
			{
				base.LogPermissions();
				TestDFSPermission.Log.Info("dst ancestor permission: " + Sharpen.Extensions.ToOctalString
					(this.dstAncestorPermission));
				TestDFSPermission.Log.Info("dst parent permission: " + Sharpen.Extensions.ToOctalString
					(this.dstParentPermission));
			}

			internal RenamePermissionVerifier(TestDFSPermission _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestDFSPermission _enclosing;
		}

		internal readonly TestDFSPermission.RenamePermissionVerifier renameVerifier;

		/* test if the permission checking of rename is correct */
		/// <exception cref="System.Exception"/>
		private void TestRename(UserGroupInformation ugi, Path src, Path dst, short srcAncestorPermission
			, short srcParentPermission, short dstAncestorPermission, short dstParentPermission
			)
		{
			renameVerifier.Set(src, srcAncestorPermission, srcParentPermission, dst, dstAncestorPermission
				, dstParentPermission);
			renameVerifier.VerifyPermission(ugi);
		}

		private class DeletePermissionVerifier : TestDFSPermission.PermissionVerifier
		{
			/* A class that verifies the permission checking is correct for delete */
			internal virtual void Set(Path path, short ancestorPermission, short parentPermission
				)
			{
				base.Set(path, ancestorPermission, parentPermission, TestDFSPermission.NullMask);
			}

			internal override void SetOpPermission()
			{
				this.opParentPermission = TestDFSPermission.SearchMask | TestDFSPermission.WriteMask;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void Call()
			{
				this._enclosing.fs.Delete(this.path, true);
			}

			internal DeletePermissionVerifier(TestDFSPermission _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestDFSPermission _enclosing;
		}

		private class DeleteDirPermissionVerifier : TestDFSPermission.DeletePermissionVerifier
		{
			private short[] childPermissions;

			/* A class that verifies the permission checking is correct for
			* directory deletion
			*/
			/* initialize */
			internal virtual void Set(Path path, short ancestorPermission, short parentPermission
				, short permission, short[] childPermissions)
			{
				this.Set(path, ancestorPermission, parentPermission, permission);
				this.childPermissions = childPermissions;
			}

			internal override void SetOpPermission()
			{
				this.opParentPermission = TestDFSPermission.SearchMask | TestDFSPermission.WriteMask;
				this.opPermission = TestDFSPermission.SearchMask | TestDFSPermission.WriteMask | 
					TestDFSPermission.ReadMask;
			}

			protected internal override bool ExpectPermissionDeny()
			{
				if (base.ExpectPermissionDeny())
				{
					return true;
				}
				else
				{
					if (this.childPermissions != null)
					{
						foreach (short childPermission in this.childPermissions)
						{
							if ((this.requiredPermission & childPermission) != this.requiredPermission)
							{
								return true;
							}
						}
					}
					return false;
				}
			}

			internal DeleteDirPermissionVerifier(TestDFSPermission _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestDFSPermission _enclosing;
		}

		private class DeleteEmptyDirPermissionVerifier : TestDFSPermission.DeleteDirPermissionVerifier
		{
			/* A class that verifies the permission checking is correct for
			* empty-directory deletion
			*/
			internal override void SetOpPermission()
			{
				this.opParentPermission = TestDFSPermission.SearchMask | TestDFSPermission.WriteMask;
				this.opPermission = TestDFSPermission.NullMask;
			}

			internal DeleteEmptyDirPermissionVerifier(TestDFSPermission _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestDFSPermission _enclosing;
		}

		internal readonly TestDFSPermission.DeletePermissionVerifier fileDeletionVerifier;

		/* test if the permission checking of file deletion is correct */
		/// <exception cref="System.Exception"/>
		private void TestDeleteFile(UserGroupInformation ugi, Path file, short ancestorPermission
			, short parentPermission)
		{
			fileDeletionVerifier.Set(file, ancestorPermission, parentPermission);
			fileDeletionVerifier.VerifyPermission(ugi);
		}

		internal readonly TestDFSPermission.DeleteDirPermissionVerifier dirDeletionVerifier;

		internal readonly TestDFSPermission.DeleteEmptyDirPermissionVerifier emptyDirDeletionVerifier;

		/* test if the permission checking of directory deletion is correct */
		/// <exception cref="System.Exception"/>
		private void TestDeleteDir(UserGroupInformation ugi, Path path, short ancestorPermission
			, short parentPermission, short permission, short[] childPermissions, bool isDirEmpty
			)
		{
			TestDFSPermission.DeleteDirPermissionVerifier ddpv = isDirEmpty ? emptyDirDeletionVerifier
				 : dirDeletionVerifier;
			ddpv.Set(path, ancestorPermission, parentPermission, permission, childPermissions
				);
			ddpv.VerifyPermission(ugi);
		}

		/* log into dfs as the given user */
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void Login(UserGroupInformation ugi)
		{
			if (fs != null)
			{
				fs.Close();
			}
			fs = DFSTestUtil.GetFileSystemAs(ugi, conf);
		}

		/* test non-existent file */
		private void CheckNonExistentFile()
		{
			try
			{
				NUnit.Framework.Assert.IsFalse(fs.Exists(NonExistentFile));
			}
			catch (IOException e)
			{
				CheckNoPermissionDeny(e);
			}
			try
			{
				fs.Open(NonExistentFile);
			}
			catch (IOException e)
			{
				CheckNoPermissionDeny(e);
			}
			try
			{
				fs.SetReplication(NonExistentFile, (short)4);
			}
			catch (IOException e)
			{
				CheckNoPermissionDeny(e);
			}
			try
			{
				fs.GetFileStatus(NonExistentFile);
			}
			catch (IOException e)
			{
				CheckNoPermissionDeny(e);
			}
			try
			{
				fs.GetContentSummary(NonExistentFile).GetLength();
			}
			catch (IOException e)
			{
				CheckNoPermissionDeny(e);
			}
			try
			{
				fs.ListStatus(NonExistentFile);
			}
			catch (IOException e)
			{
				CheckNoPermissionDeny(e);
			}
			try
			{
				fs.Delete(NonExistentFile, true);
			}
			catch (IOException e)
			{
				CheckNoPermissionDeny(e);
			}
			try
			{
				fs.Rename(NonExistentFile, new Path(NonExistentFile + ".txt"));
			}
			catch (IOException e)
			{
				CheckNoPermissionDeny(e);
			}
		}

		private void CheckNoPermissionDeny(IOException e)
		{
			NUnit.Framework.Assert.IsFalse(e is AccessControlException);
		}
	}
}
