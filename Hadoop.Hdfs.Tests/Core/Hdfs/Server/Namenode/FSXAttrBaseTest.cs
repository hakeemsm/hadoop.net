using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Tests NameNode interaction for all XAttr APIs.</summary>
	/// <remarks>
	/// Tests NameNode interaction for all XAttr APIs.
	/// This test suite covers restarting the NN, saving a new checkpoint.
	/// </remarks>
	public class FSXAttrBaseTest
	{
		protected internal static MiniDFSCluster dfsCluster;

		protected internal static Configuration conf;

		private static int pathCount = 0;

		protected internal static Path path;

		protected internal static Path filePath;

		protected internal static Path rawPath;

		protected internal static Path rawFilePath;

		protected internal const string name1 = "user.a1";

		protected internal static readonly byte[] value1 = new byte[] { unchecked((int)(0x31
			)), unchecked((int)(0x32)), unchecked((int)(0x33)) };

		protected internal static readonly byte[] newValue1 = new byte[] { unchecked((int
			)(0x31)), unchecked((int)(0x31)), unchecked((int)(0x31)) };

		protected internal const string name2 = "user.a2";

		protected internal static readonly byte[] value2 = new byte[] { unchecked((int)(0x37
			)), unchecked((int)(0x38)), unchecked((int)(0x39)) };

		protected internal const string name3 = "user.a3";

		protected internal const string name4 = "user.a4";

		protected internal const string raw1 = "raw.a1";

		protected internal const string raw2 = "raw.a2";

		protected internal const string security1 = HdfsServerConstants.SecurityXattrUnreadableBySuperuser;

		private static readonly int MaxSize = security1.Length;

		protected internal FileSystem fs;

		private static readonly UserGroupInformation Bruce = UserGroupInformation.CreateUserForTesting
			("bruce", new string[] {  });

		private static readonly UserGroupInformation Diana = UserGroupInformation.CreateUserForTesting
			("diana", new string[] {  });

		// XAttrs
		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void Init()
		{
			conf = new HdfsConfiguration();
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeXattrsEnabledKey, true);
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeAclsEnabledKey, true);
			conf.SetInt(DFSConfigKeys.DfsNamenodeMaxXattrsPerInodeKey, 3);
			conf.SetInt(DFSConfigKeys.DfsNamenodeMaxXattrSizeKey, MaxSize);
			InitCluster(true);
		}

		[AfterClass]
		public static void Shutdown()
		{
			if (dfsCluster != null)
			{
				dfsCluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			pathCount += 1;
			path = new Path("/p" + pathCount);
			filePath = new Path(path, "file");
			rawPath = new Path("/.reserved/raw/p" + pathCount);
			rawFilePath = new Path(rawPath, "file");
			InitFileSystem();
		}

		[TearDown]
		public virtual void DestroyFileSystems()
		{
			IOUtils.Cleanup(null, fs);
			fs = null;
		}

		/// <summary>
		/// Tests for creating xattr
		/// 1.
		/// </summary>
		/// <remarks>
		/// Tests for creating xattr
		/// 1. Create an xattr using XAttrSetFlag.CREATE.
		/// 2. Create an xattr which already exists and expect an exception.
		/// 3. Create multiple xattrs.
		/// 4. Restart NN and save checkpoint scenarios.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestCreateXAttr()
		{
			IDictionary<string, byte[]> expectedXAttrs = Maps.NewHashMap();
			expectedXAttrs[name1] = value1;
			expectedXAttrs[name2] = null;
			expectedXAttrs[security1] = null;
			DoTestCreateXAttr(filePath, expectedXAttrs);
			expectedXAttrs[raw1] = value1;
			DoTestCreateXAttr(rawFilePath, expectedXAttrs);
		}

		/// <exception cref="System.Exception"/>
		private void DoTestCreateXAttr(Path usePath, IDictionary<string, byte[]> expectedXAttrs
			)
		{
			DFSTestUtil.CreateFile(fs, usePath, 8192, (short)1, unchecked((int)(0xFEED)));
			fs.SetXAttr(usePath, name1, value1, EnumSet.Of(XAttrSetFlag.Create));
			IDictionary<string, byte[]> xattrs = fs.GetXAttrs(usePath);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 1);
			Assert.AssertArrayEquals(value1, xattrs[name1]);
			fs.RemoveXAttr(usePath, name1);
			xattrs = fs.GetXAttrs(usePath);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 0);
			// Create xattr which already exists.
			fs.SetXAttr(usePath, name1, value1, EnumSet.Of(XAttrSetFlag.Create));
			try
			{
				fs.SetXAttr(usePath, name1, value1, EnumSet.Of(XAttrSetFlag.Create));
				NUnit.Framework.Assert.Fail("Creating xattr which already exists should fail.");
			}
			catch (IOException)
			{
			}
			fs.RemoveXAttr(usePath, name1);
			// Create the xattrs
			foreach (KeyValuePair<string, byte[]> ent in expectedXAttrs)
			{
				fs.SetXAttr(usePath, ent.Key, ent.Value, EnumSet.Of(XAttrSetFlag.Create));
			}
			xattrs = fs.GetXAttrs(usePath);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, expectedXAttrs.Count);
			foreach (KeyValuePair<string, byte[]> ent_1 in expectedXAttrs)
			{
				byte[] val = (ent_1.Value == null) ? new byte[0] : ent_1.Value;
				Assert.AssertArrayEquals(val, xattrs[ent_1.Key]);
			}
			Restart(false);
			InitFileSystem();
			xattrs = fs.GetXAttrs(usePath);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, expectedXAttrs.Count);
			foreach (KeyValuePair<string, byte[]> ent_2 in expectedXAttrs)
			{
				byte[] val = (ent_2.Value == null) ? new byte[0] : ent_2.Value;
				Assert.AssertArrayEquals(val, xattrs[ent_2.Key]);
			}
			Restart(true);
			InitFileSystem();
			xattrs = fs.GetXAttrs(usePath);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, expectedXAttrs.Count);
			foreach (KeyValuePair<string, byte[]> ent_3 in expectedXAttrs)
			{
				byte[] val = (ent_3.Value == null) ? new byte[0] : ent_3.Value;
				Assert.AssertArrayEquals(val, xattrs[ent_3.Key]);
			}
			fs.Delete(usePath, false);
		}

		/// <summary>
		/// Tests for replacing xattr
		/// 1.
		/// </summary>
		/// <remarks>
		/// Tests for replacing xattr
		/// 1. Replace an xattr using XAttrSetFlag.REPLACE.
		/// 2. Replace an xattr which doesn't exist and expect an exception.
		/// 3. Create multiple xattrs and replace some.
		/// 4. Restart NN and save checkpoint scenarios.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestReplaceXAttr()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			fs.SetXAttr(path, name1, value1, EnumSet.Of(XAttrSetFlag.Create));
			fs.SetXAttr(path, name1, newValue1, EnumSet.Of(XAttrSetFlag.Replace));
			IDictionary<string, byte[]> xattrs = fs.GetXAttrs(path);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 1);
			Assert.AssertArrayEquals(newValue1, xattrs[name1]);
			fs.RemoveXAttr(path, name1);
			// Replace xattr which does not exist.
			try
			{
				fs.SetXAttr(path, name1, value1, EnumSet.Of(XAttrSetFlag.Replace));
				NUnit.Framework.Assert.Fail("Replacing xattr which does not exist should fail.");
			}
			catch (IOException)
			{
			}
			// Create two xattrs, then replace one
			fs.SetXAttr(path, name1, value1, EnumSet.Of(XAttrSetFlag.Create));
			fs.SetXAttr(path, name2, value2, EnumSet.Of(XAttrSetFlag.Create));
			fs.SetXAttr(path, name2, null, EnumSet.Of(XAttrSetFlag.Replace));
			xattrs = fs.GetXAttrs(path);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 2);
			Assert.AssertArrayEquals(value1, xattrs[name1]);
			Assert.AssertArrayEquals(new byte[0], xattrs[name2]);
			Restart(false);
			InitFileSystem();
			xattrs = fs.GetXAttrs(path);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 2);
			Assert.AssertArrayEquals(value1, xattrs[name1]);
			Assert.AssertArrayEquals(new byte[0], xattrs[name2]);
			Restart(true);
			InitFileSystem();
			xattrs = fs.GetXAttrs(path);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 2);
			Assert.AssertArrayEquals(value1, xattrs[name1]);
			Assert.AssertArrayEquals(new byte[0], xattrs[name2]);
			fs.RemoveXAttr(path, name1);
			fs.RemoveXAttr(path, name2);
		}

		/// <summary>
		/// Tests for setting xattr
		/// 1.
		/// </summary>
		/// <remarks>
		/// Tests for setting xattr
		/// 1. Set xattr with XAttrSetFlag.CREATE|XAttrSetFlag.REPLACE flag.
		/// 2. Set xattr with illegal name.
		/// 3. Set xattr without XAttrSetFlag.
		/// 4. Set xattr and total number exceeds max limit.
		/// 5. Set xattr and name is too long.
		/// 6. Set xattr and value is too long.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestSetXAttr()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			fs.SetXAttr(path, name1, value1, EnumSet.Of(XAttrSetFlag.Create, XAttrSetFlag.Replace
				));
			IDictionary<string, byte[]> xattrs = fs.GetXAttrs(path);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 1);
			Assert.AssertArrayEquals(value1, xattrs[name1]);
			fs.RemoveXAttr(path, name1);
			// Set xattr with null name
			try
			{
				fs.SetXAttr(path, null, value1, EnumSet.Of(XAttrSetFlag.Create, XAttrSetFlag.Replace
					));
				NUnit.Framework.Assert.Fail("Setting xattr with null name should fail.");
			}
			catch (ArgumentNullException e)
			{
				GenericTestUtils.AssertExceptionContains("XAttr name cannot be null", e);
			}
			catch (RemoteException e)
			{
				GenericTestUtils.AssertExceptionContains("XAttr name cannot be null", e);
			}
			// Set xattr with empty name: "user."
			try
			{
				fs.SetXAttr(path, "user.", value1, EnumSet.Of(XAttrSetFlag.Create, XAttrSetFlag.Replace
					));
				NUnit.Framework.Assert.Fail("Setting xattr with empty name should fail.");
			}
			catch (RemoteException e)
			{
				NUnit.Framework.Assert.AreEqual("Unexpected RemoteException: " + e, e.GetClassName
					(), typeof(HadoopIllegalArgumentException).GetCanonicalName());
				GenericTestUtils.AssertExceptionContains("XAttr name cannot be empty", e);
			}
			catch (HadoopIllegalArgumentException e)
			{
				GenericTestUtils.AssertExceptionContains("XAttr name cannot be empty", e);
			}
			// Set xattr with invalid name: "a1"
			try
			{
				fs.SetXAttr(path, "a1", value1, EnumSet.Of(XAttrSetFlag.Create, XAttrSetFlag.Replace
					));
				NUnit.Framework.Assert.Fail("Setting xattr with invalid name prefix or without " 
					+ "name prefix should fail.");
			}
			catch (RemoteException e)
			{
				NUnit.Framework.Assert.AreEqual("Unexpected RemoteException: " + e, e.GetClassName
					(), typeof(HadoopIllegalArgumentException).GetCanonicalName());
				GenericTestUtils.AssertExceptionContains("XAttr name must be prefixed", e);
			}
			catch (HadoopIllegalArgumentException e)
			{
				GenericTestUtils.AssertExceptionContains("XAttr name must be prefixed", e);
			}
			// Set xattr without XAttrSetFlag
			fs.SetXAttr(path, name1, value1);
			xattrs = fs.GetXAttrs(path);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 1);
			Assert.AssertArrayEquals(value1, xattrs[name1]);
			fs.RemoveXAttr(path, name1);
			// XAttr exists, and replace it using CREATE|REPLACE flag.
			fs.SetXAttr(path, name1, value1, EnumSet.Of(XAttrSetFlag.Create));
			fs.SetXAttr(path, name1, newValue1, EnumSet.Of(XAttrSetFlag.Create, XAttrSetFlag.
				Replace));
			xattrs = fs.GetXAttrs(path);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 1);
			Assert.AssertArrayEquals(newValue1, xattrs[name1]);
			fs.RemoveXAttr(path, name1);
			// Total number exceeds max limit
			fs.SetXAttr(path, name1, value1);
			fs.SetXAttr(path, name2, value2);
			fs.SetXAttr(path, name3, null);
			try
			{
				fs.SetXAttr(path, name4, null);
				NUnit.Framework.Assert.Fail("Setting xattr should fail if total number of xattrs "
					 + "for inode exceeds max limit.");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("Cannot add additional XAttr", e);
			}
			fs.RemoveXAttr(path, name1);
			fs.RemoveXAttr(path, name2);
			fs.RemoveXAttr(path, name3);
			// Name length exceeds max limit
			string longName = "user.0123456789abcdefX0123456789abcdefX0123456789abcdef";
			try
			{
				fs.SetXAttr(path, longName, null);
				NUnit.Framework.Assert.Fail("Setting xattr should fail if name is too long.");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("XAttr is too big", e);
				GenericTestUtils.AssertExceptionContains("total size is 50", e);
			}
			// Value length exceeds max limit
			byte[] longValue = new byte[MaxSize];
			try
			{
				fs.SetXAttr(path, "user.a", longValue);
				NUnit.Framework.Assert.Fail("Setting xattr should fail if value is too long.");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("XAttr is too big", e);
				GenericTestUtils.AssertExceptionContains("total size is 38", e);
			}
			// Name + value exactly equal the limit
			string name = "user.111";
			byte[] value = new byte[MaxSize - 3];
			fs.SetXAttr(path, name, value);
		}

		/// <summary>getxattr tests.</summary>
		/// <remarks>
		/// getxattr tests. Test that getxattr throws an exception if any of
		/// the following are true:
		/// an xattr that was requested doesn't exist
		/// the caller specifies an unknown namespace
		/// the caller doesn't have access to the namespace
		/// the caller doesn't have permission to get the value of the xattr
		/// the caller does not have search access to the parent directory
		/// the caller has only read access to the owning directory
		/// the caller has only search access to the owning directory and
		/// execute/search access to the actual entity
		/// the caller does not have search access to the owning directory and read
		/// access to the actual entity
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestGetXAttrs()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			fs.SetXAttr(path, name1, value1, EnumSet.Of(XAttrSetFlag.Create));
			fs.SetXAttr(path, name2, value2, EnumSet.Of(XAttrSetFlag.Create));
			/* An XAttr that was requested does not exist. */
			try
			{
				byte[] value = fs.GetXAttr(path, name3);
				NUnit.Framework.Assert.Fail("expected IOException");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("At least one of the attributes provided was not found."
					, e);
			}
			{
				/* Throw an exception if an xattr that was requested does not exist. */
				IList<string> names = Lists.NewArrayList();
				names.AddItem(name1);
				names.AddItem(name2);
				names.AddItem(name3);
				try
				{
					IDictionary<string, byte[]> xattrs = fs.GetXAttrs(path, names);
					NUnit.Framework.Assert.Fail("expected IOException");
				}
				catch (IOException e)
				{
					GenericTestUtils.AssertExceptionContains("At least one of the attributes provided was not found."
						, e);
				}
			}
			fs.RemoveXAttr(path, name1);
			fs.RemoveXAttr(path, name2);
			/* Unknown namespace should throw an exception. */
			try
			{
				byte[] xattr = fs.GetXAttr(path, "wackynamespace.foo");
				NUnit.Framework.Assert.Fail("expected IOException");
			}
			catch (Exception e)
			{
				GenericTestUtils.AssertExceptionContains("An XAttr name must be prefixed with " +
					 "user/trusted/security/system/raw, " + "followed by a '.'", e);
			}
			/*
			* The 'trusted' namespace should not be accessible and should throw an
			* exception.
			*/
			UserGroupInformation user = UserGroupInformation.CreateUserForTesting("user", new 
				string[] { "mygroup" });
			fs.SetXAttr(path, "trusted.foo", Sharpen.Runtime.GetBytesForString("1234"));
			try
			{
				user.DoAs(new _PrivilegedExceptionAction_446());
				NUnit.Framework.Assert.Fail("expected IOException");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("User doesn't have permission", e);
			}
			fs.SetXAttr(path, name1, Sharpen.Runtime.GetBytesForString("1234"));
			/*
			* Test that an exception is thrown if the caller doesn't have permission to
			* get the value of the xattr.
			*/
			/* Set access so that only the owner has access. */
			fs.SetPermission(path, new FsPermission((short)0x1c0));
			try
			{
				user.DoAs(new _PrivilegedExceptionAction_469());
				NUnit.Framework.Assert.Fail("expected IOException");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("Permission denied", e);
			}
			/*
			* The caller must have search access to the parent directory.
			*/
			Path childDir = new Path(path, "child" + pathCount);
			/* Set access to parent so that only the owner has access. */
			FileSystem.Mkdirs(fs, childDir, FsPermission.CreateImmutable((short)0x1c0));
			fs.SetXAttr(childDir, name1, Sharpen.Runtime.GetBytesForString("1234"));
			try
			{
				user.DoAs(new _PrivilegedExceptionAction_490(childDir));
				NUnit.Framework.Assert.Fail("expected IOException");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("Permission denied", e);
			}
			/* Check that read access to the owning directory is not good enough. */
			fs.SetPermission(path, new FsPermission((short)0x1c4));
			try
			{
				user.DoAs(new _PrivilegedExceptionAction_506(childDir));
				NUnit.Framework.Assert.Fail("expected IOException");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("Permission denied", e);
			}
			/*
			* Check that search access to the owning directory and search/execute
			* access to the actual entity with extended attributes is not good enough.
			*/
			fs.SetPermission(path, new FsPermission((short)0x1c1));
			fs.SetPermission(childDir, new FsPermission((short)0x1c1));
			try
			{
				user.DoAs(new _PrivilegedExceptionAction_526(childDir));
				NUnit.Framework.Assert.Fail("expected IOException");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("Permission denied", e);
			}
			/*
			* Check that search access to the owning directory and read access to
			* the actual entity with the extended attribute is good enough.
			*/
			fs.SetPermission(path, new FsPermission((short)0x1c1));
			fs.SetPermission(childDir, new FsPermission((short)0x1c4));
			user.DoAs(new _PrivilegedExceptionAction_545(childDir));
		}

		private sealed class _PrivilegedExceptionAction_446 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_446()
			{
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				FileSystem userFs = FSXAttrBaseTest.dfsCluster.GetFileSystem();
				byte[] xattr = userFs.GetXAttr(FSXAttrBaseTest.path, "trusted.foo");
				return null;
			}
		}

		private sealed class _PrivilegedExceptionAction_469 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_469()
			{
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				FileSystem userFs = FSXAttrBaseTest.dfsCluster.GetFileSystem();
				byte[] xattr = userFs.GetXAttr(FSXAttrBaseTest.path, FSXAttrBaseTest.name1);
				return null;
			}
		}

		private sealed class _PrivilegedExceptionAction_490 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_490(Path childDir)
			{
				this.childDir = childDir;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				FileSystem userFs = FSXAttrBaseTest.dfsCluster.GetFileSystem();
				byte[] xattr = userFs.GetXAttr(childDir, FSXAttrBaseTest.name1);
				return null;
			}

			private readonly Path childDir;
		}

		private sealed class _PrivilegedExceptionAction_506 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_506(Path childDir)
			{
				this.childDir = childDir;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				FileSystem userFs = FSXAttrBaseTest.dfsCluster.GetFileSystem();
				byte[] xattr = userFs.GetXAttr(childDir, FSXAttrBaseTest.name1);
				return null;
			}

			private readonly Path childDir;
		}

		private sealed class _PrivilegedExceptionAction_526 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_526(Path childDir)
			{
				this.childDir = childDir;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				FileSystem userFs = FSXAttrBaseTest.dfsCluster.GetFileSystem();
				byte[] xattr = userFs.GetXAttr(childDir, FSXAttrBaseTest.name1);
				return null;
			}

			private readonly Path childDir;
		}

		private sealed class _PrivilegedExceptionAction_545 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_545(Path childDir)
			{
				this.childDir = childDir;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				FileSystem userFs = FSXAttrBaseTest.dfsCluster.GetFileSystem();
				byte[] xattr = userFs.GetXAttr(childDir, FSXAttrBaseTest.name1);
				return null;
			}

			private readonly Path childDir;
		}

		/// <summary>
		/// Tests for removing xattr
		/// 1.
		/// </summary>
		/// <remarks>
		/// Tests for removing xattr
		/// 1. Remove xattr.
		/// 2. Restart NN and save checkpoint scenarios.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestRemoveXAttr()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			fs.SetXAttr(path, name1, value1, EnumSet.Of(XAttrSetFlag.Create));
			fs.SetXAttr(path, name2, value2, EnumSet.Of(XAttrSetFlag.Create));
			fs.SetXAttr(path, name3, null, EnumSet.Of(XAttrSetFlag.Create));
			fs.RemoveXAttr(path, name1);
			fs.RemoveXAttr(path, name2);
			IDictionary<string, byte[]> xattrs = fs.GetXAttrs(path);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 1);
			Assert.AssertArrayEquals(new byte[0], xattrs[name3]);
			Restart(false);
			InitFileSystem();
			xattrs = fs.GetXAttrs(path);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 1);
			Assert.AssertArrayEquals(new byte[0], xattrs[name3]);
			Restart(true);
			InitFileSystem();
			xattrs = fs.GetXAttrs(path);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 1);
			Assert.AssertArrayEquals(new byte[0], xattrs[name3]);
			fs.RemoveXAttr(path, name3);
		}

		/// <summary>removexattr tests.</summary>
		/// <remarks>
		/// removexattr tests. Test that removexattr throws an exception if any of
		/// the following are true:
		/// an xattr that was requested doesn't exist
		/// the caller specifies an unknown namespace
		/// the caller doesn't have access to the namespace
		/// the caller doesn't have permission to get the value of the xattr
		/// the caller does not have "execute" (scan) access to the parent directory
		/// the caller has only read access to the owning directory
		/// the caller has only execute access to the owning directory and execute
		/// access to the actual entity
		/// the caller does not have execute access to the owning directory and write
		/// access to the actual entity
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestRemoveXAttrPermissions()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			fs.SetXAttr(path, name1, value1, EnumSet.Of(XAttrSetFlag.Create));
			fs.SetXAttr(path, name2, value2, EnumSet.Of(XAttrSetFlag.Create));
			fs.SetXAttr(path, name3, null, EnumSet.Of(XAttrSetFlag.Create));
			try
			{
				fs.RemoveXAttr(path, name2);
				fs.RemoveXAttr(path, name2);
				NUnit.Framework.Assert.Fail("expected IOException");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("No matching attributes found", e);
			}
			/* Unknown namespace should throw an exception. */
			string expectedExceptionString = "An XAttr name must be prefixed " + "with user/trusted/security/system/raw, followed by a '.'";
			try
			{
				fs.RemoveXAttr(path, "wackynamespace.foo");
				NUnit.Framework.Assert.Fail("expected IOException");
			}
			catch (RemoteException e)
			{
				NUnit.Framework.Assert.AreEqual("Unexpected RemoteException: " + e, e.GetClassName
					(), typeof(HadoopIllegalArgumentException).GetCanonicalName());
				GenericTestUtils.AssertExceptionContains(expectedExceptionString, e);
			}
			catch (HadoopIllegalArgumentException e)
			{
				GenericTestUtils.AssertExceptionContains(expectedExceptionString, e);
			}
			/*
			* The 'trusted' namespace should not be accessible and should throw an
			* exception.
			*/
			UserGroupInformation user = UserGroupInformation.CreateUserForTesting("user", new 
				string[] { "mygroup" });
			fs.SetXAttr(path, "trusted.foo", Sharpen.Runtime.GetBytesForString("1234"));
			try
			{
				user.DoAs(new _PrivilegedExceptionAction_640());
				NUnit.Framework.Assert.Fail("expected IOException");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("User doesn't have permission", e);
			}
			finally
			{
				fs.RemoveXAttr(path, "trusted.foo");
			}
			/*
			* Test that an exception is thrown if the caller doesn't have permission to
			* get the value of the xattr.
			*/
			/* Set access so that only the owner has access. */
			fs.SetPermission(path, new FsPermission((short)0x1c0));
			try
			{
				user.DoAs(new _PrivilegedExceptionAction_663());
				NUnit.Framework.Assert.Fail("expected IOException");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("Permission denied", e);
			}
			/*
			* The caller must have "execute" (scan) access to the parent directory.
			*/
			Path childDir = new Path(path, "child" + pathCount);
			/* Set access to parent so that only the owner has access. */
			FileSystem.Mkdirs(fs, childDir, FsPermission.CreateImmutable((short)0x1c0));
			fs.SetXAttr(childDir, name1, Sharpen.Runtime.GetBytesForString("1234"));
			try
			{
				user.DoAs(new _PrivilegedExceptionAction_684(childDir));
				NUnit.Framework.Assert.Fail("expected IOException");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("Permission denied", e);
			}
			/* Check that read access to the owning directory is not good enough. */
			fs.SetPermission(path, new FsPermission((short)0x1c4));
			try
			{
				user.DoAs(new _PrivilegedExceptionAction_700(childDir));
				NUnit.Framework.Assert.Fail("expected IOException");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("Permission denied", e);
			}
			/*
			* Check that execute access to the owning directory and scan access to
			* the actual entity with extended attributes is not good enough.
			*/
			fs.SetPermission(path, new FsPermission((short)0x1c1));
			fs.SetPermission(childDir, new FsPermission((short)0x1c1));
			try
			{
				user.DoAs(new _PrivilegedExceptionAction_720(childDir));
				NUnit.Framework.Assert.Fail("expected IOException");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("Permission denied", e);
			}
			/*
			* Check that execute access to the owning directory and write access to
			* the actual entity with extended attributes is good enough.
			*/
			fs.SetPermission(path, new FsPermission((short)0x1c1));
			fs.SetPermission(childDir, new FsPermission((short)0x1c6));
			user.DoAs(new _PrivilegedExceptionAction_739(childDir));
		}

		private sealed class _PrivilegedExceptionAction_640 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_640()
			{
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				FileSystem userFs = FSXAttrBaseTest.dfsCluster.GetFileSystem();
				userFs.RemoveXAttr(FSXAttrBaseTest.path, "trusted.foo");
				return null;
			}
		}

		private sealed class _PrivilegedExceptionAction_663 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_663()
			{
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				FileSystem userFs = FSXAttrBaseTest.dfsCluster.GetFileSystem();
				userFs.RemoveXAttr(FSXAttrBaseTest.path, FSXAttrBaseTest.name1);
				return null;
			}
		}

		private sealed class _PrivilegedExceptionAction_684 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_684(Path childDir)
			{
				this.childDir = childDir;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				FileSystem userFs = FSXAttrBaseTest.dfsCluster.GetFileSystem();
				userFs.RemoveXAttr(childDir, FSXAttrBaseTest.name1);
				return null;
			}

			private readonly Path childDir;
		}

		private sealed class _PrivilegedExceptionAction_700 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_700(Path childDir)
			{
				this.childDir = childDir;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				FileSystem userFs = FSXAttrBaseTest.dfsCluster.GetFileSystem();
				userFs.RemoveXAttr(childDir, FSXAttrBaseTest.name1);
				return null;
			}

			private readonly Path childDir;
		}

		private sealed class _PrivilegedExceptionAction_720 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_720(Path childDir)
			{
				this.childDir = childDir;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				FileSystem userFs = FSXAttrBaseTest.dfsCluster.GetFileSystem();
				userFs.RemoveXAttr(childDir, FSXAttrBaseTest.name1);
				return null;
			}

			private readonly Path childDir;
		}

		private sealed class _PrivilegedExceptionAction_739 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_739(Path childDir)
			{
				this.childDir = childDir;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				FileSystem userFs = FSXAttrBaseTest.dfsCluster.GetFileSystem();
				userFs.RemoveXAttr(childDir, FSXAttrBaseTest.name1);
				return null;
			}

			private readonly Path childDir;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRenameFileWithXAttr()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			fs.SetXAttr(path, name1, value1, EnumSet.Of(XAttrSetFlag.Create));
			fs.SetXAttr(path, name2, value2, EnumSet.Of(XAttrSetFlag.Create));
			Path renamePath = new Path(path.ToString() + "-rename");
			fs.Rename(path, renamePath);
			IDictionary<string, byte[]> xattrs = fs.GetXAttrs(renamePath);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 2);
			Assert.AssertArrayEquals(value1, xattrs[name1]);
			Assert.AssertArrayEquals(value2, xattrs[name2]);
			fs.RemoveXAttr(renamePath, name1);
			fs.RemoveXAttr(renamePath, name2);
		}

		/// <summary>Test the listXAttrs api.</summary>
		/// <remarks>
		/// Test the listXAttrs api.
		/// listXAttrs on a path that doesn't exist.
		/// listXAttrs on a path with no XAttrs
		/// Check basic functionality.
		/// Check that read access to parent dir is not enough to get xattr names
		/// Check that write access to the parent dir is not enough to get names
		/// Check that execute/scan access to the parent dir is sufficient to get
		/// xattr names.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestListXAttrs()
		{
			UserGroupInformation user = UserGroupInformation.CreateUserForTesting("user", new 
				string[] { "mygroup" });
			/* listXAttrs in a path that doesn't exist. */
			try
			{
				fs.ListXAttrs(path);
				NUnit.Framework.Assert.Fail("expected FileNotFoundException");
			}
			catch (FileNotFoundException e)
			{
				GenericTestUtils.AssertExceptionContains("cannot find", e);
			}
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			/* listXAttrs on a path with no XAttrs.*/
			IList<string> noXAttrs = fs.ListXAttrs(path);
			NUnit.Framework.Assert.IsTrue("XAttrs were found?", noXAttrs.Count == 0);
			fs.SetXAttr(path, name1, value1, EnumSet.Of(XAttrSetFlag.Create));
			fs.SetXAttr(path, name2, value2, EnumSet.Of(XAttrSetFlag.Create));
			IList<string> xattrNames = fs.ListXAttrs(path);
			NUnit.Framework.Assert.IsTrue(xattrNames.Contains(name1));
			NUnit.Framework.Assert.IsTrue(xattrNames.Contains(name2));
			NUnit.Framework.Assert.IsTrue(xattrNames.Count == 2);
			/* Check that read access to parent dir is not enough to get xattr names. */
			fs.SetPermission(path, new FsPermission((short)0x1c4));
			Path childDir = new Path(path, "child" + pathCount);
			FileSystem.Mkdirs(fs, childDir, FsPermission.CreateImmutable((short)0x1c0));
			fs.SetXAttr(childDir, name1, Sharpen.Runtime.GetBytesForString("1234"));
			try
			{
				user.DoAs(new _PrivilegedExceptionAction_808(childDir));
				NUnit.Framework.Assert.Fail("expected IOException");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("Permission denied", e);
			}
			/*
			* Check that write access to the parent dir is not enough to get names.
			*/
			fs.SetPermission(path, new FsPermission((short)0x1c2));
			try
			{
				user.DoAs(new _PrivilegedExceptionAction_826(childDir));
				NUnit.Framework.Assert.Fail("expected IOException");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("Permission denied", e);
			}
			/*
			* Check that execute/scan access to the parent dir is sufficient to get
			* xattr names.
			*/
			fs.SetPermission(path, new FsPermission((short)0x1c1));
			user.DoAs(new _PrivilegedExceptionAction_844(childDir));
			/*
			* Test that xattrs in the "trusted" namespace are filtered correctly.
			*/
			fs.SetXAttr(childDir, "trusted.myxattr", Sharpen.Runtime.GetBytesForString("1234"
				));
			user.DoAs(new _PrivilegedExceptionAction_857(childDir));
			NUnit.Framework.Assert.IsTrue(fs.ListXAttrs(childDir).Count == 2);
		}

		private sealed class _PrivilegedExceptionAction_808 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_808(Path childDir)
			{
				this.childDir = childDir;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				FileSystem userFs = FSXAttrBaseTest.dfsCluster.GetFileSystem();
				userFs.ListXAttrs(childDir);
				return null;
			}

			private readonly Path childDir;
		}

		private sealed class _PrivilegedExceptionAction_826 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_826(Path childDir)
			{
				this.childDir = childDir;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				FileSystem userFs = FSXAttrBaseTest.dfsCluster.GetFileSystem();
				userFs.ListXAttrs(childDir);
				return null;
			}

			private readonly Path childDir;
		}

		private sealed class _PrivilegedExceptionAction_844 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_844(Path childDir)
			{
				this.childDir = childDir;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				FileSystem userFs = FSXAttrBaseTest.dfsCluster.GetFileSystem();
				userFs.ListXAttrs(childDir);
				return null;
			}

			private readonly Path childDir;
		}

		private sealed class _PrivilegedExceptionAction_857 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_857(Path childDir)
			{
				this.childDir = childDir;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				FileSystem userFs = FSXAttrBaseTest.dfsCluster.GetFileSystem();
				NUnit.Framework.Assert.IsTrue(userFs.ListXAttrs(childDir).Count == 1);
				return null;
			}

			private readonly Path childDir;
		}

		/// <summary>
		/// Steps:
		/// 1) Set xattrs on a file.
		/// </summary>
		/// <remarks>
		/// Steps:
		/// 1) Set xattrs on a file.
		/// 2) Remove xattrs from that file.
		/// 3) Save a checkpoint and restart NN.
		/// 4) Set xattrs again on the same file.
		/// 5) Remove xattrs from that file.
		/// 6) Restart NN without saving a checkpoint.
		/// 7) Set xattrs again on the same file.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestCleanupXAttrs()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			fs.SetXAttr(path, name1, value1, EnumSet.Of(XAttrSetFlag.Create));
			fs.SetXAttr(path, name2, value2, EnumSet.Of(XAttrSetFlag.Create));
			fs.RemoveXAttr(path, name1);
			fs.RemoveXAttr(path, name2);
			Restart(true);
			InitFileSystem();
			fs.SetXAttr(path, name1, value1, EnumSet.Of(XAttrSetFlag.Create));
			fs.SetXAttr(path, name2, value2, EnumSet.Of(XAttrSetFlag.Create));
			fs.RemoveXAttr(path, name1);
			fs.RemoveXAttr(path, name2);
			Restart(false);
			InitFileSystem();
			fs.SetXAttr(path, name1, value1, EnumSet.Of(XAttrSetFlag.Create));
			fs.SetXAttr(path, name2, value2, EnumSet.Of(XAttrSetFlag.Create));
			fs.RemoveXAttr(path, name1);
			fs.RemoveXAttr(path, name2);
			fs.SetXAttr(path, name1, value1, EnumSet.Of(XAttrSetFlag.Create));
			fs.SetXAttr(path, name2, value2, EnumSet.Of(XAttrSetFlag.Create));
			IDictionary<string, byte[]> xattrs = fs.GetXAttrs(path);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 2);
			Assert.AssertArrayEquals(value1, xattrs[name1]);
			Assert.AssertArrayEquals(value2, xattrs[name2]);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestXAttrAcl()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			fs.SetOwner(path, Bruce.GetUserName(), null);
			FileSystem fsAsBruce = CreateFileSystem(Bruce);
			FileSystem fsAsDiana = CreateFileSystem(Diana);
			fsAsBruce.SetXAttr(path, name1, value1);
			IDictionary<string, byte[]> xattrs;
			try
			{
				xattrs = fsAsDiana.GetXAttrs(path);
				NUnit.Framework.Assert.Fail("Diana should not have read access to get xattrs");
			}
			catch (AccessControlException)
			{
			}
			// Ignore
			// Give Diana read permissions to the path
			fsAsBruce.ModifyAclEntries(path, Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, Diana.GetUserName(), FsAction.Read)));
			xattrs = fsAsDiana.GetXAttrs(path);
			Assert.AssertArrayEquals(value1, xattrs[name1]);
			try
			{
				fsAsDiana.RemoveXAttr(path, name1);
				NUnit.Framework.Assert.Fail("Diana should not have write access to remove xattrs"
					);
			}
			catch (AccessControlException)
			{
			}
			// Ignore
			try
			{
				fsAsDiana.SetXAttr(path, name2, value2);
				NUnit.Framework.Assert.Fail("Diana should not have write access to set xattrs");
			}
			catch (AccessControlException)
			{
			}
			// Ignore
			fsAsBruce.ModifyAclEntries(path, Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, Diana.GetUserName(), FsAction.All)));
			fsAsDiana.SetXAttr(path, name2, value2);
			Assert.AssertArrayEquals(value2, fsAsDiana.GetXAttrs(path)[name2]);
			fsAsDiana.RemoveXAttr(path, name1);
			fsAsDiana.RemoveXAttr(path, name2);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRawXAttrs()
		{
			UserGroupInformation user = UserGroupInformation.CreateUserForTesting("user", new 
				string[] { "mygroup" });
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			fs.SetXAttr(rawPath, raw1, value1, EnumSet.Of(XAttrSetFlag.Create, XAttrSetFlag.Replace
				));
			{
				// getXAttr
				byte[] value = fs.GetXAttr(rawPath, raw1);
				Assert.AssertArrayEquals(value, value1);
			}
			{
				// getXAttrs
				IDictionary<string, byte[]> xattrs = fs.GetXAttrs(rawPath);
				NUnit.Framework.Assert.AreEqual(xattrs.Count, 1);
				Assert.AssertArrayEquals(value1, xattrs[raw1]);
				fs.RemoveXAttr(rawPath, raw1);
			}
			{
				// replace and re-get
				fs.SetXAttr(rawPath, raw1, value1, EnumSet.Of(XAttrSetFlag.Create));
				fs.SetXAttr(rawPath, raw1, newValue1, EnumSet.Of(XAttrSetFlag.Create, XAttrSetFlag
					.Replace));
				IDictionary<string, byte[]> xattrs = fs.GetXAttrs(rawPath);
				NUnit.Framework.Assert.AreEqual(xattrs.Count, 1);
				Assert.AssertArrayEquals(newValue1, xattrs[raw1]);
				fs.RemoveXAttr(rawPath, raw1);
			}
			{
				// listXAttrs on rawPath ensuring raw.* xattrs are returned
				fs.SetXAttr(rawPath, raw1, value1, EnumSet.Of(XAttrSetFlag.Create));
				fs.SetXAttr(rawPath, raw2, value2, EnumSet.Of(XAttrSetFlag.Create));
				IList<string> xattrNames = fs.ListXAttrs(rawPath);
				NUnit.Framework.Assert.IsTrue(xattrNames.Contains(raw1));
				NUnit.Framework.Assert.IsTrue(xattrNames.Contains(raw2));
				NUnit.Framework.Assert.IsTrue(xattrNames.Count == 2);
				fs.RemoveXAttr(rawPath, raw1);
				fs.RemoveXAttr(rawPath, raw2);
			}
			{
				// listXAttrs on non-rawPath ensuring no raw.* xattrs returned
				fs.SetXAttr(rawPath, raw1, value1, EnumSet.Of(XAttrSetFlag.Create));
				fs.SetXAttr(rawPath, raw2, value2, EnumSet.Of(XAttrSetFlag.Create));
				IList<string> xattrNames = fs.ListXAttrs(path);
				NUnit.Framework.Assert.IsTrue(xattrNames.Count == 0);
				fs.RemoveXAttr(rawPath, raw1);
				fs.RemoveXAttr(rawPath, raw2);
			}
			{
				/*
				* Test non-root user operations in the "raw.*" namespace.
				*/
				user.DoAs(new _PrivilegedExceptionAction_1020());
			}
			{
				// Test that non-root can not set xattrs in the "raw.*" namespace
				// non-raw path
				// ignore
				// raw path
				// ignore
				// Test that non-root can not do getXAttrs in the "raw.*" namespace
				// non-raw path
				// ignore
				// raw path
				// ignore
				// Test that non-root can not do getXAttr in the "raw.*" namespace
				// non-raw path
				// ignore
				// raw path
				// ignore
				/*
				* Test that non-root can not do getXAttr in the "raw.*" namespace
				*/
				fs.SetXAttr(rawPath, raw1, value1);
				user.DoAs(new _PrivilegedExceptionAction_1084());
				// non-raw path
				// ignore
				// raw path
				// ignore
				/*
				* Test that only root can see raw.* xattrs returned from listXAttr
				* and non-root can't do listXAttrs on /.reserved/raw.
				*/
				// non-raw path
				// raw path
				// ignore
				fs.RemoveXAttr(rawPath, raw1);
			}
		}

		private sealed class _PrivilegedExceptionAction_1020 : PrivilegedExceptionAction<
			object>
		{
			public _PrivilegedExceptionAction_1020()
			{
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				FileSystem userFs = FSXAttrBaseTest.dfsCluster.GetFileSystem();
				try
				{
					userFs.SetXAttr(FSXAttrBaseTest.path, FSXAttrBaseTest.raw1, FSXAttrBaseTest.value1
						);
					NUnit.Framework.Assert.Fail("setXAttr should have thrown");
				}
				catch (AccessControlException)
				{
				}
				try
				{
					userFs.SetXAttr(FSXAttrBaseTest.rawPath, FSXAttrBaseTest.raw1, FSXAttrBaseTest.value1
						);
					NUnit.Framework.Assert.Fail("setXAttr should have thrown");
				}
				catch (AccessControlException)
				{
				}
				try
				{
					userFs.GetXAttrs(FSXAttrBaseTest.rawPath);
					NUnit.Framework.Assert.Fail("getXAttrs should have thrown");
				}
				catch (AccessControlException)
				{
				}
				try
				{
					userFs.GetXAttrs(FSXAttrBaseTest.path);
					NUnit.Framework.Assert.Fail("getXAttrs should have thrown");
				}
				catch (AccessControlException)
				{
				}
				try
				{
					userFs.GetXAttr(FSXAttrBaseTest.rawPath, FSXAttrBaseTest.raw1);
					NUnit.Framework.Assert.Fail("getXAttr should have thrown");
				}
				catch (AccessControlException)
				{
				}
				try
				{
					userFs.GetXAttr(FSXAttrBaseTest.path, FSXAttrBaseTest.raw1);
					NUnit.Framework.Assert.Fail("getXAttr should have thrown");
				}
				catch (AccessControlException)
				{
				}
				return null;
			}
		}

		private sealed class _PrivilegedExceptionAction_1084 : PrivilegedExceptionAction<
			object>
		{
			public _PrivilegedExceptionAction_1084()
			{
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				FileSystem userFs = FSXAttrBaseTest.dfsCluster.GetFileSystem();
				try
				{
					userFs.GetXAttr(FSXAttrBaseTest.rawPath, FSXAttrBaseTest.raw1);
					NUnit.Framework.Assert.Fail("getXAttr should have thrown");
				}
				catch (AccessControlException)
				{
				}
				try
				{
					userFs.GetXAttr(FSXAttrBaseTest.path, FSXAttrBaseTest.raw1);
					NUnit.Framework.Assert.Fail("getXAttr should have thrown");
				}
				catch (AccessControlException)
				{
				}
				IList<string> xattrNames = userFs.ListXAttrs(FSXAttrBaseTest.path);
				NUnit.Framework.Assert.IsTrue(xattrNames.Count == 0);
				try
				{
					userFs.ListXAttrs(FSXAttrBaseTest.rawPath);
					NUnit.Framework.Assert.Fail("listXAttrs on raw path should have thrown");
				}
				catch (AccessControlException)
				{
				}
				return null;
			}
		}

		/// <summary>
		/// This tests the "unreadable by superuser" xattr which denies access to a
		/// file for the superuser.
		/// </summary>
		/// <remarks>
		/// This tests the "unreadable by superuser" xattr which denies access to a
		/// file for the superuser. See HDFS-6705 for details.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestUnreadableBySuperuserXAttr()
		{
			// Run tests as superuser...
			DoTestUnreadableBySuperuserXAttr(fs, true);
			// ...and again as non-superuser
			UserGroupInformation user = UserGroupInformation.CreateUserForTesting("user", new 
				string[] { "mygroup" });
			user.DoAs(new _PrivilegedExceptionAction_1138(this));
		}

		private sealed class _PrivilegedExceptionAction_1138 : PrivilegedExceptionAction<
			object>
		{
			public _PrivilegedExceptionAction_1138(FSXAttrBaseTest _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				FileSystem userFs = FSXAttrBaseTest.dfsCluster.GetFileSystem();
				this._enclosing.DoTestUnreadableBySuperuserXAttr(userFs, false);
				return null;
			}

			private readonly FSXAttrBaseTest _enclosing;
		}

		/// <exception cref="System.Exception"/>
		private void DoTestUnreadableBySuperuserXAttr(FileSystem userFs, bool expectOpenFailure
			)
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1ff));
			DFSTestUtil.CreateFile(userFs, filePath, 8192, (short)1, unchecked((int)(0xFEED))
				);
			try
			{
				DoTUBSXAInt(userFs, expectOpenFailure);
				// Deleting the file is allowed.
				userFs.Delete(filePath, false);
			}
			finally
			{
				fs.Delete(path, true);
			}
		}

		/// <exception cref="System.Exception"/>
		private void DoTUBSXAInt(FileSystem userFs, bool expectOpenFailure)
		{
			// Test that xattr can't be set on a dir
			try
			{
				userFs.SetXAttr(path, security1, null, EnumSet.Of(XAttrSetFlag.Create));
			}
			catch (IOException e)
			{
				// WebHDFS throws IOException instead of RemoteException
				GenericTestUtils.AssertExceptionContains("Can only set '" + HdfsServerConstants.SecurityXattrUnreadableBySuperuser
					 + "' on a file", e);
			}
			// Test that xattr can actually be set. Repeatedly.
			userFs.SetXAttr(filePath, security1, null, EnumSet.Of(XAttrSetFlag.Create));
			VerifySecurityXAttrExists(userFs);
			userFs.SetXAttr(filePath, security1, null, EnumSet.Of(XAttrSetFlag.Create, XAttrSetFlag
				.Replace));
			VerifySecurityXAttrExists(userFs);
			// Test that the xattr can't be deleted by anyone.
			try
			{
				userFs.RemoveXAttr(filePath, security1);
				NUnit.Framework.Assert.Fail("Removing security xattr should fail.");
			}
			catch (AccessControlException e)
			{
				GenericTestUtils.AssertExceptionContains("The xattr '" + HdfsServerConstants.SecurityXattrUnreadableBySuperuser
					 + "' can not be deleted.", e);
			}
			// Test that xattr can be read.
			VerifySecurityXAttrExists(userFs);
			// Test that a value can't be set for the xattr.
			try
			{
				userFs.SetXAttr(filePath, security1, value1, EnumSet.Of(XAttrSetFlag.Replace));
				NUnit.Framework.Assert.Fail("Should have thrown on attempt to set value");
			}
			catch (AccessControlException e)
			{
				GenericTestUtils.AssertExceptionContains("Values are not allowed", e);
			}
			// Test that unreadable by superuser xattr appears in listXAttrs results
			// (for superuser and non-superuser)
			IList<string> xattrNames = userFs.ListXAttrs(filePath);
			NUnit.Framework.Assert.IsTrue(xattrNames.Contains(security1));
			NUnit.Framework.Assert.IsTrue(xattrNames.Count == 1);
			VerifyFileAccess(userFs, expectOpenFailure);
			// Rename of the file is allowed by anyone.
			Path toPath = new Path(filePath.ToString() + "x");
			userFs.Rename(filePath, toPath);
			userFs.Rename(toPath, filePath);
		}

		/// <exception cref="System.Exception"/>
		private void VerifySecurityXAttrExists(FileSystem userFs)
		{
			try
			{
				IDictionary<string, byte[]> xattrs = userFs.GetXAttrs(filePath);
				NUnit.Framework.Assert.AreEqual(1, xattrs.Count);
				NUnit.Framework.Assert.IsNotNull(xattrs[security1]);
				Assert.AssertArrayEquals("expected empty byte[] from getXAttr", new byte[0], userFs
					.GetXAttr(filePath, security1));
			}
			catch (AccessControlException)
			{
				NUnit.Framework.Assert.Fail("getXAttrs failed but expected it to succeed");
			}
		}

		/// <exception cref="System.Exception"/>
		private void VerifyFileAccess(FileSystem userFs, bool expectOpenFailure)
		{
			// Test that a file with the xattr can or can't be opened.
			try
			{
				userFs.Open(filePath);
				NUnit.Framework.Assert.IsFalse("open succeeded but expected it to fail", expectOpenFailure
					);
			}
			catch (AccessControlException)
			{
				NUnit.Framework.Assert.IsTrue("open failed but expected it to succeed", expectOpenFailure
					);
			}
		}

		/// <summary>Creates a FileSystem for the super-user.</summary>
		/// <returns>FileSystem for super-user</returns>
		/// <exception cref="System.Exception">if creation fails</exception>
		protected internal virtual FileSystem CreateFileSystem()
		{
			return dfsCluster.GetFileSystem();
		}

		/// <summary>Creates a FileSystem for a specific user.</summary>
		/// <param name="user">UserGroupInformation specific user</param>
		/// <returns>FileSystem for specific user</returns>
		/// <exception cref="System.Exception">if creation fails</exception>
		protected internal virtual FileSystem CreateFileSystem(UserGroupInformation user)
		{
			return DFSTestUtil.GetFileSystemAs(user, conf);
		}

		/// <summary>Initializes all FileSystem instances used in the tests.</summary>
		/// <exception cref="System.Exception">if initialization fails</exception>
		private void InitFileSystem()
		{
			fs = CreateFileSystem();
		}

		/// <summary>
		/// Initialize the cluster, wait for it to become active, and get FileSystem
		/// instances for our test users.
		/// </summary>
		/// <param name="format">if true, format the NameNode and DataNodes before starting up
		/// 	</param>
		/// <exception cref="System.Exception">if any step fails</exception>
		protected internal static void InitCluster(bool format)
		{
			dfsCluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Format(format).Build
				();
			dfsCluster.WaitActive();
		}

		/// <summary>Restart the cluster, optionally saving a new checkpoint.</summary>
		/// <param name="checkpoint">boolean true to save a new checkpoint</param>
		/// <exception cref="System.Exception">if restart fails</exception>
		protected internal static void Restart(bool checkpoint)
		{
			NameNode nameNode = dfsCluster.GetNameNode();
			if (checkpoint)
			{
				NameNodeAdapter.EnterSafeMode(nameNode, false);
				NameNodeAdapter.SaveNamespace(nameNode);
			}
			Shutdown();
			InitCluster(false);
		}
	}
}
