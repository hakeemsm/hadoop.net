using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using Javax.Xml.Parsers;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Crypto;
using Org.Apache.Hadoop.Crypto.Key;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Tools;
using Org.Apache.Hadoop.Hdfs.Tools.OfflineImageViewer;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Org.Mockito;
using Org.Xml.Sax;
using Org.Xml.Sax.Helpers;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestEncryptionZones
	{
		private Configuration conf;

		private FileSystemTestHelper fsHelper;

		private MiniDFSCluster cluster;

		protected internal HdfsAdmin dfsAdmin;

		protected internal DistributedFileSystem fs;

		private FilePath testRootDir;

		protected internal readonly string TestKey = "test_key";

		protected internal FileSystemTestWrapper fsWrapper;

		protected internal FileContextTestWrapper fcWrapper;

		protected internal virtual string GetKeyProviderURI()
		{
			return JavaKeyStoreProvider.SchemeName + "://file" + new Path(testRootDir.ToString
				(), "test.jks").ToUri();
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			conf = new HdfsConfiguration();
			fsHelper = new FileSystemTestHelper();
			// Set up java key store
			string testRoot = fsHelper.GetTestRootDir();
			testRootDir = new FilePath(testRoot).GetAbsoluteFile();
			conf.Set(DFSConfigKeys.DfsEncryptionKeyProviderUri, GetKeyProviderURI());
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeDelegationTokenAlwaysUseKey, true);
			// Lower the batch size for testing
			conf.SetInt(DFSConfigKeys.DfsNamenodeListEncryptionZonesNumResponses, 2);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			Logger.GetLogger(typeof(EncryptionZoneManager)).SetLevel(Level.Trace);
			fs = cluster.GetFileSystem();
			fsWrapper = new FileSystemTestWrapper(fs);
			fcWrapper = new FileContextTestWrapper(FileContext.GetFileContext(cluster.GetURI(
				), conf));
			dfsAdmin = new HdfsAdmin(cluster.GetURI(), conf);
			SetProvider();
			// Create a test key
			DFSTestUtil.CreateKey(TestKey, cluster, conf);
		}

		protected internal virtual void SetProvider()
		{
			// Need to set the client's KeyProvider to the NN's for JKS,
			// else the updates do not get flushed properly
			fs.GetClient().SetKeyProvider(cluster.GetNameNode().GetNamesystem().GetProvider()
				);
		}

		[TearDown]
		public virtual void Teardown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
			EncryptionFaultInjector.instance = new EncryptionFaultInjector();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void AssertNumZones(int numZones)
		{
			RemoteIterator<EncryptionZone> it = dfsAdmin.ListEncryptionZones();
			int count = 0;
			while (it.HasNext())
			{
				count++;
				it.Next();
			}
			NUnit.Framework.Assert.AreEqual("Unexpected number of encryption zones!", numZones
				, count);
		}

		/// <summary>
		/// Checks that an encryption zone with the specified keyName and path (if not
		/// null) is present.
		/// </summary>
		/// <exception cref="System.IO.IOException">if a matching zone could not be found</exception>
		public virtual void AssertZonePresent(string keyName, string path)
		{
			RemoteIterator<EncryptionZone> it = dfsAdmin.ListEncryptionZones();
			bool match = false;
			while (it.HasNext())
			{
				EncryptionZone zone = it.Next();
				bool matchKey = (keyName == null);
				bool matchPath = (path == null);
				if (keyName != null && zone.GetKeyName().Equals(keyName))
				{
					matchKey = true;
				}
				if (path != null && zone.GetPath().Equals(path))
				{
					matchPath = true;
				}
				if (matchKey && matchPath)
				{
					match = true;
					break;
				}
			}
			NUnit.Framework.Assert.IsTrue("Did not find expected encryption zone with keyName "
				 + keyName + " path " + path, match);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestBasicOperations()
		{
			int numZones = 0;
			/* Test failure of create EZ on a directory that doesn't exist. */
			Path zoneParent = new Path("/zones");
			Path zone1 = new Path(zoneParent, "zone1");
			try
			{
				dfsAdmin.CreateEncryptionZone(zone1, TestKey);
				NUnit.Framework.Assert.Fail("expected /test doesn't exist");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("cannot find", e);
			}
			/* Normal creation of an EZ */
			fsWrapper.Mkdir(zone1, FsPermission.GetDirDefault(), true);
			dfsAdmin.CreateEncryptionZone(zone1, TestKey);
			AssertNumZones(++numZones);
			AssertZonePresent(null, zone1.ToString());
			/* Test failure of create EZ on a directory which is already an EZ. */
			try
			{
				dfsAdmin.CreateEncryptionZone(zone1, TestKey);
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("already in an encryption zone", e);
			}
			/* Test failure of create EZ operation in an existing EZ. */
			Path zone1Child = new Path(zone1, "child");
			fsWrapper.Mkdir(zone1Child, FsPermission.GetDirDefault(), false);
			try
			{
				dfsAdmin.CreateEncryptionZone(zone1Child, TestKey);
				NUnit.Framework.Assert.Fail("EZ in an EZ");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("already in an encryption zone", e);
			}
			/* create EZ on parent of an EZ should fail */
			try
			{
				dfsAdmin.CreateEncryptionZone(zoneParent, TestKey);
				NUnit.Framework.Assert.Fail("EZ over an EZ");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("encryption zone for a non-empty directory"
					, e);
			}
			/* create EZ on a folder with a folder fails */
			Path notEmpty = new Path("/notEmpty");
			Path notEmptyChild = new Path(notEmpty, "child");
			fsWrapper.Mkdir(notEmptyChild, FsPermission.GetDirDefault(), true);
			try
			{
				dfsAdmin.CreateEncryptionZone(notEmpty, TestKey);
				NUnit.Framework.Assert.Fail("Created EZ on an non-empty directory with folder");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("create an encryption zone", e);
			}
			fsWrapper.Delete(notEmptyChild, false);
			/* create EZ on a folder with a file fails */
			fsWrapper.CreateFile(notEmptyChild);
			try
			{
				dfsAdmin.CreateEncryptionZone(notEmpty, TestKey);
				NUnit.Framework.Assert.Fail("Created EZ on an non-empty directory with file");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("create an encryption zone", e);
			}
			/* Test failure of create EZ on a file. */
			try
			{
				dfsAdmin.CreateEncryptionZone(notEmptyChild, TestKey);
				NUnit.Framework.Assert.Fail("Created EZ on a file");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("create an encryption zone for a file.", 
					e);
			}
			/* Test failure of creating an EZ passing a key that doesn't exist. */
			Path zone2 = new Path("/zone2");
			fsWrapper.Mkdir(zone2, FsPermission.GetDirDefault(), false);
			string myKeyName = "mykeyname";
			try
			{
				dfsAdmin.CreateEncryptionZone(zone2, myKeyName);
				NUnit.Framework.Assert.Fail("expected key doesn't exist");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("doesn't exist.", e);
			}
			/* Test failure of empty and null key name */
			try
			{
				dfsAdmin.CreateEncryptionZone(zone2, string.Empty);
				NUnit.Framework.Assert.Fail("created a zone with empty key name");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("Must specify a key name when creating", 
					e);
			}
			try
			{
				dfsAdmin.CreateEncryptionZone(zone2, null);
				NUnit.Framework.Assert.Fail("created a zone with null key name");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("Must specify a key name when creating", 
					e);
			}
			AssertNumZones(1);
			/* Test success of creating an EZ when they key exists. */
			DFSTestUtil.CreateKey(myKeyName, cluster, conf);
			dfsAdmin.CreateEncryptionZone(zone2, myKeyName);
			AssertNumZones(++numZones);
			AssertZonePresent(myKeyName, zone2.ToString());
			/* Test failure of create encryption zones as a non super user. */
			UserGroupInformation user = UserGroupInformation.CreateUserForTesting("user", new 
				string[] { "mygroup" });
			Path nonSuper = new Path("/nonSuper");
			fsWrapper.Mkdir(nonSuper, FsPermission.GetDirDefault(), false);
			user.DoAs(new _PrivilegedExceptionAction_327(this, nonSuper));
			// Test success of creating an encryption zone a few levels down.
			Path deepZone = new Path("/d/e/e/p/zone");
			fsWrapper.Mkdir(deepZone, FsPermission.GetDirDefault(), true);
			dfsAdmin.CreateEncryptionZone(deepZone, TestKey);
			AssertNumZones(++numZones);
			AssertZonePresent(null, deepZone.ToString());
			// Create and list some zones to test batching of listEZ
			for (int i = 1; i < 6; i++)
			{
				Path zonePath = new Path("/listZone" + i);
				fsWrapper.Mkdir(zonePath, FsPermission.GetDirDefault(), false);
				dfsAdmin.CreateEncryptionZone(zonePath, TestKey);
				numZones++;
				AssertNumZones(numZones);
				AssertZonePresent(null, zonePath.ToString());
			}
			fs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
			fs.SaveNamespace();
			fs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave);
			cluster.RestartNameNode(true);
			AssertNumZones(numZones);
			AssertZonePresent(null, zone1.ToString());
			// Verify newly added ez is present after restarting the NameNode
			// without persisting the namespace.
			Path nonpersistZone = new Path("/nonpersistZone");
			fsWrapper.Mkdir(nonpersistZone, FsPermission.GetDirDefault(), false);
			dfsAdmin.CreateEncryptionZone(nonpersistZone, TestKey);
			numZones++;
			cluster.RestartNameNode(true);
			AssertNumZones(numZones);
			AssertZonePresent(null, nonpersistZone.ToString());
		}

		private sealed class _PrivilegedExceptionAction_327 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_327(TestEncryptionZones _enclosing, Path nonSuper
				)
			{
				this._enclosing = _enclosing;
				this.nonSuper = nonSuper;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				HdfsAdmin userAdmin = new HdfsAdmin(FileSystem.GetDefaultUri(this._enclosing.conf
					), this._enclosing.conf);
				try
				{
					userAdmin.CreateEncryptionZone(nonSuper, this._enclosing.TestKey);
					NUnit.Framework.Assert.Fail("createEncryptionZone is superuser-only operation");
				}
				catch (AccessControlException e)
				{
					GenericTestUtils.AssertExceptionContains("Superuser privilege is required", e);
				}
				return null;
			}

			private readonly TestEncryptionZones _enclosing;

			private readonly Path nonSuper;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestBasicOperationsRootDir()
		{
			int numZones = 0;
			Path rootDir = new Path("/");
			Path zone1 = new Path(rootDir, "zone1");
			/* Normal creation of an EZ on rootDir */
			dfsAdmin.CreateEncryptionZone(rootDir, TestKey);
			AssertNumZones(++numZones);
			AssertZonePresent(null, rootDir.ToString());
			/* create EZ on child of rootDir which is already an EZ should fail */
			fsWrapper.Mkdir(zone1, FsPermission.GetDirDefault(), true);
			try
			{
				dfsAdmin.CreateEncryptionZone(zone1, TestKey);
				NUnit.Framework.Assert.Fail("EZ over an EZ");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("already in an encryption zone", e);
			}
			// Verify rootDir ez is present after restarting the NameNode
			// and saving/loading from fsimage.
			fs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
			fs.SaveNamespace();
			fs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave);
			cluster.RestartNameNode(true);
			AssertNumZones(numZones);
			AssertZonePresent(null, rootDir.ToString());
			/* create EZ on child of rootDir which is already an EZ should fail */
			try
			{
				dfsAdmin.CreateEncryptionZone(zone1, TestKey);
				NUnit.Framework.Assert.Fail("EZ over an EZ");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("already in an encryption zone", e);
			}
		}

		/// <summary>Test listing encryption zones as a non super user.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestListEncryptionZonesAsNonSuperUser()
		{
			UserGroupInformation user = UserGroupInformation.CreateUserForTesting("user", new 
				string[] { "mygroup" });
			Path testRoot = new Path("/tmp/TestEncryptionZones");
			Path superPath = new Path(testRoot, "superuseronly");
			Path allPath = new Path(testRoot, "accessall");
			fsWrapper.Mkdir(superPath, new FsPermission((short)0x1c0), true);
			dfsAdmin.CreateEncryptionZone(superPath, TestKey);
			fsWrapper.Mkdir(allPath, new FsPermission((short)0x1c7), true);
			dfsAdmin.CreateEncryptionZone(allPath, TestKey);
			user.DoAs(new _PrivilegedExceptionAction_434(this));
		}

		private sealed class _PrivilegedExceptionAction_434 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_434(TestEncryptionZones _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				HdfsAdmin userAdmin = new HdfsAdmin(FileSystem.GetDefaultUri(this._enclosing.conf
					), this._enclosing.conf);
				try
				{
					userAdmin.ListEncryptionZones();
				}
				catch (AccessControlException e)
				{
					GenericTestUtils.AssertExceptionContains("Superuser privilege is required", e);
				}
				return null;
			}

			private readonly TestEncryptionZones _enclosing;
		}

		/// <summary>Test getEncryptionZoneForPath as a non super user.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestGetEZAsNonSuperUser()
		{
			UserGroupInformation user = UserGroupInformation.CreateUserForTesting("user", new 
				string[] { "mygroup" });
			Path testRoot = new Path("/tmp/TestEncryptionZones");
			Path superPath = new Path(testRoot, "superuseronly");
			Path superPathFile = new Path(superPath, "file1");
			Path allPath = new Path(testRoot, "accessall");
			Path allPathFile = new Path(allPath, "file1");
			Path nonEZDir = new Path(testRoot, "nonEZDir");
			Path nonEZFile = new Path(nonEZDir, "file1");
			Path nonexistent = new Path("/nonexistent");
			int len = 8192;
			fsWrapper.Mkdir(testRoot, new FsPermission((short)0x1ff), true);
			fsWrapper.Mkdir(superPath, new FsPermission((short)0x1c0), false);
			fsWrapper.Mkdir(allPath, new FsPermission((short)0x1ff), false);
			fsWrapper.Mkdir(nonEZDir, new FsPermission((short)0x1ff), false);
			dfsAdmin.CreateEncryptionZone(superPath, TestKey);
			dfsAdmin.CreateEncryptionZone(allPath, TestKey);
			dfsAdmin.AllowSnapshot(new Path("/"));
			Path newSnap = fs.CreateSnapshot(new Path("/"));
			DFSTestUtil.CreateFile(fs, superPathFile, len, (short)1, unchecked((int)(0xFEED))
				);
			DFSTestUtil.CreateFile(fs, allPathFile, len, (short)1, unchecked((int)(0xFEED)));
			DFSTestUtil.CreateFile(fs, nonEZFile, len, (short)1, unchecked((int)(0xFEED)));
			user.DoAs(new _PrivilegedExceptionAction_480(this, allPath, allPathFile, superPathFile
				, nonexistent, nonEZDir, nonEZFile, newSnap));
		}

		private sealed class _PrivilegedExceptionAction_480 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_480(TestEncryptionZones _enclosing, Path allPath
				, Path allPathFile, Path superPathFile, Path nonexistent, Path nonEZDir, Path nonEZFile
				, Path newSnap)
			{
				this._enclosing = _enclosing;
				this.allPath = allPath;
				this.allPathFile = allPathFile;
				this.superPathFile = superPathFile;
				this.nonexistent = nonexistent;
				this.nonEZDir = nonEZDir;
				this.nonEZFile = nonEZFile;
				this.newSnap = newSnap;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				HdfsAdmin userAdmin = new HdfsAdmin(FileSystem.GetDefaultUri(this._enclosing.conf
					), this._enclosing.conf);
				// Check null arg
				try
				{
					userAdmin.GetEncryptionZoneForPath(null);
					NUnit.Framework.Assert.Fail("should have thrown NPE");
				}
				catch (ArgumentNullException)
				{
				}
				/*
				* IWBNI we could use assertExceptionContains, but the NPE that is
				* thrown has no message text.
				*/
				// Check operation with accessible paths
				NUnit.Framework.Assert.AreEqual("expected ez path", allPath.ToString(), userAdmin
					.GetEncryptionZoneForPath(allPath).GetPath().ToString());
				NUnit.Framework.Assert.AreEqual("expected ez path", allPath.ToString(), userAdmin
					.GetEncryptionZoneForPath(allPathFile).GetPath().ToString());
				// Check operation with inaccessible (lack of permissions) path
				try
				{
					userAdmin.GetEncryptionZoneForPath(superPathFile);
					NUnit.Framework.Assert.Fail("expected AccessControlException");
				}
				catch (AccessControlException e)
				{
					GenericTestUtils.AssertExceptionContains("Permission denied:", e);
				}
				NUnit.Framework.Assert.IsNull("expected null for nonexistent path", userAdmin.GetEncryptionZoneForPath
					(nonexistent));
				// Check operation with non-ez paths
				NUnit.Framework.Assert.IsNull("expected null for non-ez path", userAdmin.GetEncryptionZoneForPath
					(nonEZDir));
				NUnit.Framework.Assert.IsNull("expected null for non-ez path", userAdmin.GetEncryptionZoneForPath
					(nonEZFile));
				// Check operation with snapshots
				string snapshottedAllPath = newSnap.ToString() + allPath.ToString();
				NUnit.Framework.Assert.AreEqual("expected ez path", allPath.ToString(), userAdmin
					.GetEncryptionZoneForPath(new Path(snapshottedAllPath)).GetPath().ToString());
				/*
				* Delete the file from the non-snapshot and test that it is still ok
				* in the ez.
				*/
				this._enclosing.fs.Delete(allPathFile, false);
				NUnit.Framework.Assert.AreEqual("expected ez path", allPath.ToString(), userAdmin
					.GetEncryptionZoneForPath(new Path(snapshottedAllPath)).GetPath().ToString());
				// Delete the ez and make sure ss's ez is still ok.
				this._enclosing.fs.Delete(allPath, true);
				NUnit.Framework.Assert.AreEqual("expected ez path", allPath.ToString(), userAdmin
					.GetEncryptionZoneForPath(new Path(snapshottedAllPath)).GetPath().ToString());
				NUnit.Framework.Assert.IsNull("expected null for deleted file path", userAdmin.GetEncryptionZoneForPath
					(allPathFile));
				NUnit.Framework.Assert.IsNull("expected null for deleted directory path", userAdmin
					.GetEncryptionZoneForPath(allPath));
				return null;
			}

			private readonly TestEncryptionZones _enclosing;

			private readonly Path allPath;

			private readonly Path allPathFile;

			private readonly Path superPathFile;

			private readonly Path nonexistent;

			private readonly Path nonEZDir;

			private readonly Path nonEZFile;

			private readonly Path newSnap;
		}

		/// <summary>Test success of Rename EZ on a directory which is already an EZ.</summary>
		/// <exception cref="System.Exception"/>
		private void DoRenameEncryptionZone(FSTestWrapper wrapper)
		{
			Path testRoot = new Path("/tmp/TestEncryptionZones");
			Path pathFoo = new Path(testRoot, "foo");
			Path pathFooBaz = new Path(pathFoo, "baz");
			Path pathFooBazFile = new Path(pathFooBaz, "file");
			Path pathFooBar = new Path(pathFoo, "bar");
			Path pathFooBarFile = new Path(pathFooBar, "file");
			int len = 8192;
			wrapper.Mkdir(pathFoo, FsPermission.GetDirDefault(), true);
			dfsAdmin.CreateEncryptionZone(pathFoo, TestKey);
			wrapper.Mkdir(pathFooBaz, FsPermission.GetDirDefault(), true);
			DFSTestUtil.CreateFile(fs, pathFooBazFile, len, (short)1, unchecked((int)(0xFEED)
				));
			string contents = DFSTestUtil.ReadFile(fs, pathFooBazFile);
			try
			{
				wrapper.Rename(pathFooBaz, testRoot);
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains(pathFooBaz.ToString() + " can't be moved from"
					 + " an encryption zone.", e);
			}
			// Verify that we can rename dir and files within an encryption zone.
			NUnit.Framework.Assert.IsTrue(fs.Rename(pathFooBaz, pathFooBar));
			NUnit.Framework.Assert.IsTrue("Rename of dir and file within ez failed", !wrapper
				.Exists(pathFooBaz) && wrapper.Exists(pathFooBar));
			NUnit.Framework.Assert.AreEqual("Renamed file contents not the same", contents, DFSTestUtil
				.ReadFile(fs, pathFooBarFile));
			// Verify that we can rename an EZ root
			Path newFoo = new Path(testRoot, "newfoo");
			NUnit.Framework.Assert.IsTrue("Rename of EZ root", fs.Rename(pathFoo, newFoo));
			NUnit.Framework.Assert.IsTrue("Rename of EZ root failed", !wrapper.Exists(pathFoo
				) && wrapper.Exists(newFoo));
			// Verify that we can't rename an EZ root onto itself
			try
			{
				wrapper.Rename(newFoo, newFoo);
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("are the same", e);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRenameFileSystem()
		{
			DoRenameEncryptionZone(fsWrapper);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRenameFileContext()
		{
			DoRenameEncryptionZone(fcWrapper);
		}

		/// <exception cref="System.Exception"/>
		private FileEncryptionInfo GetFileEncryptionInfo(Path path)
		{
			LocatedBlocks blocks = fs.GetClient().GetLocatedBlocks(path.ToString(), 0);
			return blocks.GetFileEncryptionInfo();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestReadWrite()
		{
			HdfsAdmin dfsAdmin = new HdfsAdmin(FileSystem.GetDefaultUri(conf), conf);
			// Create a base file for comparison
			Path baseFile = new Path("/base");
			int len = 8192;
			DFSTestUtil.CreateFile(fs, baseFile, len, (short)1, unchecked((int)(0xFEED)));
			// Create the first enc file
			Path zone = new Path("/zone");
			fs.Mkdirs(zone);
			dfsAdmin.CreateEncryptionZone(zone, TestKey);
			Path encFile1 = new Path(zone, "myfile");
			DFSTestUtil.CreateFile(fs, encFile1, len, (short)1, unchecked((int)(0xFEED)));
			// Read them back in and compare byte-by-byte
			DFSTestUtil.VerifyFilesEqual(fs, baseFile, encFile1, len);
			// Roll the key of the encryption zone
			AssertNumZones(1);
			string keyName = dfsAdmin.ListEncryptionZones().Next().GetKeyName();
			cluster.GetNamesystem().GetProvider().RollNewVersion(keyName);
			// Read them back in and compare byte-by-byte
			DFSTestUtil.VerifyFilesEqual(fs, baseFile, encFile1, len);
			// Write a new enc file and validate
			Path encFile2 = new Path(zone, "myfile2");
			DFSTestUtil.CreateFile(fs, encFile2, len, (short)1, unchecked((int)(0xFEED)));
			// FEInfos should be different
			FileEncryptionInfo feInfo1 = GetFileEncryptionInfo(encFile1);
			FileEncryptionInfo feInfo2 = GetFileEncryptionInfo(encFile2);
			NUnit.Framework.Assert.IsFalse("EDEKs should be different", Arrays.Equals(feInfo1
				.GetEncryptedDataEncryptionKey(), feInfo2.GetEncryptedDataEncryptionKey()));
			Assert.AssertNotEquals("Key was rolled, versions should be different", feInfo1.GetEzKeyVersionName
				(), feInfo2.GetEzKeyVersionName());
			// Contents still equal
			DFSTestUtil.VerifyFilesEqual(fs, encFile1, encFile2, len);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestReadWriteUsingWebHdfs()
		{
			HdfsAdmin dfsAdmin = new HdfsAdmin(FileSystem.GetDefaultUri(conf), conf);
			FileSystem webHdfsFs = WebHdfsTestUtil.GetWebHdfsFileSystem(conf, WebHdfsFileSystem
				.Scheme);
			Path zone = new Path("/zone");
			fs.Mkdirs(zone);
			dfsAdmin.CreateEncryptionZone(zone, TestKey);
			/* Create an unencrypted file for comparison purposes. */
			Path unencFile = new Path("/unenc");
			int len = 8192;
			DFSTestUtil.CreateFile(webHdfsFs, unencFile, len, (short)1, unchecked((int)(0xFEED
				)));
			/*
			* Create the same file via webhdfs, but this time encrypted. Compare it
			* using both webhdfs and DFS.
			*/
			Path encFile1 = new Path(zone, "myfile");
			DFSTestUtil.CreateFile(webHdfsFs, encFile1, len, (short)1, unchecked((int)(0xFEED
				)));
			DFSTestUtil.VerifyFilesEqual(webHdfsFs, unencFile, encFile1, len);
			DFSTestUtil.VerifyFilesEqual(fs, unencFile, encFile1, len);
			/*
			* Same thing except this time create the encrypted file using DFS.
			*/
			Path encFile2 = new Path(zone, "myfile2");
			DFSTestUtil.CreateFile(fs, encFile2, len, (short)1, unchecked((int)(0xFEED)));
			DFSTestUtil.VerifyFilesEqual(webHdfsFs, unencFile, encFile2, len);
			DFSTestUtil.VerifyFilesEqual(fs, unencFile, encFile2, len);
			/* Verify appending to files works correctly. */
			AppendOneByte(fs, unencFile);
			AppendOneByte(webHdfsFs, encFile1);
			AppendOneByte(fs, encFile2);
			DFSTestUtil.VerifyFilesEqual(webHdfsFs, unencFile, encFile1, len);
			DFSTestUtil.VerifyFilesEqual(fs, unencFile, encFile1, len);
			DFSTestUtil.VerifyFilesEqual(webHdfsFs, unencFile, encFile2, len);
			DFSTestUtil.VerifyFilesEqual(fs, unencFile, encFile2, len);
		}

		/// <exception cref="System.IO.IOException"/>
		private void AppendOneByte(FileSystem fs, Path p)
		{
			FSDataOutputStream @out = fs.Append(p);
			@out.Write(unchecked((byte)unchecked((int)(0x123))));
			@out.Close();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestVersionAndSuiteNegotiation()
		{
			HdfsAdmin dfsAdmin = new HdfsAdmin(FileSystem.GetDefaultUri(conf), conf);
			Path zone = new Path("/zone");
			fs.Mkdirs(zone);
			dfsAdmin.CreateEncryptionZone(zone, TestKey);
			// Create a file in an EZ, which should succeed
			DFSTestUtil.CreateFile(fs, new Path(zone, "success1"), 0, (short)1, unchecked((int
				)(0xFEED)));
			// Pass no supported versions, fail
			DFSOutputStream.SupportedCryptoVersions = new CryptoProtocolVersion[] {  };
			try
			{
				DFSTestUtil.CreateFile(fs, new Path(zone, "fail"), 0, (short)1, unchecked((int)(0xFEED
					)));
				NUnit.Framework.Assert.Fail("Created a file without specifying a crypto protocol version"
					);
			}
			catch (UnknownCryptoProtocolVersionException e)
			{
				GenericTestUtils.AssertExceptionContains("No crypto protocol versions", e);
			}
			// Pass some unknown versions, fail
			DFSOutputStream.SupportedCryptoVersions = new CryptoProtocolVersion[] { CryptoProtocolVersion
				.Unknown, CryptoProtocolVersion.Unknown };
			try
			{
				DFSTestUtil.CreateFile(fs, new Path(zone, "fail"), 0, (short)1, unchecked((int)(0xFEED
					)));
				NUnit.Framework.Assert.Fail("Created a file without specifying a known crypto protocol version"
					);
			}
			catch (UnknownCryptoProtocolVersionException e)
			{
				GenericTestUtils.AssertExceptionContains("No crypto protocol versions", e);
			}
			// Pass some unknown and a good cipherSuites, success
			DFSOutputStream.SupportedCryptoVersions = new CryptoProtocolVersion[] { CryptoProtocolVersion
				.Unknown, CryptoProtocolVersion.Unknown, CryptoProtocolVersion.EncryptionZones };
			DFSTestUtil.CreateFile(fs, new Path(zone, "success2"), 0, (short)1, unchecked((int
				)(0xFEED)));
			DFSOutputStream.SupportedCryptoVersions = new CryptoProtocolVersion[] { CryptoProtocolVersion
				.EncryptionZones, CryptoProtocolVersion.Unknown, CryptoProtocolVersion.Unknown };
			DFSTestUtil.CreateFile(fs, new Path(zone, "success3"), 4096, (short)1, unchecked(
				(int)(0xFEED)));
			// Check KeyProvider state
			// Flushing the KP on the NN, since it caches, and init a test one
			cluster.GetNamesystem().GetProvider().Flush();
			KeyProvider provider = KeyProviderFactory.Get(new URI(conf.GetTrimmed(DFSConfigKeys
				.DfsEncryptionKeyProviderUri)), conf);
			IList<string> keys = provider.GetKeys();
			NUnit.Framework.Assert.AreEqual("Expected NN to have created one key per zone", 1
				, keys.Count);
			IList<KeyProvider.KeyVersion> allVersions = Lists.NewArrayList();
			foreach (string key in keys)
			{
				IList<KeyProvider.KeyVersion> versions = provider.GetKeyVersions(key);
				NUnit.Framework.Assert.AreEqual("Should only have one key version per key", 1, versions
					.Count);
				Sharpen.Collections.AddAll(allVersions, versions);
			}
			// Check that the specified CipherSuite was correctly saved on the NN
			for (int i = 2; i <= 3; i++)
			{
				FileEncryptionInfo feInfo = GetFileEncryptionInfo(new Path(zone.ToString() + "/success"
					 + i));
				NUnit.Framework.Assert.AreEqual(feInfo.GetCipherSuite(), CipherSuite.AesCtrNopadding
					);
			}
			DFSClient old = fs.dfs;
			try
			{
				TestCipherSuiteNegotiation(fs, conf);
			}
			finally
			{
				fs.dfs = old;
			}
		}

		/// <exception cref="System.Exception"/>
		private static void MockCreate(ClientProtocol mcp, CipherSuite suite, CryptoProtocolVersion
			 version)
		{
			Org.Mockito.Mockito.DoReturn(new HdfsFileStatus(0, false, 1, 1024, 0, 0, new FsPermission
				((short)777), "owner", "group", new byte[0], new byte[0], 1010, 0, new FileEncryptionInfo
				(suite, version, new byte[suite.GetAlgorithmBlockSize()], new byte[suite.GetAlgorithmBlockSize
				()], "fakeKey", "fakeVersion"), unchecked((byte)0))).When(mcp).Create(Matchers.AnyString
				(), (FsPermission)Matchers.AnyObject(), Matchers.AnyString(), (EnumSetWritable<CreateFlag
				>)Matchers.AnyObject(), Matchers.AnyBoolean(), Matchers.AnyShort(), Matchers.AnyLong
				(), (CryptoProtocolVersion[])Matchers.AnyObject());
		}

		// This test only uses mocks. Called from the end of an existing test to
		// avoid an extra mini cluster.
		/// <exception cref="System.Exception"/>
		private static void TestCipherSuiteNegotiation(DistributedFileSystem fs, Configuration
			 conf)
		{
			// Set up mock ClientProtocol to test client-side CipherSuite negotiation
			ClientProtocol mcp = Org.Mockito.Mockito.Mock<ClientProtocol>();
			// Try with an empty conf
			Configuration noCodecConf = new Configuration(conf);
			CipherSuite suite = CipherSuite.AesCtrNopadding;
			string confKey = CommonConfigurationKeysPublic.HadoopSecurityCryptoCodecClassesKeyPrefix
				 + suite.GetConfigSuffix();
			noCodecConf.Set(confKey, string.Empty);
			fs.dfs = new DFSClient(null, mcp, noCodecConf, null);
			MockCreate(mcp, suite, CryptoProtocolVersion.EncryptionZones);
			try
			{
				fs.Create(new Path("/mock"));
				NUnit.Framework.Assert.Fail("Created with no configured codecs!");
			}
			catch (UnknownCipherSuiteException e)
			{
				GenericTestUtils.AssertExceptionContains("No configuration found for the cipher", 
					e);
			}
			// Try create with an UNKNOWN CipherSuite
			fs.dfs = new DFSClient(null, mcp, conf, null);
			CipherSuite unknown = CipherSuite.Unknown;
			unknown.SetUnknownValue(989);
			MockCreate(mcp, unknown, CryptoProtocolVersion.EncryptionZones);
			try
			{
				fs.Create(new Path("/mock"));
				NUnit.Framework.Assert.Fail("Created with unknown cipher!");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("unknown CipherSuite with ID 989", e);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCreateEZWithNoProvider()
		{
			// Unset the key provider and make sure EZ ops don't work
			Configuration clusterConf = cluster.GetConfiguration(0);
			clusterConf.Unset(DFSConfigKeys.DfsEncryptionKeyProviderUri);
			cluster.RestartNameNode(true);
			cluster.WaitActive();
			Path zone1 = new Path("/zone1");
			fsWrapper.Mkdir(zone1, FsPermission.GetDirDefault(), true);
			try
			{
				dfsAdmin.CreateEncryptionZone(zone1, TestKey);
				NUnit.Framework.Assert.Fail("expected exception");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("since no key provider is available", e);
			}
			Path jksPath = new Path(testRootDir.ToString(), "test.jks");
			clusterConf.Set(DFSConfigKeys.DfsEncryptionKeyProviderUri, JavaKeyStoreProvider.SchemeName
				 + "://file" + jksPath.ToUri());
			// Try listing EZs as well
			AssertNumZones(0);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestIsEncryptedMethod()
		{
			DoTestIsEncryptedMethod(new Path("/"));
			DoTestIsEncryptedMethod(new Path("/.reserved/raw"));
		}

		/// <exception cref="System.Exception"/>
		private void DoTestIsEncryptedMethod(Path prefix)
		{
			try
			{
				Dtiem(prefix);
			}
			finally
			{
				foreach (FileStatus s in fsWrapper.ListStatus(prefix))
				{
					fsWrapper.Delete(s.GetPath(), true);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private void Dtiem(Path prefix)
		{
			HdfsAdmin dfsAdmin = new HdfsAdmin(FileSystem.GetDefaultUri(conf), conf);
			// Create an unencrypted file to check isEncrypted returns false
			Path baseFile = new Path(prefix, "base");
			fsWrapper.CreateFile(baseFile);
			FileStatus stat = fsWrapper.GetFileStatus(baseFile);
			NUnit.Framework.Assert.IsFalse("Expected isEncrypted to return false for " + baseFile
				, stat.IsEncrypted());
			// Create an encrypted file to check isEncrypted returns true
			Path zone = new Path(prefix, "zone");
			fsWrapper.Mkdir(zone, FsPermission.GetDirDefault(), true);
			dfsAdmin.CreateEncryptionZone(zone, TestKey);
			Path encFile = new Path(zone, "encfile");
			fsWrapper.CreateFile(encFile);
			stat = fsWrapper.GetFileStatus(encFile);
			NUnit.Framework.Assert.IsTrue("Expected isEncrypted to return true for enc file" 
				+ encFile, stat.IsEncrypted());
			// check that it returns true for an ez root
			stat = fsWrapper.GetFileStatus(zone);
			NUnit.Framework.Assert.IsTrue("Expected isEncrypted to return true for ezroot", stat
				.IsEncrypted());
			// check that it returns true for a dir in the ez
			Path zoneSubdir = new Path(zone, "subdir");
			fsWrapper.Mkdir(zoneSubdir, FsPermission.GetDirDefault(), true);
			stat = fsWrapper.GetFileStatus(zoneSubdir);
			NUnit.Framework.Assert.IsTrue("Expected isEncrypted to return true for ez subdir "
				 + zoneSubdir, stat.IsEncrypted());
			// check that it returns false for a non ez dir
			Path nonEzDirPath = new Path(prefix, "nonzone");
			fsWrapper.Mkdir(nonEzDirPath, FsPermission.GetDirDefault(), true);
			stat = fsWrapper.GetFileStatus(nonEzDirPath);
			NUnit.Framework.Assert.IsFalse("Expected isEncrypted to return false for directory "
				 + nonEzDirPath, stat.IsEncrypted());
			// check that it returns true for listings within an ez
			FileStatus[] statuses = fsWrapper.ListStatus(zone);
			foreach (FileStatus s in statuses)
			{
				NUnit.Framework.Assert.IsTrue("Expected isEncrypted to return true for ez stat " 
					+ zone, s.IsEncrypted());
			}
			statuses = fsWrapper.ListStatus(encFile);
			foreach (FileStatus s_1 in statuses)
			{
				NUnit.Framework.Assert.IsTrue("Expected isEncrypted to return true for ez file stat "
					 + encFile, s_1.IsEncrypted());
			}
			// check that it returns false for listings outside an ez
			statuses = fsWrapper.ListStatus(nonEzDirPath);
			foreach (FileStatus s_2 in statuses)
			{
				NUnit.Framework.Assert.IsFalse("Expected isEncrypted to return false for nonez stat "
					 + nonEzDirPath, s_2.IsEncrypted());
			}
			statuses = fsWrapper.ListStatus(baseFile);
			foreach (FileStatus s_3 in statuses)
			{
				NUnit.Framework.Assert.IsFalse("Expected isEncrypted to return false for non ez stat "
					 + baseFile, s_3.IsEncrypted());
			}
		}

		private class MyInjector : EncryptionFaultInjector
		{
			internal int generateCount;

			internal CountDownLatch ready;

			internal CountDownLatch wait;

			public MyInjector(TestEncryptionZones _enclosing)
			{
				this._enclosing = _enclosing;
				this.ready = new CountDownLatch(1);
				this.wait = new CountDownLatch(1);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void StartFileAfterGenerateKey()
			{
				this.ready.CountDown();
				try
				{
					this.wait.Await();
				}
				catch (Exception e)
				{
					throw new IOException(e);
				}
				this.generateCount++;
			}

			private readonly TestEncryptionZones _enclosing;
		}

		private class CreateFileTask : Callable<Void>
		{
			private FileSystemTestWrapper fsWrapper;

			private Path name;

			internal CreateFileTask(TestEncryptionZones _enclosing, FileSystemTestWrapper fsWrapper
				, Path name)
			{
				this._enclosing = _enclosing;
				this.fsWrapper = fsWrapper;
				this.name = name;
			}

			/// <exception cref="System.Exception"/>
			public virtual Void Call()
			{
				this.fsWrapper.CreateFile(this.name);
				return null;
			}

			private readonly TestEncryptionZones _enclosing;
		}

		private class InjectFaultTask : Callable<Void>
		{
			internal readonly Path zone1 = new Path("/zone1");

			internal readonly Path file = new Path(this.zone1, "file1");

			internal readonly ExecutorService executor = Executors.NewSingleThreadExecutor();

			internal TestEncryptionZones.MyInjector injector;

			/// <exception cref="System.Exception"/>
			public virtual Void Call()
			{
				// Set up the injector
				this.injector = new TestEncryptionZones.MyInjector(this);
				EncryptionFaultInjector.instance = this.injector;
				Future<Void> future = this.executor.Submit(new TestEncryptionZones.CreateFileTask
					(this, this._enclosing.fsWrapper, this.file));
				this.injector.ready.Await();
				// Do the fault
				this.DoFault();
				// Allow create to proceed
				this.injector.wait.CountDown();
				future.Get();
				// Cleanup and postconditions
				this.DoCleanup();
				return null;
			}

			/// <exception cref="System.Exception"/>
			public virtual void DoFault()
			{
			}

			/// <exception cref="System.Exception"/>
			public virtual void DoCleanup()
			{
			}

			internal InjectFaultTask(TestEncryptionZones _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestEncryptionZones _enclosing;
		}

		/// <summary>Tests the retry logic in startFile.</summary>
		/// <remarks>
		/// Tests the retry logic in startFile. We release the lock while generating
		/// an EDEK, so tricky things can happen in the intervening time.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestStartFileRetry()
		{
			Path zone1 = new Path("/zone1");
			Path file = new Path(zone1, "file1");
			fsWrapper.Mkdir(zone1, FsPermission.GetDirDefault(), true);
			ExecutorService executor = Executors.NewSingleThreadExecutor();
			// Test when the parent directory becomes an EZ
			executor.Submit(new _InjectFaultTask_1014(this)).Get();
			// Test when the parent directory unbecomes an EZ
			executor.Submit(new _InjectFaultTask_1027(this)).Get();
			// Test when the parent directory becomes a different EZ
			fsWrapper.Mkdir(zone1, FsPermission.GetDirDefault(), true);
			string otherKey = "other_key";
			DFSTestUtil.CreateKey(otherKey, cluster, conf);
			dfsAdmin.CreateEncryptionZone(zone1, TestKey);
			executor.Submit(new _InjectFaultTask_1045(this, otherKey)).Get();
			// Test that the retry limit leads to an error
			fsWrapper.Mkdir(zone1, FsPermission.GetDirDefault(), true);
			string anotherKey = "another_key";
			DFSTestUtil.CreateKey(anotherKey, cluster, conf);
			dfsAdmin.CreateEncryptionZone(zone1, anotherKey);
			string keyToUse = otherKey;
			TestEncryptionZones.MyInjector injector = new TestEncryptionZones.MyInjector(this
				);
			EncryptionFaultInjector.instance = injector;
			Future<object> future = executor.Submit(new TestEncryptionZones.CreateFileTask(this
				, fsWrapper, file));
			// Flip-flop between two EZs to repeatedly fail
			for (int i = 0; i < DFSOutputStream.CreateRetryCount + 1; i++)
			{
				injector.ready.Await();
				fsWrapper.Delete(zone1, true);
				fsWrapper.Mkdir(zone1, FsPermission.GetDirDefault(), true);
				dfsAdmin.CreateEncryptionZone(zone1, keyToUse);
				if (keyToUse == otherKey)
				{
					keyToUse = anotherKey;
				}
				else
				{
					keyToUse = otherKey;
				}
				injector.wait.CountDown();
				injector = new TestEncryptionZones.MyInjector(this);
				EncryptionFaultInjector.instance = injector;
			}
			try
			{
				future.Get();
				NUnit.Framework.Assert.Fail("Expected exception from too many retries");
			}
			catch (ExecutionException e)
			{
				GenericTestUtils.AssertExceptionContains("Too many retries because of encryption zone operations"
					, e.InnerException);
			}
		}

		private sealed class _InjectFaultTask_1014 : TestEncryptionZones.InjectFaultTask
		{
			public _InjectFaultTask_1014(TestEncryptionZones _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public override void DoFault()
			{
				this._enclosing.dfsAdmin.CreateEncryptionZone(this.zone1, this._enclosing.TestKey
					);
			}

			/// <exception cref="System.Exception"/>
			public override void DoCleanup()
			{
				NUnit.Framework.Assert.AreEqual("Expected a startFile retry", 2, this.injector.generateCount
					);
				this._enclosing.fsWrapper.Delete(this.file, false);
			}

			private readonly TestEncryptionZones _enclosing;
		}

		private sealed class _InjectFaultTask_1027 : TestEncryptionZones.InjectFaultTask
		{
			public _InjectFaultTask_1027(TestEncryptionZones _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public override void DoFault()
			{
				this._enclosing.fsWrapper.Delete(this.zone1, true);
			}

			/// <exception cref="System.Exception"/>
			public override void DoCleanup()
			{
				NUnit.Framework.Assert.AreEqual("Expected no startFile retries", 1, this.injector
					.generateCount);
				this._enclosing.fsWrapper.Delete(this.file, false);
			}

			private readonly TestEncryptionZones _enclosing;
		}

		private sealed class _InjectFaultTask_1045 : TestEncryptionZones.InjectFaultTask
		{
			public _InjectFaultTask_1045(TestEncryptionZones _enclosing, string otherKey)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.otherKey = otherKey;
			}

			/// <exception cref="System.Exception"/>
			public override void DoFault()
			{
				this._enclosing.fsWrapper.Delete(this.zone1, true);
				this._enclosing.fsWrapper.Mkdir(this.zone1, FsPermission.GetDirDefault(), true);
				this._enclosing.dfsAdmin.CreateEncryptionZone(this.zone1, otherKey);
			}

			/// <exception cref="System.Exception"/>
			public override void DoCleanup()
			{
				NUnit.Framework.Assert.AreEqual("Expected a startFile retry", 2, this.injector.generateCount
					);
				this._enclosing.fsWrapper.Delete(this.zone1, true);
			}

			private readonly TestEncryptionZones _enclosing;

			private readonly string otherKey;
		}

		/// <summary>Tests obtaining delegation token from stored key</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestDelegationToken()
		{
			UserGroupInformation.CreateRemoteUser("JobTracker");
			DistributedFileSystem dfs = cluster.GetFileSystem();
			KeyProvider keyProvider = Org.Mockito.Mockito.Mock<KeyProvider>(Org.Mockito.Mockito.WithSettings
				().ExtraInterfaces(typeof(KeyProviderDelegationTokenExtension.DelegationTokenExtension
				), typeof(KeyProviderCryptoExtension.CryptoExtension)));
			Org.Mockito.Mockito.When(keyProvider.GetConf()).ThenReturn(conf);
			byte[] testIdentifier = Sharpen.Runtime.GetBytesForString("Test identifier for delegation token"
				);
			Org.Apache.Hadoop.Security.Token.Token<object> testToken = new Org.Apache.Hadoop.Security.Token.Token
				(testIdentifier, new byte[0], new Text(), new Text());
			Org.Mockito.Mockito.When(((KeyProviderDelegationTokenExtension.DelegationTokenExtension
				)keyProvider).AddDelegationTokens(Matchers.AnyString(), (Credentials)Matchers.Any
				())).ThenReturn(new Org.Apache.Hadoop.Security.Token.Token<object>[] { testToken
				 });
			dfs.GetClient().SetKeyProvider(keyProvider);
			Credentials creds = new Credentials();
			Org.Apache.Hadoop.Security.Token.Token<object>[] tokens = dfs.AddDelegationTokens
				("JobTracker", creds);
			DistributedFileSystem.Log.Debug("Delegation tokens: " + Arrays.AsList(tokens));
			NUnit.Framework.Assert.AreEqual(2, tokens.Length);
			NUnit.Framework.Assert.AreEqual(tokens[1], testToken);
			NUnit.Framework.Assert.AreEqual(1, creds.NumberOfTokens());
		}

		/// <summary>Test running fsck on a system with encryption zones.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestFsckOnEncryptionZones()
		{
			int len = 8196;
			Path zoneParent = new Path("/zones");
			Path zone1 = new Path(zoneParent, "zone1");
			Path zone1File = new Path(zone1, "file");
			fsWrapper.Mkdir(zone1, FsPermission.GetDirDefault(), true);
			dfsAdmin.CreateEncryptionZone(zone1, TestKey);
			DFSTestUtil.CreateFile(fs, zone1File, len, (short)1, unchecked((int)(0xFEED)));
			ByteArrayOutputStream bStream = new ByteArrayOutputStream();
			TextWriter @out = new TextWriter(bStream, true);
			int errCode = ToolRunner.Run(new DFSck(conf, @out), new string[] { "/" });
			NUnit.Framework.Assert.AreEqual("Fsck ran with non-zero error code", 0, errCode);
			string result = bStream.ToString();
			NUnit.Framework.Assert.IsTrue("Fsck did not return HEALTHY status", result.Contains
				(NamenodeFsck.HealthyStatus));
			// Run fsck directly on the encryption zone instead of root
			errCode = ToolRunner.Run(new DFSck(conf, @out), new string[] { zoneParent.ToString
				() });
			NUnit.Framework.Assert.AreEqual("Fsck ran with non-zero error code", 0, errCode);
			result = bStream.ToString();
			NUnit.Framework.Assert.IsTrue("Fsck did not return HEALTHY status", result.Contains
				(NamenodeFsck.HealthyStatus));
		}

		/// <summary>
		/// Test correctness of successive snapshot creation and deletion
		/// on a system with encryption zones.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestSnapshotsOnEncryptionZones()
		{
			string TestKey2 = "testkey2";
			DFSTestUtil.CreateKey(TestKey2, cluster, conf);
			int len = 8196;
			Path zoneParent = new Path("/zones");
			Path zone = new Path(zoneParent, "zone");
			Path zoneFile = new Path(zone, "zoneFile");
			fsWrapper.Mkdir(zone, FsPermission.GetDirDefault(), true);
			dfsAdmin.AllowSnapshot(zoneParent);
			dfsAdmin.CreateEncryptionZone(zone, TestKey);
			DFSTestUtil.CreateFile(fs, zoneFile, len, (short)1, unchecked((int)(0xFEED)));
			string contents = DFSTestUtil.ReadFile(fs, zoneFile);
			Path snap1 = fs.CreateSnapshot(zoneParent, "snap1");
			Path snap1Zone = new Path(snap1, zone.GetName());
			NUnit.Framework.Assert.AreEqual("Got unexpected ez path", zone.ToString(), dfsAdmin
				.GetEncryptionZoneForPath(snap1Zone).GetPath().ToString());
			// Now delete the encryption zone, recreate the dir, and take another
			// snapshot
			fsWrapper.Delete(zone, true);
			fsWrapper.Mkdir(zone, FsPermission.GetDirDefault(), true);
			Path snap2 = fs.CreateSnapshot(zoneParent, "snap2");
			Path snap2Zone = new Path(snap2, zone.GetName());
			NUnit.Framework.Assert.IsNull("Expected null ez path", dfsAdmin.GetEncryptionZoneForPath
				(snap2Zone));
			// Create the encryption zone again
			dfsAdmin.CreateEncryptionZone(zone, TestKey2);
			Path snap3 = fs.CreateSnapshot(zoneParent, "snap3");
			Path snap3Zone = new Path(snap3, zone.GetName());
			// Check that snap3's EZ has the correct settings
			EncryptionZone ezSnap3 = dfsAdmin.GetEncryptionZoneForPath(snap3Zone);
			NUnit.Framework.Assert.AreEqual("Got unexpected ez path", zone.ToString(), ezSnap3
				.GetPath().ToString());
			NUnit.Framework.Assert.AreEqual("Unexpected ez key", TestKey2, ezSnap3.GetKeyName
				());
			// Check that older snapshots still have the old EZ settings
			EncryptionZone ezSnap1 = dfsAdmin.GetEncryptionZoneForPath(snap1Zone);
			NUnit.Framework.Assert.AreEqual("Got unexpected ez path", zone.ToString(), ezSnap1
				.GetPath().ToString());
			NUnit.Framework.Assert.AreEqual("Unexpected ez key", TestKey, ezSnap1.GetKeyName(
				));
			// Check that listEZs only shows the current filesystem state
			AList<EncryptionZone> listZones = Lists.NewArrayList();
			RemoteIterator<EncryptionZone> it = dfsAdmin.ListEncryptionZones();
			while (it.HasNext())
			{
				listZones.AddItem(it.Next());
			}
			foreach (EncryptionZone z in listZones)
			{
				System.Console.Out.WriteLine(z);
			}
			NUnit.Framework.Assert.AreEqual("Did not expect additional encryption zones!", 1, 
				listZones.Count);
			EncryptionZone listZone = listZones[0];
			NUnit.Framework.Assert.AreEqual("Got unexpected ez path", zone.ToString(), listZone
				.GetPath().ToString());
			NUnit.Framework.Assert.AreEqual("Unexpected ez key", TestKey2, listZone.GetKeyName
				());
			// Verify contents of the snapshotted file
			Path snapshottedZoneFile = new Path(snap1.ToString() + "/" + zone.GetName() + "/"
				 + zoneFile.GetName());
			NUnit.Framework.Assert.AreEqual("Contents of snapshotted file have changed unexpectedly"
				, contents, DFSTestUtil.ReadFile(fs, snapshottedZoneFile));
			// Now delete the snapshots out of order and verify the zones are still
			// correct
			fs.DeleteSnapshot(zoneParent, snap2.GetName());
			NUnit.Framework.Assert.AreEqual("Got unexpected ez path", zone.ToString(), dfsAdmin
				.GetEncryptionZoneForPath(snap1Zone).GetPath().ToString());
			NUnit.Framework.Assert.AreEqual("Got unexpected ez path", zone.ToString(), dfsAdmin
				.GetEncryptionZoneForPath(snap3Zone).GetPath().ToString());
			fs.DeleteSnapshot(zoneParent, snap1.GetName());
			NUnit.Framework.Assert.AreEqual("Got unexpected ez path", zone.ToString(), dfsAdmin
				.GetEncryptionZoneForPath(snap3Zone).GetPath().ToString());
		}

		/// <summary>
		/// Verify symlinks can be created in encryption zones and that
		/// they function properly when the target is in the same
		/// or different ez.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestEncryptionZonesWithSymlinks()
		{
			// Verify we can create an encryption zone over both link and target
			int len = 8192;
			Path parent = new Path("/parent");
			Path linkParent = new Path(parent, "symdir1");
			Path targetParent = new Path(parent, "symdir2");
			Path link = new Path(linkParent, "link");
			Path target = new Path(targetParent, "target");
			fs.Mkdirs(parent);
			dfsAdmin.CreateEncryptionZone(parent, TestKey);
			fs.Mkdirs(linkParent);
			fs.Mkdirs(targetParent);
			DFSTestUtil.CreateFile(fs, target, len, (short)1, unchecked((int)(0xFEED)));
			string content = DFSTestUtil.ReadFile(fs, target);
			fs.CreateSymlink(target, link, false);
			NUnit.Framework.Assert.AreEqual("Contents read from link are not the same as target"
				, content, DFSTestUtil.ReadFile(fs, link));
			fs.Delete(parent, true);
			// Now let's test when the symlink and target are in different
			// encryption zones
			fs.Mkdirs(linkParent);
			fs.Mkdirs(targetParent);
			dfsAdmin.CreateEncryptionZone(linkParent, TestKey);
			dfsAdmin.CreateEncryptionZone(targetParent, TestKey);
			DFSTestUtil.CreateFile(fs, target, len, (short)1, unchecked((int)(0xFEED)));
			content = DFSTestUtil.ReadFile(fs, target);
			fs.CreateSymlink(target, link, false);
			NUnit.Framework.Assert.AreEqual("Contents read from link are not the same as target"
				, content, DFSTestUtil.ReadFile(fs, link));
			fs.Delete(link, true);
			fs.Delete(target, true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestConcatFailsInEncryptionZones()
		{
			int len = 8192;
			Path ez = new Path("/ez");
			fs.Mkdirs(ez);
			dfsAdmin.CreateEncryptionZone(ez, TestKey);
			Path src1 = new Path(ez, "src1");
			Path src2 = new Path(ez, "src2");
			Path target = new Path(ez, "target");
			DFSTestUtil.CreateFile(fs, src1, len, (short)1, unchecked((int)(0xFEED)));
			DFSTestUtil.CreateFile(fs, src2, len, (short)1, unchecked((int)(0xFEED)));
			DFSTestUtil.CreateFile(fs, target, len, (short)1, unchecked((int)(0xFEED)));
			try
			{
				fs.Concat(target, new Path[] { src1, src2 });
				NUnit.Framework.Assert.Fail("expected concat to throw en exception for files in an ez"
					);
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("concat can not be called for files in an encryption zone"
					, e);
			}
			fs.Delete(ez, true);
		}

		/// <summary>Test running the OfflineImageViewer on a system with encryption zones.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestOfflineImageViewerOnEncryptionZones()
		{
			int len = 8196;
			Path zoneParent = new Path("/zones");
			Path zone1 = new Path(zoneParent, "zone1");
			Path zone1File = new Path(zone1, "file");
			fsWrapper.Mkdir(zone1, FsPermission.GetDirDefault(), true);
			dfsAdmin.CreateEncryptionZone(zone1, TestKey);
			DFSTestUtil.CreateFile(fs, zone1File, len, (short)1, unchecked((int)(0xFEED)));
			fs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter, false);
			fs.SaveNamespace();
			FilePath originalFsimage = FSImageTestUtil.FindLatestImageFile(FSImageTestUtil.GetFSImage
				(cluster.GetNameNode()).GetStorage().GetStorageDir(0));
			if (originalFsimage == null)
			{
				throw new RuntimeException("Didn't generate or can't find fsimage");
			}
			// Run the XML OIV processor
			ByteArrayOutputStream output = new ByteArrayOutputStream();
			TextWriter pw = new TextWriter(output);
			PBImageXmlWriter v = new PBImageXmlWriter(new Configuration(), pw);
			v.Visit(new RandomAccessFile(originalFsimage, "r"));
			string xml = output.ToString();
			SAXParser parser = SAXParserFactory.NewInstance().NewSAXParser();
			parser.Parse(new InputSource(new StringReader(xml)), new DefaultHandler());
		}

		/// <summary>Test creating encryption zone on the root path</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestEncryptionZonesOnRootPath()
		{
			int len = 8196;
			Path rootDir = new Path("/");
			Path zoneFile = new Path(rootDir, "file");
			Path rawFile = new Path("/.reserved/raw/file");
			dfsAdmin.CreateEncryptionZone(rootDir, TestKey);
			DFSTestUtil.CreateFile(fs, zoneFile, len, (short)1, unchecked((int)(0xFEED)));
			NUnit.Framework.Assert.AreEqual("File can be created on the root encryption zone "
				 + "with correct length", len, fs.GetFileStatus(zoneFile).GetLen());
			NUnit.Framework.Assert.AreEqual("Root dir is encrypted", true, fs.GetFileStatus(rootDir
				).IsEncrypted());
			NUnit.Framework.Assert.AreEqual("File is encrypted", true, fs.GetFileStatus(zoneFile
				).IsEncrypted());
			DFSTestUtil.VerifyFilesNotEqual(fs, zoneFile, rawFile, len);
		}
	}
}
