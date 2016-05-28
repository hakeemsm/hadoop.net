using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Permission
{
	public class TestStickyBit
	{
		internal static readonly UserGroupInformation user1 = UserGroupInformation.CreateUserForTesting
			("theDoctor", new string[] { "tardis" });

		internal static readonly UserGroupInformation user2 = UserGroupInformation.CreateUserForTesting
			("rose", new string[] { "powellestates" });

		private static MiniDFSCluster cluster;

		private static Configuration conf;

		private static FileSystem hdfs;

		private static FileSystem hdfsAsUser1;

		private static FileSystem hdfsAsUser2;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void Init()
		{
			conf = new HdfsConfiguration();
			conf.SetBoolean(DFSConfigKeys.DfsPermissionsEnabledKey, true);
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeAclsEnabledKey, true);
			InitCluster(true);
		}

		/// <exception cref="System.Exception"/>
		private static void InitCluster(bool format)
		{
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(4).Format(format).Build();
			hdfs = cluster.GetFileSystem();
			NUnit.Framework.Assert.IsTrue(hdfs is DistributedFileSystem);
			hdfsAsUser1 = DFSTestUtil.GetFileSystemAs(user1, conf);
			NUnit.Framework.Assert.IsTrue(hdfsAsUser1 is DistributedFileSystem);
			hdfsAsUser2 = DFSTestUtil.GetFileSystemAs(user2, conf);
			NUnit.Framework.Assert.IsTrue(hdfsAsUser2 is DistributedFileSystem);
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			if (hdfs != null)
			{
				foreach (FileStatus stat in hdfs.ListStatus(new Path("/")))
				{
					hdfs.Delete(stat.GetPath(), true);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void Shutdown()
		{
			IOUtils.Cleanup(null, hdfs, hdfsAsUser1, hdfsAsUser2);
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Ensure that even if a file is in a directory with the sticky bit on,
		/// another user can write to that file (assuming correct permissions).
		/// </summary>
		/// <exception cref="System.Exception"/>
		private void ConfirmCanAppend(Configuration conf, Path p)
		{
			// Write a file to the new tmp directory as a regular user
			Path file = new Path(p, "foo");
			WriteFile(hdfsAsUser1, file);
			hdfsAsUser1.SetPermission(file, new FsPermission((short)0x1ff));
			// Log onto cluster as another user and attempt to append to file
			Path file2 = new Path(p, "foo");
			FSDataOutputStream h = null;
			try
			{
				h = hdfsAsUser2.Append(file2);
				h.Write(Sharpen.Runtime.GetBytesForString("Some more data"));
				h.Close();
				h = null;
			}
			finally
			{
				IOUtils.Cleanup(null, h);
			}
		}

		/// <summary>
		/// Test that one user can't delete another user's file when the sticky bit is
		/// set.
		/// </summary>
		/// <exception cref="System.Exception"/>
		private void ConfirmDeletingFiles(Configuration conf, Path p)
		{
			// Write a file to the new temp directory as a regular user
			Path file = new Path(p, "foo");
			WriteFile(hdfsAsUser1, file);
			// Make sure the correct user is the owner
			NUnit.Framework.Assert.AreEqual(user1.GetShortUserName(), hdfsAsUser1.GetFileStatus
				(file).GetOwner());
			// Log onto cluster as another user and attempt to delete the file
			try
			{
				hdfsAsUser2.Delete(file, false);
				NUnit.Framework.Assert.Fail("Shouldn't be able to delete someone else's file with SB on"
					);
			}
			catch (IOException ioe)
			{
				NUnit.Framework.Assert.IsTrue(ioe is AccessControlException);
				NUnit.Framework.Assert.IsTrue(ioe.Message.Contains("sticky bit"));
			}
		}

		/// <summary>
		/// Test that if a directory is created in a directory that has the sticky bit
		/// on, the new directory does not automatically get a sticky bit, as is
		/// standard Unix behavior
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void ConfirmStickyBitDoesntPropagate(FileSystem hdfs, Path p)
		{
			// Create a subdirectory within it
			Path p2 = new Path(p, "bar");
			hdfs.Mkdirs(p2);
			// Ensure new directory doesn't have its sticky bit on
			NUnit.Framework.Assert.IsFalse(hdfs.GetFileStatus(p2).GetPermission().GetStickyBit
				());
		}

		/// <summary>Test basic ability to get and set sticky bits on files and directories.</summary>
		/// <exception cref="System.IO.IOException"/>
		private void ConfirmSettingAndGetting(FileSystem hdfs, Path p, Path baseDir)
		{
			// Initially sticky bit should not be set
			NUnit.Framework.Assert.IsFalse(hdfs.GetFileStatus(p).GetPermission().GetStickyBit
				());
			// Same permission, but with sticky bit on
			short withSB;
			withSB = (short)(hdfs.GetFileStatus(p).GetPermission().ToShort() | 0x200);
			NUnit.Framework.Assert.IsTrue((new FsPermission(withSB)).GetStickyBit());
			hdfs.SetPermission(p, new FsPermission(withSB));
			NUnit.Framework.Assert.IsTrue(hdfs.GetFileStatus(p).GetPermission().GetStickyBit(
				));
			// Write a file to the fs, try to set its sticky bit
			Path f = new Path(baseDir, "somefile");
			WriteFile(hdfs, f);
			NUnit.Framework.Assert.IsFalse(hdfs.GetFileStatus(f).GetPermission().GetStickyBit
				());
			withSB = (short)(hdfs.GetFileStatus(f).GetPermission().ToShort() | 0x200);
			hdfs.SetPermission(f, new FsPermission(withSB));
			NUnit.Framework.Assert.IsTrue(hdfs.GetFileStatus(f).GetPermission().GetStickyBit(
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGeneralSBBehavior()
		{
			Path baseDir = new Path("/mcgann");
			hdfs.Mkdirs(baseDir);
			// Create a tmp directory with wide-open permissions and sticky bit
			Path p = new Path(baseDir, "tmp");
			hdfs.Mkdirs(p);
			hdfs.SetPermission(p, new FsPermission((short)0x3ff));
			ConfirmCanAppend(conf, p);
			baseDir = new Path("/eccleston");
			hdfs.Mkdirs(baseDir);
			p = new Path(baseDir, "roguetraders");
			hdfs.Mkdirs(p);
			ConfirmSettingAndGetting(hdfs, p, baseDir);
			baseDir = new Path("/tennant");
			hdfs.Mkdirs(baseDir);
			p = new Path(baseDir, "contemporary");
			hdfs.Mkdirs(p);
			hdfs.SetPermission(p, new FsPermission((short)0x3ff));
			ConfirmDeletingFiles(conf, p);
			baseDir = new Path("/smith");
			hdfs.Mkdirs(baseDir);
			p = new Path(baseDir, "scissorsisters");
			// Turn on its sticky bit
			hdfs.Mkdirs(p, new FsPermission((short)0x3b6));
			ConfirmStickyBitDoesntPropagate(hdfs, baseDir);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAclGeneralSBBehavior()
		{
			Path baseDir = new Path("/mcgann");
			hdfs.Mkdirs(baseDir);
			// Create a tmp directory with wide-open permissions and sticky bit
			Path p = new Path(baseDir, "tmp");
			hdfs.Mkdirs(p);
			hdfs.SetPermission(p, new FsPermission((short)0x3ff));
			ApplyAcl(p);
			ConfirmCanAppend(conf, p);
			baseDir = new Path("/eccleston");
			hdfs.Mkdirs(baseDir);
			p = new Path(baseDir, "roguetraders");
			hdfs.Mkdirs(p);
			ApplyAcl(p);
			ConfirmSettingAndGetting(hdfs, p, baseDir);
			baseDir = new Path("/tennant");
			hdfs.Mkdirs(baseDir);
			p = new Path(baseDir, "contemporary");
			hdfs.Mkdirs(p);
			hdfs.SetPermission(p, new FsPermission((short)0x3ff));
			ApplyAcl(p);
			ConfirmDeletingFiles(conf, p);
			baseDir = new Path("/smith");
			hdfs.Mkdirs(baseDir);
			p = new Path(baseDir, "scissorsisters");
			// Turn on its sticky bit
			hdfs.Mkdirs(p, new FsPermission((short)0x3b6));
			ApplyAcl(p);
			ConfirmStickyBitDoesntPropagate(hdfs, p);
		}

		/// <summary>
		/// Test that one user can't rename/move another user's file when the sticky
		/// bit is set.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMovingFiles()
		{
			TestMovingFiles(false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAclMovingFiles()
		{
			TestMovingFiles(true);
		}

		/// <exception cref="System.Exception"/>
		private void TestMovingFiles(bool useAcl)
		{
			// Create a tmp directory with wide-open permissions and sticky bit
			Path tmpPath = new Path("/tmp");
			Path tmpPath2 = new Path("/tmp2");
			hdfs.Mkdirs(tmpPath);
			hdfs.Mkdirs(tmpPath2);
			hdfs.SetPermission(tmpPath, new FsPermission((short)0x3ff));
			if (useAcl)
			{
				ApplyAcl(tmpPath);
			}
			hdfs.SetPermission(tmpPath2, new FsPermission((short)0x3ff));
			if (useAcl)
			{
				ApplyAcl(tmpPath2);
			}
			// Write a file to the new tmp directory as a regular user
			Path file = new Path(tmpPath, "foo");
			WriteFile(hdfsAsUser1, file);
			// Log onto cluster as another user and attempt to move the file
			try
			{
				hdfsAsUser2.Rename(file, new Path(tmpPath2, "renamed"));
				NUnit.Framework.Assert.Fail("Shouldn't be able to rename someone else's file with SB on"
					);
			}
			catch (IOException ioe)
			{
				NUnit.Framework.Assert.IsTrue(ioe is AccessControlException);
				NUnit.Framework.Assert.IsTrue(ioe.Message.Contains("sticky bit"));
			}
		}

		/// <summary>
		/// Ensure that when we set a sticky bit and shut down the file system, we get
		/// the sticky bit back on re-start, and that no extra sticky bits appear after
		/// re-start.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStickyBitPersistence()
		{
			// A tale of three directories...
			Path sbSet = new Path("/Housemartins");
			Path sbNotSpecified = new Path("/INXS");
			Path sbSetOff = new Path("/Easyworld");
			foreach (Path p in new Path[] { sbSet, sbNotSpecified, sbSetOff })
			{
				hdfs.Mkdirs(p);
			}
			// Two directories had there sticky bits set explicitly...
			hdfs.SetPermission(sbSet, new FsPermission((short)0x3ff));
			hdfs.SetPermission(sbSetOff, new FsPermission((short)0x1ff));
			Shutdown();
			// Start file system up again
			InitCluster(false);
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(sbSet));
			NUnit.Framework.Assert.IsTrue(hdfs.GetFileStatus(sbSet).GetPermission().GetStickyBit
				());
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(sbNotSpecified));
			NUnit.Framework.Assert.IsFalse(hdfs.GetFileStatus(sbNotSpecified).GetPermission()
				.GetStickyBit());
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(sbSetOff));
			NUnit.Framework.Assert.IsFalse(hdfs.GetFileStatus(sbSetOff).GetPermission().GetStickyBit
				());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAclStickyBitPersistence()
		{
			// A tale of three directories...
			Path sbSet = new Path("/Housemartins");
			Path sbNotSpecified = new Path("/INXS");
			Path sbSetOff = new Path("/Easyworld");
			foreach (Path p in new Path[] { sbSet, sbNotSpecified, sbSetOff })
			{
				hdfs.Mkdirs(p);
			}
			// Two directories had there sticky bits set explicitly...
			hdfs.SetPermission(sbSet, new FsPermission((short)0x3ff));
			ApplyAcl(sbSet);
			hdfs.SetPermission(sbSetOff, new FsPermission((short)0x1ff));
			ApplyAcl(sbSetOff);
			Shutdown();
			// Start file system up again
			InitCluster(false);
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(sbSet));
			NUnit.Framework.Assert.IsTrue(hdfs.GetFileStatus(sbSet).GetPermission().GetStickyBit
				());
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(sbNotSpecified));
			NUnit.Framework.Assert.IsFalse(hdfs.GetFileStatus(sbNotSpecified).GetPermission()
				.GetStickyBit());
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(sbSetOff));
			NUnit.Framework.Assert.IsFalse(hdfs.GetFileStatus(sbSetOff).GetPermission().GetStickyBit
				());
		}

		/// <summary>Write a quick file to the specified file system at specified path</summary>
		/// <exception cref="System.IO.IOException"/>
		private static void WriteFile(FileSystem hdfs, Path p)
		{
			FSDataOutputStream o = null;
			try
			{
				o = hdfs.Create(p);
				o.Write(Sharpen.Runtime.GetBytesForString("some file contents"));
				o.Close();
				o = null;
			}
			finally
			{
				IOUtils.Cleanup(null, o);
			}
		}

		/// <summary>Applies an ACL (both access and default) to the given path.</summary>
		/// <param name="p">Path to set</param>
		/// <exception cref="System.IO.IOException">if an ACL could not be modified</exception>
		private static void ApplyAcl(Path p)
		{
			hdfs.ModifyAclEntries(p, Arrays.AsList(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, user2.GetShortUserName(), FsAction.All), AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.User, user2.GetShortUserName(), FsAction.All
				)));
		}
	}
}
