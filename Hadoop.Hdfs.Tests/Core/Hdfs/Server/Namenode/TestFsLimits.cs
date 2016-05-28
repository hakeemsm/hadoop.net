using System;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestFsLimits
	{
		internal static Configuration conf;

		internal static FSNamesystem fs;

		internal static bool fsIsReady;

		internal static readonly PermissionStatus perms = new PermissionStatus("admin", "admin"
			, FsPermission.GetDefault());

		/// <exception cref="System.IO.IOException"/>
		private static FSNamesystem GetMockNamesystem()
		{
			FSImage fsImage = Org.Mockito.Mockito.Mock<FSImage>();
			FSEditLog editLog = Org.Mockito.Mockito.Mock<FSEditLog>();
			Org.Mockito.Mockito.DoReturn(editLog).When(fsImage).GetEditLog();
			FSNamesystem fsn = new FSNamesystem(conf, fsImage);
			fsn.SetImageLoaded(fsIsReady);
			return fsn;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new Configuration();
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, Util.FileAsURI(new FilePath(MiniDFSCluster
				.GetBaseDirectory(), "namenode")).ToString());
			NameNode.InitMetrics(conf, HdfsServerConstants.NamenodeRole.Namenode);
			fs = null;
			fsIsReady = true;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNoLimits()
		{
			Mkdirs("/1", null);
			Mkdirs("/22", null);
			Mkdirs("/333", null);
			Mkdirs("/4444", null);
			Mkdirs("/55555", null);
			Mkdirs("/1/" + HdfsConstants.DotSnapshotDir, typeof(HadoopIllegalArgumentException
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMaxComponentLength()
		{
			conf.SetInt(DFSConfigKeys.DfsNamenodeMaxComponentLengthKey, 2);
			Mkdirs("/1", null);
			Mkdirs("/22", null);
			Mkdirs("/333", typeof(FSLimitException.PathComponentTooLongException));
			Mkdirs("/4444", typeof(FSLimitException.PathComponentTooLongException));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMaxComponentLengthRename()
		{
			conf.SetInt(DFSConfigKeys.DfsNamenodeMaxComponentLengthKey, 2);
			Mkdirs("/5", null);
			Rename("/5", "/555", typeof(FSLimitException.PathComponentTooLongException));
			Rename("/5", "/55", null);
			Mkdirs("/6", null);
			DeprecatedRename("/6", "/666", typeof(FSLimitException.PathComponentTooLongException
				));
			DeprecatedRename("/6", "/66", null);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMaxDirItems()
		{
			conf.SetInt(DFSConfigKeys.DfsNamenodeMaxDirectoryItemsKey, 2);
			Mkdirs("/1", null);
			Mkdirs("/22", null);
			Mkdirs("/333", typeof(FSLimitException.MaxDirectoryItemsExceededException));
			Mkdirs("/4444", typeof(FSLimitException.MaxDirectoryItemsExceededException));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMaxDirItemsRename()
		{
			conf.SetInt(DFSConfigKeys.DfsNamenodeMaxDirectoryItemsKey, 2);
			Mkdirs("/1", null);
			Mkdirs("/2", null);
			Mkdirs("/2/A", null);
			Rename("/2/A", "/A", typeof(FSLimitException.MaxDirectoryItemsExceededException));
			Rename("/2/A", "/1/A", null);
			Mkdirs("/2/B", null);
			DeprecatedRename("/2/B", "/B", typeof(FSLimitException.MaxDirectoryItemsExceededException
				));
			DeprecatedRename("/2/B", "/1/B", null);
			Rename("/1", "/3", null);
			DeprecatedRename("/2", "/4", null);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMaxDirItemsLimits()
		{
			conf.SetInt(DFSConfigKeys.DfsNamenodeMaxDirectoryItemsKey, 0);
			try
			{
				Mkdirs("1", null);
			}
			catch (ArgumentException e)
			{
				GenericTestUtils.AssertExceptionContains("Cannot set dfs", e);
			}
			conf.SetInt(DFSConfigKeys.DfsNamenodeMaxDirectoryItemsKey, 64 * 100 * 1024);
			try
			{
				Mkdirs("1", null);
			}
			catch (ArgumentException e)
			{
				GenericTestUtils.AssertExceptionContains("Cannot set dfs", e);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMaxComponentsAndMaxDirItems()
		{
			conf.SetInt(DFSConfigKeys.DfsNamenodeMaxComponentLengthKey, 3);
			conf.SetInt(DFSConfigKeys.DfsNamenodeMaxDirectoryItemsKey, 2);
			Mkdirs("/1", null);
			Mkdirs("/22", null);
			Mkdirs("/333", typeof(FSLimitException.MaxDirectoryItemsExceededException));
			Mkdirs("/4444", typeof(FSLimitException.PathComponentTooLongException));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDuringEditLogs()
		{
			conf.SetInt(DFSConfigKeys.DfsNamenodeMaxComponentLengthKey, 3);
			conf.SetInt(DFSConfigKeys.DfsNamenodeMaxDirectoryItemsKey, 2);
			fsIsReady = false;
			Mkdirs("/1", null);
			Mkdirs("/22", null);
			Mkdirs("/333", null);
			Mkdirs("/4444", null);
			Mkdirs("/1/" + HdfsConstants.DotSnapshotDir, typeof(HadoopIllegalArgumentException
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestParentDirectoryNameIsCorrect()
		{
			conf.SetInt(DFSConfigKeys.DfsNamenodeMaxComponentLengthKey, 20);
			Mkdirs("/user", null);
			Mkdirs("/user/testHome", null);
			Mkdirs("/user/testHome/FileNameLength", null);
			MkdirCheckParentDirectory("/user/testHome/FileNameLength/really_big_name_0003_fail"
				, "/user/testHome/FileNameLength", typeof(FSLimitException.PathComponentTooLongException
				));
			RenameCheckParentDirectory("/user/testHome/FileNameLength", "/user/testHome/really_big_name_0003_fail"
				, "/user/testHome/", typeof(FSLimitException.PathComponentTooLongException));
		}

		/// <summary>Verifies that Parent Directory is correct after a failed call to mkdir</summary>
		/// <param name="name">Directory Name</param>
		/// <param name="ParentDirName">Expected Parent Directory</param>
		/// <param name="expected">Exception that is expected</param>
		/// <exception cref="System.Exception"/>
		private void MkdirCheckParentDirectory(string name, string ParentDirName, Type expected
			)
		{
			Verify(Mkdirs(name, expected), ParentDirName);
		}

		/// <summary>
		/// /
		/// Verifies that Parent Directory is correct after a failed call to mkdir
		/// </summary>
		/// <param name="name">Directory Name</param>
		/// <param name="dst">Destination Name</param>
		/// <param name="ParentDirName">Expected Parent Directory</param>
		/// <param name="expected">Exception that is expected</param>
		/// <exception cref="System.Exception"/>
		private void RenameCheckParentDirectory(string name, string dst, string ParentDirName
			, Type expected)
		{
			Verify(Rename(name, dst, expected), ParentDirName);
		}

		/// <summary>verifies the ParentDirectory Name is present in the message given.</summary>
		/// <param name="message">- Expection Message</param>
		/// <param name="ParentDirName">- Parent Directory Name to look for.</param>
		private void Verify(string message, string ParentDirName)
		{
			bool found = false;
			if (message != null)
			{
				string[] tokens = message.Split("\\s+");
				foreach (string token in tokens)
				{
					if (token != null && token.Equals(ParentDirName))
					{
						found = true;
						break;
					}
				}
			}
			NUnit.Framework.Assert.IsTrue(found);
		}

		/// <exception cref="System.Exception"/>
		private string Mkdirs(string name, Type expected)
		{
			LazyInitFSDirectory();
			Type generated = null;
			string errorString = null;
			try
			{
				fs.Mkdirs(name, perms, false);
			}
			catch (Exception e)
			{
				generated = e.GetType();
				Sharpen.Runtime.PrintStackTrace(e);
				errorString = e.Message;
			}
			NUnit.Framework.Assert.AreEqual(expected, generated);
			return errorString;
		}

		/// <exception cref="System.Exception"/>
		private string Rename(string src, string dst, Type expected)
		{
			LazyInitFSDirectory();
			Type generated = null;
			string errorString = null;
			try
			{
				fs.RenameTo(src, dst, false, new Options.Rename[] {  });
			}
			catch (Exception e)
			{
				generated = e.GetType();
				errorString = e.Message;
			}
			NUnit.Framework.Assert.AreEqual(expected, generated);
			return errorString;
		}

		/// <exception cref="System.Exception"/>
		private void DeprecatedRename(string src, string dst, Type expected)
		{
			LazyInitFSDirectory();
			Type generated = null;
			try
			{
				fs.RenameTo(src, dst, false);
			}
			catch (Exception e)
			{
				generated = e.GetType();
			}
			NUnit.Framework.Assert.AreEqual(expected, generated);
		}

		/// <exception cref="System.IO.IOException"/>
		private static void LazyInitFSDirectory()
		{
			// have to create after the caller has had a chance to set conf values
			if (fs == null)
			{
				fs = GetMockNamesystem();
			}
		}
	}
}
