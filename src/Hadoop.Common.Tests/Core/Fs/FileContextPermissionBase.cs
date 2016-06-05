using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;


namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// <p>
	/// A collection of permission tests for the
	/// <see cref="FileContext"/>
	/// .
	/// This test should be used for testing an instance of FileContext
	/// that has been initialized to a specific default FileSystem such a
	/// LocalFileSystem, HDFS,S3, etc.
	/// </p>
	/// <p>
	/// To test a given
	/// <see cref="FileSystem"/>
	/// implementation create a subclass of this
	/// test and override
	/// <see cref="SetUp()"/>
	/// to initialize the <code>fc</code>
	/// <see cref="FileContext"/>
	/// instance variable.
	/// Since this a junit 4 you can also do a single setup before
	/// the start of any tests.
	/// E.g.
	/// </summary>
	/// <BeforeClass>public static void clusterSetupAtBegining()</BeforeClass>
	/// <AfterClass>
	/// public static void ClusterShutdownAtEnd()
	/// </p>
	/// </AfterClass>
	public abstract class FileContextPermissionBase
	{
		protected internal FileContextTestHelper fileContextTestHelper;

		protected internal FileContext fc;

		protected internal virtual FileContextTestHelper GetFileContextHelper()
		{
			return new FileContextTestHelper();
		}

		/// <exception cref="System.Exception"/>
		protected internal abstract FileContext GetFileContext();

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			fileContextTestHelper = GetFileContextHelper();
			fc = GetFileContext();
			fc.Mkdir(fileContextTestHelper.GetTestRootPath(fc), FileContext.DefaultPerm, true
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			fc.Delete(fileContextTestHelper.GetTestRootPath(fc), true);
		}

		/// <exception cref="System.IO.IOException"/>
		private void CleanupFile(FileContext fc, Path name)
		{
			Assert.True(FileContextTestHelper.Exists(fc, name));
			fc.Delete(name, true);
			Assert.True(!FileContextTestHelper.Exists(fc, name));
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestCreatePermission()
		{
			if (Path.Windows)
			{
				System.Console.Out.WriteLine("Cannot run test for Windows");
				return;
			}
			string filename = "foo";
			Path f = fileContextTestHelper.GetTestRootPath(fc, filename);
			fileContextTestHelper.CreateFile(fc, filename);
			DoFilePermissionCheck(FileContext.FileDefaultPerm.ApplyUMask(fc.GetUMask()), fc.GetFileStatus
				(f).GetPermission());
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestSetPermission()
		{
			if (Path.Windows)
			{
				System.Console.Out.WriteLine("Cannot run test for Windows");
				return;
			}
			string filename = "foo";
			Path f = fileContextTestHelper.GetTestRootPath(fc, filename);
			FileContextTestHelper.CreateFile(fc, f);
			try
			{
				// create files and manipulate them.
				FsPermission all = new FsPermission((short)0x1ff);
				FsPermission none = new FsPermission((short)0);
				fc.SetPermission(f, none);
				DoFilePermissionCheck(none, fc.GetFileStatus(f).GetPermission());
				fc.SetPermission(f, all);
				DoFilePermissionCheck(all, fc.GetFileStatus(f).GetPermission());
			}
			finally
			{
				CleanupFile(fc, f);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestSetOwner()
		{
			if (Path.Windows)
			{
				System.Console.Out.WriteLine("Cannot run test for Windows");
				return;
			}
			string filename = "bar";
			Path f = fileContextTestHelper.GetTestRootPath(fc, filename);
			FileContextTestHelper.CreateFile(fc, f);
			IList<string> groups = null;
			try
			{
				groups = GetGroups();
				System.Console.Out.WriteLine(filename + ": " + fc.GetFileStatus(f).GetPermission(
					));
			}
			catch (IOException e)
			{
				System.Console.Out.WriteLine(StringUtils.StringifyException(e));
				System.Console.Out.WriteLine("Cannot run test");
				return;
			}
			if (groups == null || groups.Count < 1)
			{
				System.Console.Out.WriteLine("Cannot run test: need at least one group.  groups="
					 + groups);
				return;
			}
			// create files and manipulate them.
			try
			{
				string g0 = groups[0];
				fc.SetOwner(f, null, g0);
				Assert.Equal(g0, fc.GetFileStatus(f).GetGroup());
				if (groups.Count > 1)
				{
					string g1 = groups[1];
					fc.SetOwner(f, null, g1);
					Assert.Equal(g1, fc.GetFileStatus(f).GetGroup());
				}
				else
				{
					System.Console.Out.WriteLine("Not testing changing the group since user " + "belongs to only one group."
						);
				}
				try
				{
					fc.SetOwner(f, null, null);
					NUnit.Framework.Assert.Fail("Exception expected.");
				}
				catch (ArgumentException)
				{
				}
			}
			finally
			{
				// okay
				CleanupFile(fc, f);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestUgi()
		{
			UserGroupInformation otherUser = UserGroupInformation.CreateRemoteUser("otherUser"
				);
			FileContext newFc = otherUser.DoAs(new _PrivilegedExceptionAction_194());
			Assert.Equal("otherUser", newFc.GetUgi().GetUserName());
		}

		private sealed class _PrivilegedExceptionAction_194 : PrivilegedExceptionAction<FileContext
			>
		{
			public _PrivilegedExceptionAction_194()
			{
			}

			/// <exception cref="System.Exception"/>
			public FileContext Run()
			{
				FileContext newFc = FileContext.GetFileContext();
				return newFc;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static IList<string> GetGroups()
		{
			IList<string> a = new AList<string>();
			string s = Shell.ExecCommand(Shell.GetGroupsCommand());
			for (StringTokenizer t = new StringTokenizer(s); t.HasMoreTokens(); )
			{
				a.AddItem(t.NextToken());
			}
			return a;
		}

		internal virtual void DoFilePermissionCheck(FsPermission expectedPerm, FsPermission
			 actualPerm)
		{
			Assert.Equal(expectedPerm.ApplyUMask(GetFileMask()), actualPerm
				);
		}

		internal static readonly FsPermission FileMaskZero = new FsPermission((short)0);

		/*
		* Override the method below if the file system being tested masks our
		* certain bits for file masks.
		*/
		internal virtual FsPermission GetFileMask()
		{
			return FileMaskZero;
		}

		public FileContextPermissionBase()
		{
			{
				try
				{
					((Log4JLogger)FileSystem.Log).GetLogger().SetLevel(Level.Debug);
				}
				catch (Exception e)
				{
					System.Console.Out.WriteLine("Cannot change log level\n" + StringUtils.StringifyException
						(e));
				}
			}
		}
	}
}
