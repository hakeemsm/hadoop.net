using Sharpen;

namespace org.apache.hadoop.fs
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
	/// <see cref="setUp()"/>
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
		protected internal org.apache.hadoop.fs.FileContextTestHelper fileContextTestHelper;

		protected internal org.apache.hadoop.fs.FileContext fc;

		protected internal virtual org.apache.hadoop.fs.FileContextTestHelper getFileContextHelper
			()
		{
			return new org.apache.hadoop.fs.FileContextTestHelper();
		}

		/// <exception cref="System.Exception"/>
		protected internal abstract org.apache.hadoop.fs.FileContext getFileContext();

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void setUp()
		{
			fileContextTestHelper = getFileContextHelper();
			fc = getFileContext();
			fc.mkdir(fileContextTestHelper.getTestRootPath(fc), org.apache.hadoop.fs.FileContext
				.DEFAULT_PERM, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void tearDown()
		{
			fc.delete(fileContextTestHelper.getTestRootPath(fc), true);
		}

		/// <exception cref="System.IO.IOException"/>
		private void cleanupFile(org.apache.hadoop.fs.FileContext fc, org.apache.hadoop.fs.Path
			 name)
		{
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.exists(fc
				, name));
			fc.delete(name, true);
			NUnit.Framework.Assert.IsTrue(!org.apache.hadoop.fs.FileContextTestHelper.exists(
				fc, name));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testCreatePermission()
		{
			if (org.apache.hadoop.fs.Path.WINDOWS)
			{
				System.Console.Out.WriteLine("Cannot run test for Windows");
				return;
			}
			string filename = "foo";
			org.apache.hadoop.fs.Path f = fileContextTestHelper.getTestRootPath(fc, filename);
			fileContextTestHelper.createFile(fc, filename);
			doFilePermissionCheck(org.apache.hadoop.fs.FileContext.FILE_DEFAULT_PERM.applyUMask
				(fc.getUMask()), fc.getFileStatus(f).getPermission());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testSetPermission()
		{
			if (org.apache.hadoop.fs.Path.WINDOWS)
			{
				System.Console.Out.WriteLine("Cannot run test for Windows");
				return;
			}
			string filename = "foo";
			org.apache.hadoop.fs.Path f = fileContextTestHelper.getTestRootPath(fc, filename);
			org.apache.hadoop.fs.FileContextTestHelper.createFile(fc, f);
			try
			{
				// create files and manipulate them.
				org.apache.hadoop.fs.permission.FsPermission all = new org.apache.hadoop.fs.permission.FsPermission
					((short)0x1ff);
				org.apache.hadoop.fs.permission.FsPermission none = new org.apache.hadoop.fs.permission.FsPermission
					((short)0);
				fc.setPermission(f, none);
				doFilePermissionCheck(none, fc.getFileStatus(f).getPermission());
				fc.setPermission(f, all);
				doFilePermissionCheck(all, fc.getFileStatus(f).getPermission());
			}
			finally
			{
				cleanupFile(fc, f);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testSetOwner()
		{
			if (org.apache.hadoop.fs.Path.WINDOWS)
			{
				System.Console.Out.WriteLine("Cannot run test for Windows");
				return;
			}
			string filename = "bar";
			org.apache.hadoop.fs.Path f = fileContextTestHelper.getTestRootPath(fc, filename);
			org.apache.hadoop.fs.FileContextTestHelper.createFile(fc, f);
			System.Collections.Generic.IList<string> groups = null;
			try
			{
				groups = getGroups();
				System.Console.Out.WriteLine(filename + ": " + fc.getFileStatus(f).getPermission(
					));
			}
			catch (System.IO.IOException e)
			{
				System.Console.Out.WriteLine(org.apache.hadoop.util.StringUtils.stringifyException
					(e));
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
				fc.setOwner(f, null, g0);
				NUnit.Framework.Assert.AreEqual(g0, fc.getFileStatus(f).getGroup());
				if (groups.Count > 1)
				{
					string g1 = groups[1];
					fc.setOwner(f, null, g1);
					NUnit.Framework.Assert.AreEqual(g1, fc.getFileStatus(f).getGroup());
				}
				else
				{
					System.Console.Out.WriteLine("Not testing changing the group since user " + "belongs to only one group."
						);
				}
				try
				{
					fc.setOwner(f, null, null);
					NUnit.Framework.Assert.Fail("Exception expected.");
				}
				catch (System.ArgumentException)
				{
				}
			}
			finally
			{
				// okay
				cleanupFile(fc, f);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testUgi()
		{
			org.apache.hadoop.security.UserGroupInformation otherUser = org.apache.hadoop.security.UserGroupInformation
				.createRemoteUser("otherUser");
			org.apache.hadoop.fs.FileContext newFc = otherUser.doAs(new _PrivilegedExceptionAction_194
				());
			NUnit.Framework.Assert.AreEqual("otherUser", newFc.getUgi().getUserName());
		}

		private sealed class _PrivilegedExceptionAction_194 : java.security.PrivilegedExceptionAction
			<org.apache.hadoop.fs.FileContext>
		{
			public _PrivilegedExceptionAction_194()
			{
			}

			/// <exception cref="System.Exception"/>
			public org.apache.hadoop.fs.FileContext run()
			{
				org.apache.hadoop.fs.FileContext newFc = org.apache.hadoop.fs.FileContext.getFileContext
					();
				return newFc;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static System.Collections.Generic.IList<string> getGroups()
		{
			System.Collections.Generic.IList<string> a = new System.Collections.Generic.List<
				string>();
			string s = org.apache.hadoop.util.Shell.execCommand(org.apache.hadoop.util.Shell.
				getGroupsCommand());
			for (java.util.StringTokenizer t = new java.util.StringTokenizer(s); t.hasMoreTokens
				(); )
			{
				a.add(t.nextToken());
			}
			return a;
		}

		internal virtual void doFilePermissionCheck(org.apache.hadoop.fs.permission.FsPermission
			 expectedPerm, org.apache.hadoop.fs.permission.FsPermission actualPerm)
		{
			NUnit.Framework.Assert.AreEqual(expectedPerm.applyUMask(getFileMask()), actualPerm
				);
		}

		internal static readonly org.apache.hadoop.fs.permission.FsPermission FILE_MASK_ZERO
			 = new org.apache.hadoop.fs.permission.FsPermission((short)0);

		/*
		* Override the method below if the file system being tested masks our
		* certain bits for file masks.
		*/
		internal virtual org.apache.hadoop.fs.permission.FsPermission getFileMask()
		{
			return FILE_MASK_ZERO;
		}

		public FileContextPermissionBase()
		{
			{
				try
				{
					((org.apache.commons.logging.impl.Log4JLogger)org.apache.hadoop.fs.FileSystem.LOG
						).getLogger().setLevel(org.apache.log4j.Level.DEBUG);
				}
				catch (System.Exception e)
				{
					System.Console.Out.WriteLine("Cannot change log level\n" + org.apache.hadoop.util.StringUtils
						.stringifyException(e));
				}
			}
		}
	}
}
