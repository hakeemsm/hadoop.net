using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>This class tests the local file system via the FileSystem abstraction.</summary>
	public class TestLocalFileSystemPermission : TestCase
	{
		internal static readonly string TestPathPrefix = new Path(Runtime.GetProperty("test.build.data"
			, "/tmp")).ToString().Replace(' ', '_') + "/" + typeof(TestLocalFileSystemPermission
			).Name + "_";

		/// <exception cref="System.IO.IOException"/>
		private Path WriteFile(FileSystem fs, string name)
		{
			Path f = new Path(TestPathPrefix + name);
			FSDataOutputStream stm = fs.Create(f);
			stm.WriteBytes("42\n");
			stm.Close();
			return f;
		}

		/// <exception cref="System.IO.IOException"/>
		private void CleanupFile(FileSystem fs, Path name)
		{
			NUnit.Framework.Assert.IsTrue(fs.Exists(name));
			fs.Delete(name, true);
			NUnit.Framework.Assert.IsTrue(!fs.Exists(name));
		}

		/// <summary>Test LocalFileSystem.setPermission</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestLocalFSsetPermission()
		{
			if (Path.Windows)
			{
				System.Console.Out.WriteLine("Cannot run test for Windows");
				return;
			}
			Configuration conf = new Configuration();
			LocalFileSystem localfs = FileSystem.GetLocal(conf);
			string filename = "foo";
			Path f = WriteFile(localfs, filename);
			try
			{
				FsPermission initialPermission = GetPermission(localfs, f);
				System.Console.Out.WriteLine(filename + ": " + initialPermission);
				NUnit.Framework.Assert.AreEqual(FsPermission.GetFileDefault().ApplyUMask(FsPermission
					.GetUMask(conf)), initialPermission);
			}
			catch (Exception e)
			{
				System.Console.Out.WriteLine(StringUtils.StringifyException(e));
				System.Console.Out.WriteLine("Cannot run test");
				return;
			}
			try
			{
				// create files and manipulate them.
				FsPermission all = new FsPermission((short)0x1ff);
				FsPermission none = new FsPermission((short)0);
				localfs.SetPermission(f, none);
				NUnit.Framework.Assert.AreEqual(none, GetPermission(localfs, f));
				localfs.SetPermission(f, all);
				NUnit.Framework.Assert.AreEqual(all, GetPermission(localfs, f));
			}
			finally
			{
				CleanupFile(localfs, f);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual FsPermission GetPermission(LocalFileSystem fs, Path p)
		{
			return fs.GetFileStatus(p).GetPermission();
		}

		/// <summary>Test LocalFileSystem.setOwner</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestLocalFSsetOwner()
		{
			if (Path.Windows)
			{
				System.Console.Out.WriteLine("Cannot run test for Windows");
				return;
			}
			Configuration conf = new Configuration();
			LocalFileSystem localfs = FileSystem.GetLocal(conf);
			string filename = "bar";
			Path f = WriteFile(localfs, filename);
			IList<string> groups = null;
			try
			{
				groups = GetGroups();
				System.Console.Out.WriteLine(filename + ": " + GetPermission(localfs, f));
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
				localfs.SetOwner(f, null, g0);
				NUnit.Framework.Assert.AreEqual(g0, GetGroup(localfs, f));
				if (groups.Count > 1)
				{
					string g1 = groups[1];
					localfs.SetOwner(f, null, g1);
					NUnit.Framework.Assert.AreEqual(g1, GetGroup(localfs, f));
				}
				else
				{
					System.Console.Out.WriteLine("Not testing changing the group since user " + "belongs to only one group."
						);
				}
			}
			finally
			{
				CleanupFile(localfs, f);
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

		/// <exception cref="System.IO.IOException"/>
		internal virtual string GetGroup(LocalFileSystem fs, Path p)
		{
			return fs.GetFileStatus(p).GetGroup();
		}

		public TestLocalFileSystemPermission()
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
