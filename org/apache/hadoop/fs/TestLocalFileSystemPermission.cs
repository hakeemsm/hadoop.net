using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>This class tests the local file system via the FileSystem abstraction.</summary>
	public class TestLocalFileSystemPermission : NUnit.Framework.TestCase
	{
		internal static readonly string TEST_PATH_PREFIX = new org.apache.hadoop.fs.Path(
			Sharpen.Runtime.getProperty("test.build.data", "/tmp")).ToString().Replace(' ', 
			'_') + "/" + Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.TestLocalFileSystemPermission
			)).getSimpleName() + "_";

		/// <exception cref="System.IO.IOException"/>
		private org.apache.hadoop.fs.Path writeFile(org.apache.hadoop.fs.FileSystem fs, string
			 name)
		{
			org.apache.hadoop.fs.Path f = new org.apache.hadoop.fs.Path(TEST_PATH_PREFIX + name
				);
			org.apache.hadoop.fs.FSDataOutputStream stm = fs.create(f);
			stm.writeBytes("42\n");
			stm.close();
			return f;
		}

		/// <exception cref="System.IO.IOException"/>
		private void cleanupFile(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.fs.Path
			 name)
		{
			NUnit.Framework.Assert.IsTrue(fs.exists(name));
			fs.delete(name, true);
			NUnit.Framework.Assert.IsTrue(!fs.exists(name));
		}

		/// <summary>Test LocalFileSystem.setPermission</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testLocalFSsetPermission()
		{
			if (org.apache.hadoop.fs.Path.WINDOWS)
			{
				System.Console.Out.WriteLine("Cannot run test for Windows");
				return;
			}
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.fs.LocalFileSystem localfs = org.apache.hadoop.fs.FileSystem.getLocal
				(conf);
			string filename = "foo";
			org.apache.hadoop.fs.Path f = writeFile(localfs, filename);
			try
			{
				org.apache.hadoop.fs.permission.FsPermission initialPermission = getPermission(localfs
					, f);
				System.Console.Out.WriteLine(filename + ": " + initialPermission);
				NUnit.Framework.Assert.AreEqual(org.apache.hadoop.fs.permission.FsPermission.getFileDefault
					().applyUMask(org.apache.hadoop.fs.permission.FsPermission.getUMask(conf)), initialPermission
					);
			}
			catch (System.Exception e)
			{
				System.Console.Out.WriteLine(org.apache.hadoop.util.StringUtils.stringifyException
					(e));
				System.Console.Out.WriteLine("Cannot run test");
				return;
			}
			try
			{
				// create files and manipulate them.
				org.apache.hadoop.fs.permission.FsPermission all = new org.apache.hadoop.fs.permission.FsPermission
					((short)0x1ff);
				org.apache.hadoop.fs.permission.FsPermission none = new org.apache.hadoop.fs.permission.FsPermission
					((short)0);
				localfs.setPermission(f, none);
				NUnit.Framework.Assert.AreEqual(none, getPermission(localfs, f));
				localfs.setPermission(f, all);
				NUnit.Framework.Assert.AreEqual(all, getPermission(localfs, f));
			}
			finally
			{
				cleanupFile(localfs, f);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual org.apache.hadoop.fs.permission.FsPermission getPermission(org.apache.hadoop.fs.LocalFileSystem
			 fs, org.apache.hadoop.fs.Path p)
		{
			return fs.getFileStatus(p).getPermission();
		}

		/// <summary>Test LocalFileSystem.setOwner</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testLocalFSsetOwner()
		{
			if (org.apache.hadoop.fs.Path.WINDOWS)
			{
				System.Console.Out.WriteLine("Cannot run test for Windows");
				return;
			}
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.fs.LocalFileSystem localfs = org.apache.hadoop.fs.FileSystem.getLocal
				(conf);
			string filename = "bar";
			org.apache.hadoop.fs.Path f = writeFile(localfs, filename);
			System.Collections.Generic.IList<string> groups = null;
			try
			{
				groups = getGroups();
				System.Console.Out.WriteLine(filename + ": " + getPermission(localfs, f));
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
				localfs.setOwner(f, null, g0);
				NUnit.Framework.Assert.AreEqual(g0, getGroup(localfs, f));
				if (groups.Count > 1)
				{
					string g1 = groups[1];
					localfs.setOwner(f, null, g1);
					NUnit.Framework.Assert.AreEqual(g1, getGroup(localfs, f));
				}
				else
				{
					System.Console.Out.WriteLine("Not testing changing the group since user " + "belongs to only one group."
						);
				}
			}
			finally
			{
				cleanupFile(localfs, f);
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

		/// <exception cref="System.IO.IOException"/>
		internal virtual string getGroup(org.apache.hadoop.fs.LocalFileSystem fs, org.apache.hadoop.fs.Path
			 p)
		{
			return fs.getFileStatus(p).getGroup();
		}

		public TestLocalFileSystemPermission()
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
