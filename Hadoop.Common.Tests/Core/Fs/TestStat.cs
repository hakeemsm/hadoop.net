using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	public class TestStat : FileSystemTestHelper
	{
		static TestStat()
		{
			FileSystem.EnableSymlinks();
		}

		private static Stat stat;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void Setup()
		{
			stat = new Stat(new Path("/dummypath"), 4096l, false, FileSystem.Get(new Configuration
				()));
		}

		private class StatOutput
		{
			internal readonly string doesNotExist;

			internal readonly string directory;

			internal readonly string file;

			internal readonly string[] symlinks;

			internal readonly string stickydir;

			internal StatOutput(TestStat _enclosing, string doesNotExist, string directory, string
				 file, string[] symlinks, string stickydir)
			{
				this._enclosing = _enclosing;
				this.doesNotExist = doesNotExist;
				this.directory = directory;
				this.file = file;
				this.symlinks = symlinks;
				this.stickydir = stickydir;
			}

			/// <exception cref="System.Exception"/>
			internal virtual void Test()
			{
				BufferedReader br;
				FileStatus status;
				try
				{
					br = new BufferedReader(new StringReader(this.doesNotExist));
					TestStat.stat.ParseExecResult(br);
				}
				catch (FileNotFoundException)
				{
				}
				// expected
				br = new BufferedReader(new StringReader(this.directory));
				TestStat.stat.ParseExecResult(br);
				status = TestStat.stat.GetFileStatusForTesting();
				Assert.True(status.IsDirectory());
				br = new BufferedReader(new StringReader(this.file));
				TestStat.stat.ParseExecResult(br);
				status = TestStat.stat.GetFileStatusForTesting();
				Assert.True(status.IsFile());
				foreach (string symlink in this.symlinks)
				{
					br = new BufferedReader(new StringReader(symlink));
					TestStat.stat.ParseExecResult(br);
					status = TestStat.stat.GetFileStatusForTesting();
					Assert.True(status.IsSymlink());
				}
				br = new BufferedReader(new StringReader(this.stickydir));
				TestStat.stat.ParseExecResult(br);
				status = TestStat.stat.GetFileStatusForTesting();
				Assert.True(status.IsDirectory());
				Assert.True(status.GetPermission().GetStickyBit());
			}

			private readonly TestStat _enclosing;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestStatLinux()
		{
			string[] symlinks = new string[] { "6,symbolic link,1373584236,1373584236,777,andrew,andrew,`link' -> `target'"
				, "6,symbolic link,1373584236,1373584236,777,andrew,andrew,'link' -> 'target'" };
			TestStat.StatOutput linux = new TestStat.StatOutput(this, "stat: cannot stat `watermelon': No such file or directory"
				, "4096,directory,1373584236,1373586485,755,andrew,root,`.'", "0,regular empty file,1373584228,1373584228,644,andrew,andrew,`target'"
				, symlinks, "4096,directory,1374622334,1375124212,1755,andrew,andrew,`stickydir'"
				);
			linux.Test();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestStatFreeBSD()
		{
			string[] symlinks = new string[] { "6,Symbolic Link,1373508941,1373508941,120755,awang,awang,`link' -> `target'"
				 };
			TestStat.StatOutput freebsd = new TestStat.StatOutput(this, "stat: symtest/link: stat: No such file or directory"
				, "512,Directory,1373583695,1373583669,40755,awang,awang,`link' -> `'", "0,Regular File,1373508937,1373508937,100644,awang,awang,`link' -> `'"
				, symlinks, "512,Directory,1375139537,1375139537,41755,awang,awang,`link' -> `'"
				);
			freebsd.Test();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestStatFileNotFound()
		{
			Assume.AssumeTrue(Stat.IsAvailable());
			try
			{
				stat.GetFileStatus();
				NUnit.Framework.Assert.Fail("Expected FileNotFoundException");
			}
			catch (FileNotFoundException)
			{
			}
		}

		// expected
		/// <exception cref="System.Exception"/>
		public virtual void TestStatEnvironment()
		{
			Assert.Equal("C", stat.GetEnvironment("LANG"));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestStat()
		{
			Assume.AssumeTrue(Stat.IsAvailable());
			FileSystem fs = FileSystem.GetLocal(new Configuration());
			Path testDir = new Path(GetTestRootPath(fs), "teststat");
			fs.Mkdirs(testDir);
			Path sub1 = new Path(testDir, "sub1");
			Path sub2 = new Path(testDir, "sub2");
			fs.Mkdirs(sub1);
			fs.CreateSymlink(sub1, sub2, false);
			FileStatus stat1 = new Stat(sub1, 4096l, false, fs).GetFileStatus();
			FileStatus stat2 = new Stat(sub2, 0, false, fs).GetFileStatus();
			Assert.True(stat1.IsDirectory());
			NUnit.Framework.Assert.IsFalse(stat2.IsDirectory());
			fs.Delete(testDir, true);
		}
	}
}
