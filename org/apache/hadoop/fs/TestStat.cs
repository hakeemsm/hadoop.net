using Sharpen;

namespace org.apache.hadoop.fs
{
	public class TestStat : org.apache.hadoop.fs.FileSystemTestHelper
	{
		static TestStat()
		{
			org.apache.hadoop.fs.FileSystem.enableSymlinks();
		}

		private static org.apache.hadoop.fs.Stat stat;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.BeforeClass]
		public static void setup()
		{
			stat = new org.apache.hadoop.fs.Stat(new org.apache.hadoop.fs.Path("/dummypath"), 
				4096l, false, org.apache.hadoop.fs.FileSystem.get(new org.apache.hadoop.conf.Configuration
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
			internal virtual void test()
			{
				java.io.BufferedReader br;
				org.apache.hadoop.fs.FileStatus status;
				try
				{
					br = new java.io.BufferedReader(new java.io.StringReader(this.doesNotExist));
					org.apache.hadoop.fs.TestStat.stat.parseExecResult(br);
				}
				catch (java.io.FileNotFoundException)
				{
				}
				// expected
				br = new java.io.BufferedReader(new java.io.StringReader(this.directory));
				org.apache.hadoop.fs.TestStat.stat.parseExecResult(br);
				status = org.apache.hadoop.fs.TestStat.stat.getFileStatusForTesting();
				NUnit.Framework.Assert.IsTrue(status.isDirectory());
				br = new java.io.BufferedReader(new java.io.StringReader(this.file));
				org.apache.hadoop.fs.TestStat.stat.parseExecResult(br);
				status = org.apache.hadoop.fs.TestStat.stat.getFileStatusForTesting();
				NUnit.Framework.Assert.IsTrue(status.isFile());
				foreach (string symlink in this.symlinks)
				{
					br = new java.io.BufferedReader(new java.io.StringReader(symlink));
					org.apache.hadoop.fs.TestStat.stat.parseExecResult(br);
					status = org.apache.hadoop.fs.TestStat.stat.getFileStatusForTesting();
					NUnit.Framework.Assert.IsTrue(status.isSymlink());
				}
				br = new java.io.BufferedReader(new java.io.StringReader(this.stickydir));
				org.apache.hadoop.fs.TestStat.stat.parseExecResult(br);
				status = org.apache.hadoop.fs.TestStat.stat.getFileStatusForTesting();
				NUnit.Framework.Assert.IsTrue(status.isDirectory());
				NUnit.Framework.Assert.IsTrue(status.getPermission().getStickyBit());
			}

			private readonly TestStat _enclosing;
		}

		/// <exception cref="System.Exception"/>
		public virtual void testStatLinux()
		{
			string[] symlinks = new string[] { "6,symbolic link,1373584236,1373584236,777,andrew,andrew,`link' -> `target'"
				, "6,symbolic link,1373584236,1373584236,777,andrew,andrew,'link' -> 'target'" };
			org.apache.hadoop.fs.TestStat.StatOutput linux = new org.apache.hadoop.fs.TestStat.StatOutput
				(this, "stat: cannot stat `watermelon': No such file or directory", "4096,directory,1373584236,1373586485,755,andrew,root,`.'"
				, "0,regular empty file,1373584228,1373584228,644,andrew,andrew,`target'", symlinks
				, "4096,directory,1374622334,1375124212,1755,andrew,andrew,`stickydir'");
			linux.test();
		}

		/// <exception cref="System.Exception"/>
		public virtual void testStatFreeBSD()
		{
			string[] symlinks = new string[] { "6,Symbolic Link,1373508941,1373508941,120755,awang,awang,`link' -> `target'"
				 };
			org.apache.hadoop.fs.TestStat.StatOutput freebsd = new org.apache.hadoop.fs.TestStat.StatOutput
				(this, "stat: symtest/link: stat: No such file or directory", "512,Directory,1373583695,1373583669,40755,awang,awang,`link' -> `'"
				, "0,Regular File,1373508937,1373508937,100644,awang,awang,`link' -> `'", symlinks
				, "512,Directory,1375139537,1375139537,41755,awang,awang,`link' -> `'");
			freebsd.test();
		}

		/// <exception cref="System.Exception"/>
		public virtual void testStatFileNotFound()
		{
			NUnit.Framework.Assume.assumeTrue(org.apache.hadoop.fs.Stat.isAvailable());
			try
			{
				stat.getFileStatus();
				NUnit.Framework.Assert.Fail("Expected FileNotFoundException");
			}
			catch (java.io.FileNotFoundException)
			{
			}
		}

		// expected
		/// <exception cref="System.Exception"/>
		public virtual void testStatEnvironment()
		{
			NUnit.Framework.Assert.AreEqual("C", stat.getEnvironment("LANG"));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testStat()
		{
			NUnit.Framework.Assume.assumeTrue(org.apache.hadoop.fs.Stat.isAvailable());
			org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(new 
				org.apache.hadoop.conf.Configuration());
			org.apache.hadoop.fs.Path testDir = new org.apache.hadoop.fs.Path(getTestRootPath
				(fs), "teststat");
			fs.mkdirs(testDir);
			org.apache.hadoop.fs.Path sub1 = new org.apache.hadoop.fs.Path(testDir, "sub1");
			org.apache.hadoop.fs.Path sub2 = new org.apache.hadoop.fs.Path(testDir, "sub2");
			fs.mkdirs(sub1);
			fs.createSymlink(sub1, sub2, false);
			org.apache.hadoop.fs.FileStatus stat1 = new org.apache.hadoop.fs.Stat(sub1, 4096l
				, false, fs).getFileStatus();
			org.apache.hadoop.fs.FileStatus stat2 = new org.apache.hadoop.fs.Stat(sub2, 0, false
				, fs).getFileStatus();
			NUnit.Framework.Assert.IsTrue(stat1.isDirectory());
			NUnit.Framework.Assert.IsFalse(stat2.isDirectory());
			fs.delete(testDir, true);
		}
	}
}
