using Sharpen;

namespace org.apache.hadoop.fs.shell
{
	public class TestPathData
	{
		private static readonly string TEST_ROOT_DIR = Sharpen.Runtime.getProperty("test.build.data"
			, "build/test/data") + "/testPD";

		protected internal org.apache.hadoop.conf.Configuration conf;

		protected internal org.apache.hadoop.fs.FileSystem fs;

		protected internal org.apache.hadoop.fs.Path testDir;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void initialize()
		{
			conf = new org.apache.hadoop.conf.Configuration();
			fs = org.apache.hadoop.fs.FileSystem.getLocal(conf);
			testDir = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR);
			// don't want scheme on the path, just an absolute path
			testDir = new org.apache.hadoop.fs.Path(fs.makeQualified(testDir).toUri().getPath
				());
			fs.mkdirs(testDir);
			org.apache.hadoop.fs.FileSystem.setDefaultUri(conf, fs.getUri());
			fs.setWorkingDirectory(testDir);
			fs.mkdirs(new org.apache.hadoop.fs.Path("d1"));
			fs.createNewFile(new org.apache.hadoop.fs.Path("d1", "f1"));
			fs.createNewFile(new org.apache.hadoop.fs.Path("d1", "f1.1"));
			fs.createNewFile(new org.apache.hadoop.fs.Path("d1", "f2"));
			fs.mkdirs(new org.apache.hadoop.fs.Path("d2"));
			fs.create(new org.apache.hadoop.fs.Path("d2", "f3"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void cleanup()
		{
			fs.delete(testDir, true);
			fs.close();
		}

		/// <exception cref="System.Exception"/>
		public virtual void testWithDirStringAndConf()
		{
			string dirString = "d1";
			org.apache.hadoop.fs.shell.PathData item = new org.apache.hadoop.fs.shell.PathData
				(dirString, conf);
			checkPathData(dirString, item);
			// properly implementing symlink support in various commands will require
			// trailing slashes to be retained
			dirString = "d1/";
			item = new org.apache.hadoop.fs.shell.PathData(dirString, conf);
			checkPathData(dirString, item);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testUnqualifiedUriContents()
		{
			string dirString = "d1";
			org.apache.hadoop.fs.shell.PathData item = new org.apache.hadoop.fs.shell.PathData
				(dirString, conf);
			org.apache.hadoop.fs.shell.PathData[] items = item.getDirectoryContents();
			NUnit.Framework.Assert.AreEqual(sortedString("d1/f1", "d1/f1.1", "d1/f2"), sortedString
				(items));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testQualifiedUriContents()
		{
			string dirString = fs.makeQualified(new org.apache.hadoop.fs.Path("d1")).ToString
				();
			org.apache.hadoop.fs.shell.PathData item = new org.apache.hadoop.fs.shell.PathData
				(dirString, conf);
			org.apache.hadoop.fs.shell.PathData[] items = item.getDirectoryContents();
			NUnit.Framework.Assert.AreEqual(sortedString(dirString + "/f1", dirString + "/f1.1"
				, dirString + "/f2"), sortedString(items));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testCwdContents()
		{
			string dirString = org.apache.hadoop.fs.Path.CUR_DIR;
			org.apache.hadoop.fs.shell.PathData item = new org.apache.hadoop.fs.shell.PathData
				(dirString, conf);
			org.apache.hadoop.fs.shell.PathData[] items = item.getDirectoryContents();
			NUnit.Framework.Assert.AreEqual(sortedString("d1", "d2"), sortedString(items));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testToFile()
		{
			org.apache.hadoop.fs.shell.PathData item = new org.apache.hadoop.fs.shell.PathData
				(".", conf);
			NUnit.Framework.Assert.AreEqual(new java.io.File(testDir.ToString()), item.toFile
				());
			item = new org.apache.hadoop.fs.shell.PathData("d1/f1", conf);
			NUnit.Framework.Assert.AreEqual(new java.io.File(testDir + "/d1/f1"), item.toFile
				());
			item = new org.apache.hadoop.fs.shell.PathData(testDir + "/d1/f1", conf);
			NUnit.Framework.Assert.AreEqual(new java.io.File(testDir + "/d1/f1"), item.toFile
				());
		}

		/// <exception cref="System.Exception"/>
		public virtual void testToFileRawWindowsPaths()
		{
			if (!org.apache.hadoop.fs.Path.WINDOWS)
			{
				return;
			}
			// Can we handle raw Windows paths? The files need not exist for
			// these tests to succeed.
			string[] winPaths = new string[] { "n:\\", "N:\\", "N:\\foo", "N:\\foo\\bar", "N:/"
				, "N:/foo", "N:/foo/bar" };
			org.apache.hadoop.fs.shell.PathData item;
			foreach (string path in winPaths)
			{
				item = new org.apache.hadoop.fs.shell.PathData(path, conf);
				NUnit.Framework.Assert.AreEqual(new java.io.File(path), item.toFile());
			}
			item = new org.apache.hadoop.fs.shell.PathData("foo\\bar", conf);
			NUnit.Framework.Assert.AreEqual(new java.io.File(testDir + "\\foo\\bar"), item.toFile
				());
		}

		/// <exception cref="System.Exception"/>
		public virtual void testInvalidWindowsPath()
		{
			if (!org.apache.hadoop.fs.Path.WINDOWS)
			{
				return;
			}
			// Verify that the following invalid paths are rejected.
			string[] winPaths = new string[] { "N:\\foo/bar" };
			foreach (string path in winPaths)
			{
				try
				{
					org.apache.hadoop.fs.shell.PathData item = new org.apache.hadoop.fs.shell.PathData
						(path, conf);
					NUnit.Framework.Assert.Fail("Did not throw for invalid path " + path);
				}
				catch (System.IO.IOException)
				{
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testAbsoluteGlob()
		{
			org.apache.hadoop.fs.shell.PathData[] items = org.apache.hadoop.fs.shell.PathData
				.expandAsGlob(testDir + "/d1/f1*", conf);
			NUnit.Framework.Assert.AreEqual(sortedString(testDir + "/d1/f1", testDir + "/d1/f1.1"
				), sortedString(items));
			string absolutePathNoDriveLetter = testDir + "/d1/f1";
			if (org.apache.hadoop.util.Shell.WINDOWS)
			{
				// testDir is an absolute path with a drive letter on Windows, i.e.
				// c:/some/path
				// and for the test we want something like the following
				// /some/path
				absolutePathNoDriveLetter = Sharpen.Runtime.substring(absolutePathNoDriveLetter, 
					2);
			}
			items = org.apache.hadoop.fs.shell.PathData.expandAsGlob(absolutePathNoDriveLetter
				, conf);
			NUnit.Framework.Assert.AreEqual(sortedString(absolutePathNoDriveLetter), sortedString
				(items));
			items = org.apache.hadoop.fs.shell.PathData.expandAsGlob(".", conf);
			NUnit.Framework.Assert.AreEqual(sortedString("."), sortedString(items));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testRelativeGlob()
		{
			org.apache.hadoop.fs.shell.PathData[] items = org.apache.hadoop.fs.shell.PathData
				.expandAsGlob("d1/f1*", conf);
			NUnit.Framework.Assert.AreEqual(sortedString("d1/f1", "d1/f1.1"), sortedString(items
				));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testRelativeGlobBack()
		{
			fs.setWorkingDirectory(new org.apache.hadoop.fs.Path("d1"));
			org.apache.hadoop.fs.shell.PathData[] items = org.apache.hadoop.fs.shell.PathData
				.expandAsGlob("../d2/*", conf);
			NUnit.Framework.Assert.AreEqual(sortedString("../d2/f3"), sortedString(items));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testWithStringAndConfForBuggyPath()
		{
			string dirString = "file:///tmp";
			org.apache.hadoop.fs.Path tmpDir = new org.apache.hadoop.fs.Path(dirString);
			org.apache.hadoop.fs.shell.PathData item = new org.apache.hadoop.fs.shell.PathData
				(dirString, conf);
			// this may fail some day if Path is fixed to not crunch the uri
			// if the authority is null, however we need to test that the PathData
			// toString() returns the given string, while Path toString() does
			// the crunching
			NUnit.Framework.Assert.AreEqual("file:/tmp", tmpDir.ToString());
			checkPathData(dirString, item);
		}

		/// <exception cref="System.Exception"/>
		public virtual void checkPathData(string dirString, org.apache.hadoop.fs.shell.PathData
			 item)
		{
			NUnit.Framework.Assert.AreEqual("checking fs", fs, item.fs);
			NUnit.Framework.Assert.AreEqual("checking string", dirString, item.ToString());
			NUnit.Framework.Assert.AreEqual("checking path", fs.makeQualified(new org.apache.hadoop.fs.Path
				(item.ToString())), item.path);
			NUnit.Framework.Assert.IsTrue("checking exist", item.stat != null);
			NUnit.Framework.Assert.IsTrue("checking isDir", item.stat.isDirectory());
		}

		/* junit does a lousy job of comparing arrays
		* if the array lengths differ, it just says that w/o showing contents
		* this sorts the paths, and builds a string of "i:<value>, ..." suitable
		* for a string compare
		*/
		private static string sortedString(params object[] list)
		{
			string[] strings = new string[list.Length];
			for (int i = 0; i < list.Length; i++)
			{
				strings[i] = Sharpen.Runtime.getStringValueOf(list[i]);
			}
			java.util.Arrays.sort(strings);
			java.lang.StringBuilder result = new java.lang.StringBuilder();
			for (int i_1 = 0; i_1 < strings.Length; i_1++)
			{
				if (result.Length > 0)
				{
					result.Append(", ");
				}
				result.Append(i_1 + ":<" + strings[i_1] + ">");
			}
			return result.ToString();
		}

		private static string sortedString(params org.apache.hadoop.fs.shell.PathData[] items
			)
		{
			return sortedString((object[])items);
		}
	}
}
