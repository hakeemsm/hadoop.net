using System.IO;
using System.Text;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell
{
	public class TestPathData
	{
		private static readonly string TestRootDir = Runtime.GetProperty("test.build.data"
			, "build/test/data") + "/testPD";

		protected internal Configuration conf;

		protected internal FileSystem fs;

		protected internal Path testDir;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Initialize()
		{
			conf = new Configuration();
			fs = FileSystem.GetLocal(conf);
			testDir = new Path(TestRootDir);
			// don't want scheme on the path, just an absolute path
			testDir = new Path(fs.MakeQualified(testDir).ToUri().GetPath());
			fs.Mkdirs(testDir);
			FileSystem.SetDefaultUri(conf, fs.GetUri());
			fs.SetWorkingDirectory(testDir);
			fs.Mkdirs(new Path("d1"));
			fs.CreateNewFile(new Path("d1", "f1"));
			fs.CreateNewFile(new Path("d1", "f1.1"));
			fs.CreateNewFile(new Path("d1", "f2"));
			fs.Mkdirs(new Path("d2"));
			fs.Create(new Path("d2", "f3"));
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void Cleanup()
		{
			fs.Delete(testDir, true);
			fs.Close();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestWithDirStringAndConf()
		{
			string dirString = "d1";
			PathData item = new PathData(dirString, conf);
			CheckPathData(dirString, item);
			// properly implementing symlink support in various commands will require
			// trailing slashes to be retained
			dirString = "d1/";
			item = new PathData(dirString, conf);
			CheckPathData(dirString, item);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestUnqualifiedUriContents()
		{
			string dirString = "d1";
			PathData item = new PathData(dirString, conf);
			PathData[] items = item.GetDirectoryContents();
			Assert.Equal(SortedString("d1/f1", "d1/f1.1", "d1/f2"), SortedString
				(items));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestQualifiedUriContents()
		{
			string dirString = fs.MakeQualified(new Path("d1")).ToString();
			PathData item = new PathData(dirString, conf);
			PathData[] items = item.GetDirectoryContents();
			Assert.Equal(SortedString(dirString + "/f1", dirString + "/f1.1"
				, dirString + "/f2"), SortedString(items));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCwdContents()
		{
			string dirString = Path.CurDir;
			PathData item = new PathData(dirString, conf);
			PathData[] items = item.GetDirectoryContents();
			Assert.Equal(SortedString("d1", "d2"), SortedString(items));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestToFile()
		{
			PathData item = new PathData(".", conf);
			Assert.Equal(new FilePath(testDir.ToString()), item.ToFile());
			item = new PathData("d1/f1", conf);
			Assert.Equal(new FilePath(testDir + "/d1/f1"), item.ToFile());
			item = new PathData(testDir + "/d1/f1", conf);
			Assert.Equal(new FilePath(testDir + "/d1/f1"), item.ToFile());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestToFileRawWindowsPaths()
		{
			if (!Path.Windows)
			{
				return;
			}
			// Can we handle raw Windows paths? The files need not exist for
			// these tests to succeed.
			string[] winPaths = new string[] { "n:\\", "N:\\", "N:\\foo", "N:\\foo\\bar", "N:/"
				, "N:/foo", "N:/foo/bar" };
			PathData item;
			foreach (string path in winPaths)
			{
				item = new PathData(path, conf);
				Assert.Equal(new FilePath(path), item.ToFile());
			}
			item = new PathData("foo\\bar", conf);
			Assert.Equal(new FilePath(testDir + "\\foo\\bar"), item.ToFile
				());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestInvalidWindowsPath()
		{
			if (!Path.Windows)
			{
				return;
			}
			// Verify that the following invalid paths are rejected.
			string[] winPaths = new string[] { "N:\\foo/bar" };
			foreach (string path in winPaths)
			{
				try
				{
					PathData item = new PathData(path, conf);
					NUnit.Framework.Assert.Fail("Did not throw for invalid path " + path);
				}
				catch (IOException)
				{
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAbsoluteGlob()
		{
			PathData[] items = PathData.ExpandAsGlob(testDir + "/d1/f1*", conf);
			Assert.Equal(SortedString(testDir + "/d1/f1", testDir + "/d1/f1.1"
				), SortedString(items));
			string absolutePathNoDriveLetter = testDir + "/d1/f1";
			if (Org.Apache.Hadoop.Util.Shell.Windows)
			{
				// testDir is an absolute path with a drive letter on Windows, i.e.
				// c:/some/path
				// and for the test we want something like the following
				// /some/path
				absolutePathNoDriveLetter = Sharpen.Runtime.Substring(absolutePathNoDriveLetter, 
					2);
			}
			items = PathData.ExpandAsGlob(absolutePathNoDriveLetter, conf);
			Assert.Equal(SortedString(absolutePathNoDriveLetter), SortedString
				(items));
			items = PathData.ExpandAsGlob(".", conf);
			Assert.Equal(SortedString("."), SortedString(items));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRelativeGlob()
		{
			PathData[] items = PathData.ExpandAsGlob("d1/f1*", conf);
			Assert.Equal(SortedString("d1/f1", "d1/f1.1"), SortedString(items
				));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRelativeGlobBack()
		{
			fs.SetWorkingDirectory(new Path("d1"));
			PathData[] items = PathData.ExpandAsGlob("../d2/*", conf);
			Assert.Equal(SortedString("../d2/f3"), SortedString(items));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestWithStringAndConfForBuggyPath()
		{
			string dirString = "file:///tmp";
			Path tmpDir = new Path(dirString);
			PathData item = new PathData(dirString, conf);
			// this may fail some day if Path is fixed to not crunch the uri
			// if the authority is null, however we need to test that the PathData
			// toString() returns the given string, while Path toString() does
			// the crunching
			Assert.Equal("file:/tmp", tmpDir.ToString());
			CheckPathData(dirString, item);
		}

		/// <exception cref="System.Exception"/>
		public virtual void CheckPathData(string dirString, PathData item)
		{
			Assert.Equal("checking fs", fs, item.fs);
			Assert.Equal("checking string", dirString, item.ToString());
			Assert.Equal("checking path", fs.MakeQualified(new Path(item.ToString
				())), item.path);
			Assert.True("checking exist", item.stat != null);
			Assert.True("checking isDir", item.stat.IsDirectory());
		}

		/* junit does a lousy job of comparing arrays
		* if the array lengths differ, it just says that w/o showing contents
		* this sorts the paths, and builds a string of "i:<value>, ..." suitable
		* for a string compare
		*/
		private static string SortedString(params object[] list)
		{
			string[] strings = new string[list.Length];
			for (int i = 0; i < list.Length; i++)
			{
				strings[i] = list[i].ToString();
			}
			Arrays.Sort(strings);
			StringBuilder result = new StringBuilder();
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

		private static string SortedString(params PathData[] items)
		{
			return SortedString((object[])items);
		}
	}
}
