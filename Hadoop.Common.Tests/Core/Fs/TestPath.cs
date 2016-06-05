using System;
using Com.Google.Common.Base;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.FS
{
	public class TestPath : TestCase
	{
		/// <summary>
		/// Merge a bunch of Path objects into a sorted semicolon-separated
		/// path string.
		/// </summary>
		public static string MergeStatuses(Path[] paths)
		{
			string[] pathStrings = new string[paths.Length];
			int i = 0;
			foreach (Path path in paths)
			{
				pathStrings[i++] = path.ToUri().GetPath();
			}
			Arrays.Sort(pathStrings);
			return Joiner.On(";").Join(pathStrings);
		}

		/// <summary>
		/// Merge a bunch of FileStatus objects into a sorted semicolon-separated
		/// path string.
		/// </summary>
		public static string MergeStatuses(FileStatus[] statuses)
		{
			Path[] paths = new Path[statuses.Length];
			int i = 0;
			foreach (FileStatus status in statuses)
			{
				paths[i++] = status.GetPath();
			}
			return MergeStatuses(paths);
		}

		public virtual void TestToString()
		{
			ToStringTest("/");
			ToStringTest("/foo");
			ToStringTest("/foo/bar");
			ToStringTest("foo");
			ToStringTest("foo/bar");
			ToStringTest("/foo/bar#boo");
			ToStringTest("foo/bar#boo");
			bool emptyException = false;
			try
			{
				ToStringTest(string.Empty);
			}
			catch (ArgumentException)
			{
				// expect to receive an IllegalArgumentException
				emptyException = true;
			}
			Assert.True(emptyException);
			if (Path.Windows)
			{
				ToStringTest("c:");
				ToStringTest("c:/");
				ToStringTest("c:foo");
				ToStringTest("c:foo/bar");
				ToStringTest("c:foo/bar");
				ToStringTest("c:/foo/bar");
				ToStringTest("C:/foo/bar#boo");
				ToStringTest("C:foo/bar#boo");
			}
		}

		private void ToStringTest(string pathString)
		{
			Assert.Equal(pathString, new Path(pathString).ToString());
		}

		/// <exception cref="URISyntaxException"/>
		public virtual void TestNormalize()
		{
			Assert.Equal(string.Empty, new Path(".").ToString());
			Assert.Equal("..", new Path("..").ToString());
			Assert.Equal("/", new Path("/").ToString());
			Assert.Equal("/", new Path("//").ToString());
			Assert.Equal("/", new Path("///").ToString());
			Assert.Equal("//foo/", new Path("//foo/").ToString());
			Assert.Equal("//foo/", new Path("//foo//").ToString());
			Assert.Equal("//foo/bar", new Path("//foo//bar").ToString());
			Assert.Equal("/foo", new Path("/foo/").ToString());
			Assert.Equal("/foo", new Path("/foo/").ToString());
			Assert.Equal("foo", new Path("foo/").ToString());
			Assert.Equal("foo", new Path("foo//").ToString());
			Assert.Equal("foo/bar", new Path("foo//bar").ToString());
			Assert.Equal("hdfs://foo/foo2/bar/baz/", new Path(new URI("hdfs://foo//foo2///bar/baz///"
				)).ToString());
			if (Path.Windows)
			{
				Assert.Equal("c:/a/b", new Path("c:\\a\\b").ToString());
			}
		}

		public virtual void TestIsAbsolute()
		{
			Assert.True(new Path("/").IsAbsolute());
			Assert.True(new Path("/foo").IsAbsolute());
			NUnit.Framework.Assert.IsFalse(new Path("foo").IsAbsolute());
			NUnit.Framework.Assert.IsFalse(new Path("foo/bar").IsAbsolute());
			NUnit.Framework.Assert.IsFalse(new Path(".").IsAbsolute());
			if (Path.Windows)
			{
				Assert.True(new Path("c:/a/b").IsAbsolute());
				NUnit.Framework.Assert.IsFalse(new Path("c:a/b").IsAbsolute());
			}
		}

		public virtual void TestParent()
		{
			Assert.Equal(new Path("/foo"), new Path("/foo/bar").GetParent(
				));
			Assert.Equal(new Path("foo"), new Path("foo/bar").GetParent());
			Assert.Equal(new Path("/"), new Path("/foo").GetParent());
			Assert.Equal(null, new Path("/").GetParent());
			if (Path.Windows)
			{
				Assert.Equal(new Path("c:/"), new Path("c:/foo").GetParent());
			}
		}

		public virtual void TestChild()
		{
			Assert.Equal(new Path("."), new Path(".", "."));
			Assert.Equal(new Path("/"), new Path("/", "."));
			Assert.Equal(new Path("/"), new Path(".", "/"));
			Assert.Equal(new Path("/foo"), new Path("/", "foo"));
			Assert.Equal(new Path("/foo/bar"), new Path("/foo", "bar"));
			Assert.Equal(new Path("/foo/bar/baz"), new Path("/foo/bar", "baz"
				));
			Assert.Equal(new Path("/foo/bar/baz"), new Path("/foo", "bar/baz"
				));
			Assert.Equal(new Path("foo"), new Path(".", "foo"));
			Assert.Equal(new Path("foo/bar"), new Path("foo", "bar"));
			Assert.Equal(new Path("foo/bar/baz"), new Path("foo", "bar/baz"
				));
			Assert.Equal(new Path("foo/bar/baz"), new Path("foo/bar", "baz"
				));
			Assert.Equal(new Path("/foo"), new Path("/bar", "/foo"));
			if (Path.Windows)
			{
				Assert.Equal(new Path("c:/foo"), new Path("/bar", "c:/foo"));
				Assert.Equal(new Path("c:/foo"), new Path("d:/bar", "c:/foo"));
			}
		}

		public virtual void TestPathThreeArgContructor()
		{
			Assert.Equal(new Path("foo"), new Path(null, null, "foo"));
			Assert.Equal(new Path("scheme:///foo"), new Path("scheme", null
				, "/foo"));
			Assert.Equal(new Path("scheme://authority/foo"), new Path("scheme"
				, "authority", "/foo"));
			if (Path.Windows)
			{
				Assert.Equal(new Path("c:/foo/bar"), new Path(null, null, "c:/foo/bar"
					));
				Assert.Equal(new Path("c:/foo/bar"), new Path(null, null, "/c:/foo/bar"
					));
			}
			else
			{
				Assert.Equal(new Path("./a:b"), new Path(null, null, "a:b"));
			}
			// Resolution tests
			if (Path.Windows)
			{
				Assert.Equal(new Path("c:/foo/bar"), new Path("/fou", new Path
					(null, null, "c:/foo/bar")));
				Assert.Equal(new Path("c:/foo/bar"), new Path("/fou", new Path
					(null, null, "/c:/foo/bar")));
				Assert.Equal(new Path("/foo/bar"), new Path("/foo", new Path(null
					, null, "bar")));
			}
			else
			{
				Assert.Equal(new Path("/foo/bar/a:b"), new Path("/foo/bar", new 
					Path(null, null, "a:b")));
				Assert.Equal(new Path("/a:b"), new Path("/foo/bar", new Path(null
					, null, "/a:b")));
			}
		}

		public virtual void TestEquals()
		{
			NUnit.Framework.Assert.IsFalse(new Path("/").Equals(new Path("/foo")));
		}

		public virtual void TestDots()
		{
			// Test Path(String) 
			Assert.Equal(new Path("/foo/bar/baz").ToString(), "/foo/bar/baz"
				);
			Assert.Equal(new Path("/foo/bar", ".").ToString(), "/foo/bar");
			Assert.Equal(new Path("/foo/bar/../baz").ToString(), "/foo/baz"
				);
			Assert.Equal(new Path("/foo/bar/./baz").ToString(), "/foo/bar/baz"
				);
			Assert.Equal(new Path("/foo/bar/baz/../../fud").ToString(), "/foo/fud"
				);
			Assert.Equal(new Path("/foo/bar/baz/.././../fud").ToString(), 
				"/foo/fud");
			Assert.Equal(new Path("../../foo/bar").ToString(), "../../foo/bar"
				);
			Assert.Equal(new Path(".././../foo/bar").ToString(), "../../foo/bar"
				);
			Assert.Equal(new Path("./foo/bar/baz").ToString(), "foo/bar/baz"
				);
			Assert.Equal(new Path("/foo/bar/../../baz/boo").ToString(), "/baz/boo"
				);
			Assert.Equal(new Path("foo/bar/").ToString(), "foo/bar");
			Assert.Equal(new Path("foo/bar/../baz").ToString(), "foo/baz");
			Assert.Equal(new Path("foo/bar/../../baz/boo").ToString(), "baz/boo"
				);
			// Test Path(Path,Path)
			Assert.Equal(new Path("/foo/bar", "baz/boo").ToString(), "/foo/bar/baz/boo"
				);
			Assert.Equal(new Path("foo/bar/", "baz/bud").ToString(), "foo/bar/baz/bud"
				);
			Assert.Equal(new Path("/foo/bar", "../../boo/bud").ToString(), 
				"/boo/bud");
			Assert.Equal(new Path("foo/bar", "../../boo/bud").ToString(), 
				"boo/bud");
			Assert.Equal(new Path(".", "boo/bud").ToString(), "boo/bud");
			Assert.Equal(new Path("/foo/bar/baz", "../../boo/bud").ToString
				(), "/foo/boo/bud");
			Assert.Equal(new Path("foo/bar/baz", "../../boo/bud").ToString
				(), "foo/boo/bud");
			Assert.Equal(new Path("../../", "../../boo/bud").ToString(), "../../../../boo/bud"
				);
			Assert.Equal(new Path("../../foo", "../../../boo/bud").ToString
				(), "../../../../boo/bud");
			Assert.Equal(new Path("../../foo/bar", "../boo/bud").ToString(
				), "../../foo/boo/bud");
			Assert.Equal(new Path("foo/bar/baz", "../../..").ToString(), string.Empty
				);
			Assert.Equal(new Path("foo/bar/baz", "../../../../..").ToString
				(), "../..");
		}

		/// <summary>Test that Windows paths are correctly handled</summary>
		/// <exception cref="URISyntaxException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestWindowsPaths()
		{
			if (!Path.Windows)
			{
				return;
			}
			Assert.Equal(new Path("c:\\foo\\bar").ToString(), "c:/foo/bar"
				);
			Assert.Equal(new Path("c:/foo/bar").ToString(), "c:/foo/bar");
			Assert.Equal(new Path("/c:/foo/bar").ToString(), "c:/foo/bar");
			Assert.Equal(new Path("file://c:/foo/bar").ToString(), "file://c:/foo/bar"
				);
		}

		/// <summary>Test invalid paths on Windows are correctly rejected</summary>
		/// <exception cref="URISyntaxException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInvalidWindowsPaths()
		{
			if (!Path.Windows)
			{
				return;
			}
			string[] invalidPaths = new string[] { "hdfs:\\\\\\tmp" };
			foreach (string path in invalidPaths)
			{
				try
				{
					Path item = new Path(path);
					Fail("Did not throw for invalid path " + path);
				}
				catch (ArgumentException)
				{
				}
			}
		}

		/// <summary>Test Path objects created from other Path objects</summary>
		/// <exception cref="URISyntaxException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestChildParentResolution()
		{
			Path parent = new Path("foo1://bar1/baz1");
			Path child = new Path("foo2://bar2/baz2");
			Assert.Equal(child, new Path(parent, child));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestScheme()
		{
			Assert.Equal("foo:/bar", new Path("foo:/", "/bar").ToString());
			Assert.Equal("foo://bar/baz", new Path("foo://bar/", "/baz").ToString
				());
		}

		/// <exception cref="URISyntaxException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestURI()
		{
			URI uri = new URI("file:///bar#baz");
			Path path = new Path(uri);
			Assert.True(uri.Equals(new URI(path.ToString())));
			FileSystem fs = path.GetFileSystem(new Configuration());
			Assert.True(uri.Equals(new URI(fs.MakeQualified(path).ToString(
				))));
			// uri without hash
			URI uri2 = new URI("file:///bar/baz");
			Assert.True(uri2.Equals(new URI(fs.MakeQualified(new Path(uri2)
				).ToString())));
			Assert.Equal("foo://bar/baz#boo", new Path("foo://bar/", new Path
				(new URI("/baz#boo"))).ToString());
			Assert.Equal("foo://bar/baz/fud#boo", new Path(new Path(new URI
				("foo://bar/baz#bud")), new Path(new URI("fud#boo"))).ToString());
			// if the child uri is absolute path
			Assert.Equal("foo://bar/fud#boo", new Path(new Path(new URI("foo://bar/baz#bud"
				)), new Path(new URI("/fud#boo"))).ToString());
		}

		/// <summary>Test URIs created from Path objects</summary>
		/// <exception cref="URISyntaxException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestPathToUriConversion()
		{
			// Path differs from URI in that it ignores the query part..
			Assert.Equal("? mark char in to URI", new URI(null, null, "/foo?bar"
				, null, null), new Path("/foo?bar").ToUri());
			Assert.Equal("escape slashes chars in to URI", new URI(null, null
				, "/foo\"bar", null, null), new Path("/foo\"bar").ToUri());
			Assert.Equal("spaces in chars to URI", new URI(null, null, "/foo bar"
				, null, null), new Path("/foo bar").ToUri());
			// therefore "foo?bar" is a valid Path, so a URI created from a Path
			// has path "foo?bar" where in a straight URI the path part is just "foo"
			Assert.Equal("/foo?bar", new Path("http://localhost/foo?bar").
				ToUri().GetPath());
			Assert.Equal("/foo", new URI("http://localhost/foo?bar").GetPath
				());
			// The path part handling in Path is equivalent to URI
			Assert.Equal(new URI("/foo;bar").GetPath(), new Path("/foo;bar"
				).ToUri().GetPath());
			Assert.Equal(new URI("/foo;bar"), new Path("/foo;bar").ToUri()
				);
			Assert.Equal(new URI("/foo+bar"), new Path("/foo+bar").ToUri()
				);
			Assert.Equal(new URI("/foo-bar"), new Path("/foo-bar").ToUri()
				);
			Assert.Equal(new URI("/foo=bar"), new Path("/foo=bar").ToUri()
				);
			Assert.Equal(new URI("/foo,bar"), new Path("/foo,bar").ToUri()
				);
		}

		/// <summary>Test reserved characters in URIs (and therefore Paths)</summary>
		/// <exception cref="URISyntaxException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestReservedCharacters()
		{
			// URI encodes the path
			Assert.Equal("/foo%20bar", new URI(null, null, "/foo bar", null
				, null).GetRawPath());
			// URI#getPath decodes the path
			Assert.Equal("/foo bar", new URI(null, null, "/foo bar", null, 
				null).GetPath());
			// URI#toString returns an encoded path
			Assert.Equal("/foo%20bar", new URI(null, null, "/foo bar", null
				, null).ToString());
			Assert.Equal("/foo%20bar", new Path("/foo bar").ToUri().ToString
				());
			// Reserved chars are not encoded
			Assert.Equal("/foo;bar", new URI("/foo;bar").GetPath());
			Assert.Equal("/foo;bar", new URI("/foo;bar").GetRawPath());
			Assert.Equal("/foo+bar", new URI("/foo+bar").GetPath());
			Assert.Equal("/foo+bar", new URI("/foo+bar").GetRawPath());
			// URI#getPath decodes the path part (and URL#getPath does not decode)
			Assert.Equal("/foo bar", new Path("http://localhost/foo bar").
				ToUri().GetPath());
			Assert.Equal("/foo%20bar", new Path("http://localhost/foo bar"
				).ToUri().ToURL().AbsolutePath);
			Assert.Equal("/foo?bar", new URI("http", "localhost", "/foo?bar"
				, null, null).GetPath());
			Assert.Equal("/foo%3Fbar", new URI("http", "localhost", "/foo?bar"
				, null, null).ToURL().AbsolutePath);
		}

		/// <exception cref="URISyntaxException"/>
		public virtual void TestMakeQualified()
		{
			URI defaultUri = new URI("hdfs://host1/dir1");
			URI wd = new URI("hdfs://host2/dir2");
			// The scheme from defaultUri is used but the path part is not
			Assert.Equal(new Path("hdfs://host1/dir/file"), new Path("file"
				).MakeQualified(defaultUri, new Path("/dir")));
			// The defaultUri is only used if the path + wd has no scheme    
			Assert.Equal(new Path("hdfs://host2/dir2/file"), new Path("file"
				).MakeQualified(defaultUri, new Path(wd)));
		}

		public virtual void TestGetName()
		{
			Assert.Equal(string.Empty, new Path("/").GetName());
			Assert.Equal("foo", new Path("foo").GetName());
			Assert.Equal("foo", new Path("/foo").GetName());
			Assert.Equal("foo", new Path("/foo/").GetName());
			Assert.Equal("bar", new Path("/foo/bar").GetName());
			Assert.Equal("bar", new Path("hdfs://host/foo/bar").GetName());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAvroReflect()
		{
			AvroTestUtil.TestReflect(new Path("foo"), "{\"type\":\"string\",\"java-class\":\"org.apache.hadoop.fs.Path\"}"
				);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGlobEscapeStatus()
		{
			// This test is not meaningful on Windows where * is disallowed in file name.
			if (Shell.Windows)
			{
				return;
			}
			FileSystem lfs = FileSystem.GetLocal(new Configuration());
			Path testRoot = lfs.MakeQualified(new Path(Runtime.GetProperty("test.build.data", 
				"test/build/data"), "testPathGlob"));
			lfs.Delete(testRoot, true);
			lfs.Mkdirs(testRoot);
			Assert.True(lfs.IsDirectory(testRoot));
			lfs.SetWorkingDirectory(testRoot);
			// create a couple dirs with file in them
			Path[] paths = new Path[] { new Path(testRoot, "*/f"), new Path(testRoot, "d1/f")
				, new Path(testRoot, "d2/f") };
			Arrays.Sort(paths);
			foreach (Path p in paths)
			{
				lfs.Create(p).Close();
				Assert.True(lfs.Exists(p));
			}
			// try the non-globbed listStatus
			FileStatus[] stats = lfs.ListStatus(new Path(testRoot, "*"));
			Assert.Equal(1, stats.Length);
			Assert.Equal(new Path(testRoot, "*/f"), stats[0].GetPath());
			// ensure globStatus with "*" finds all dir contents
			stats = lfs.GlobStatus(new Path(testRoot, "*"));
			Arrays.Sort(stats);
			Path[] parentPaths = new Path[paths.Length];
			for (int i = 0; i < paths.Length; i++)
			{
				parentPaths[i] = paths[i].GetParent();
			}
			Assert.Equal(MergeStatuses(parentPaths), MergeStatuses(stats));
			// ensure that globStatus with an escaped "\*" only finds "*"
			stats = lfs.GlobStatus(new Path(testRoot, "\\*"));
			Assert.Equal(1, stats.Length);
			Assert.Equal(new Path(testRoot, "*"), stats[0].GetPath());
			// try to glob the inner file for all dirs
			stats = lfs.GlobStatus(new Path(testRoot, "*/f"));
			Assert.Equal(paths.Length, stats.Length);
			Assert.Equal(MergeStatuses(paths), MergeStatuses(stats));
			// try to get the inner file for only the "*" dir
			stats = lfs.GlobStatus(new Path(testRoot, "\\*/f"));
			Assert.Equal(1, stats.Length);
			Assert.Equal(new Path(testRoot, "*/f"), stats[0].GetPath());
			// try to glob all the contents of the "*" dir
			stats = lfs.GlobStatus(new Path(testRoot, "\\*/*"));
			Assert.Equal(1, stats.Length);
			Assert.Equal(new Path(testRoot, "*/f"), stats[0].GetPath());
		}

		public virtual void TestMergePaths()
		{
			Assert.Equal(new Path("/foo/bar"), Path.MergePaths(new Path("/foo"
				), new Path("/bar")));
			Assert.Equal(new Path("/foo/bar/baz"), Path.MergePaths(new Path
				("/foo/bar"), new Path("/baz")));
			Assert.Equal(new Path("/foo/bar/baz"), Path.MergePaths(new Path
				("/foo"), new Path("/bar/baz")));
			Assert.Equal(new Path(Shell.Windows ? "/C:/foo/bar" : "/C:/foo/C:/bar"
				), Path.MergePaths(new Path("/C:/foo"), new Path("/C:/bar")));
			Assert.Equal(new Path(Shell.Windows ? "/C:/bar" : "/C:/C:/bar"
				), Path.MergePaths(new Path("/C:/"), new Path("/C:/bar")));
			Assert.Equal(new Path("/bar"), Path.MergePaths(new Path("/"), 
				new Path("/bar")));
			Assert.Equal(new Path("viewfs:///foo/bar"), Path.MergePaths(new 
				Path("viewfs:///foo"), new Path("file:///bar")));
			Assert.Equal(new Path("viewfs://vfsauthority/foo/bar"), Path.MergePaths
				(new Path("viewfs://vfsauthority/foo"), new Path("file://fileauthority/bar")));
		}

		public virtual void TestIsWindowsAbsolutePath()
		{
			if (!Shell.Windows)
			{
				return;
			}
			Assert.True(Path.IsWindowsAbsolutePath("C:\\test", false));
			Assert.True(Path.IsWindowsAbsolutePath("C:/test", false));
			Assert.True(Path.IsWindowsAbsolutePath("/C:/test", true));
			NUnit.Framework.Assert.IsFalse(Path.IsWindowsAbsolutePath("/test", false));
			NUnit.Framework.Assert.IsFalse(Path.IsWindowsAbsolutePath("/test", true));
			NUnit.Framework.Assert.IsFalse(Path.IsWindowsAbsolutePath("C:test", false));
			NUnit.Framework.Assert.IsFalse(Path.IsWindowsAbsolutePath("/C:test", true));
		}
	}
}
