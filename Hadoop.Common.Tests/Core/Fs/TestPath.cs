using System;
using Com.Google.Common.Base;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

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
			NUnit.Framework.Assert.IsTrue(emptyException);
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
			NUnit.Framework.Assert.AreEqual(pathString, new Path(pathString).ToString());
		}

		/// <exception cref="Sharpen.URISyntaxException"/>
		public virtual void TestNormalize()
		{
			NUnit.Framework.Assert.AreEqual(string.Empty, new Path(".").ToString());
			NUnit.Framework.Assert.AreEqual("..", new Path("..").ToString());
			NUnit.Framework.Assert.AreEqual("/", new Path("/").ToString());
			NUnit.Framework.Assert.AreEqual("/", new Path("//").ToString());
			NUnit.Framework.Assert.AreEqual("/", new Path("///").ToString());
			NUnit.Framework.Assert.AreEqual("//foo/", new Path("//foo/").ToString());
			NUnit.Framework.Assert.AreEqual("//foo/", new Path("//foo//").ToString());
			NUnit.Framework.Assert.AreEqual("//foo/bar", new Path("//foo//bar").ToString());
			NUnit.Framework.Assert.AreEqual("/foo", new Path("/foo/").ToString());
			NUnit.Framework.Assert.AreEqual("/foo", new Path("/foo/").ToString());
			NUnit.Framework.Assert.AreEqual("foo", new Path("foo/").ToString());
			NUnit.Framework.Assert.AreEqual("foo", new Path("foo//").ToString());
			NUnit.Framework.Assert.AreEqual("foo/bar", new Path("foo//bar").ToString());
			NUnit.Framework.Assert.AreEqual("hdfs://foo/foo2/bar/baz/", new Path(new URI("hdfs://foo//foo2///bar/baz///"
				)).ToString());
			if (Path.Windows)
			{
				NUnit.Framework.Assert.AreEqual("c:/a/b", new Path("c:\\a\\b").ToString());
			}
		}

		public virtual void TestIsAbsolute()
		{
			NUnit.Framework.Assert.IsTrue(new Path("/").IsAbsolute());
			NUnit.Framework.Assert.IsTrue(new Path("/foo").IsAbsolute());
			NUnit.Framework.Assert.IsFalse(new Path("foo").IsAbsolute());
			NUnit.Framework.Assert.IsFalse(new Path("foo/bar").IsAbsolute());
			NUnit.Framework.Assert.IsFalse(new Path(".").IsAbsolute());
			if (Path.Windows)
			{
				NUnit.Framework.Assert.IsTrue(new Path("c:/a/b").IsAbsolute());
				NUnit.Framework.Assert.IsFalse(new Path("c:a/b").IsAbsolute());
			}
		}

		public virtual void TestParent()
		{
			NUnit.Framework.Assert.AreEqual(new Path("/foo"), new Path("/foo/bar").GetParent(
				));
			NUnit.Framework.Assert.AreEqual(new Path("foo"), new Path("foo/bar").GetParent());
			NUnit.Framework.Assert.AreEqual(new Path("/"), new Path("/foo").GetParent());
			NUnit.Framework.Assert.AreEqual(null, new Path("/").GetParent());
			if (Path.Windows)
			{
				NUnit.Framework.Assert.AreEqual(new Path("c:/"), new Path("c:/foo").GetParent());
			}
		}

		public virtual void TestChild()
		{
			NUnit.Framework.Assert.AreEqual(new Path("."), new Path(".", "."));
			NUnit.Framework.Assert.AreEqual(new Path("/"), new Path("/", "."));
			NUnit.Framework.Assert.AreEqual(new Path("/"), new Path(".", "/"));
			NUnit.Framework.Assert.AreEqual(new Path("/foo"), new Path("/", "foo"));
			NUnit.Framework.Assert.AreEqual(new Path("/foo/bar"), new Path("/foo", "bar"));
			NUnit.Framework.Assert.AreEqual(new Path("/foo/bar/baz"), new Path("/foo/bar", "baz"
				));
			NUnit.Framework.Assert.AreEqual(new Path("/foo/bar/baz"), new Path("/foo", "bar/baz"
				));
			NUnit.Framework.Assert.AreEqual(new Path("foo"), new Path(".", "foo"));
			NUnit.Framework.Assert.AreEqual(new Path("foo/bar"), new Path("foo", "bar"));
			NUnit.Framework.Assert.AreEqual(new Path("foo/bar/baz"), new Path("foo", "bar/baz"
				));
			NUnit.Framework.Assert.AreEqual(new Path("foo/bar/baz"), new Path("foo/bar", "baz"
				));
			NUnit.Framework.Assert.AreEqual(new Path("/foo"), new Path("/bar", "/foo"));
			if (Path.Windows)
			{
				NUnit.Framework.Assert.AreEqual(new Path("c:/foo"), new Path("/bar", "c:/foo"));
				NUnit.Framework.Assert.AreEqual(new Path("c:/foo"), new Path("d:/bar", "c:/foo"));
			}
		}

		public virtual void TestPathThreeArgContructor()
		{
			NUnit.Framework.Assert.AreEqual(new Path("foo"), new Path(null, null, "foo"));
			NUnit.Framework.Assert.AreEqual(new Path("scheme:///foo"), new Path("scheme", null
				, "/foo"));
			NUnit.Framework.Assert.AreEqual(new Path("scheme://authority/foo"), new Path("scheme"
				, "authority", "/foo"));
			if (Path.Windows)
			{
				NUnit.Framework.Assert.AreEqual(new Path("c:/foo/bar"), new Path(null, null, "c:/foo/bar"
					));
				NUnit.Framework.Assert.AreEqual(new Path("c:/foo/bar"), new Path(null, null, "/c:/foo/bar"
					));
			}
			else
			{
				NUnit.Framework.Assert.AreEqual(new Path("./a:b"), new Path(null, null, "a:b"));
			}
			// Resolution tests
			if (Path.Windows)
			{
				NUnit.Framework.Assert.AreEqual(new Path("c:/foo/bar"), new Path("/fou", new Path
					(null, null, "c:/foo/bar")));
				NUnit.Framework.Assert.AreEqual(new Path("c:/foo/bar"), new Path("/fou", new Path
					(null, null, "/c:/foo/bar")));
				NUnit.Framework.Assert.AreEqual(new Path("/foo/bar"), new Path("/foo", new Path(null
					, null, "bar")));
			}
			else
			{
				NUnit.Framework.Assert.AreEqual(new Path("/foo/bar/a:b"), new Path("/foo/bar", new 
					Path(null, null, "a:b")));
				NUnit.Framework.Assert.AreEqual(new Path("/a:b"), new Path("/foo/bar", new Path(null
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
			NUnit.Framework.Assert.AreEqual(new Path("/foo/bar/baz").ToString(), "/foo/bar/baz"
				);
			NUnit.Framework.Assert.AreEqual(new Path("/foo/bar", ".").ToString(), "/foo/bar");
			NUnit.Framework.Assert.AreEqual(new Path("/foo/bar/../baz").ToString(), "/foo/baz"
				);
			NUnit.Framework.Assert.AreEqual(new Path("/foo/bar/./baz").ToString(), "/foo/bar/baz"
				);
			NUnit.Framework.Assert.AreEqual(new Path("/foo/bar/baz/../../fud").ToString(), "/foo/fud"
				);
			NUnit.Framework.Assert.AreEqual(new Path("/foo/bar/baz/.././../fud").ToString(), 
				"/foo/fud");
			NUnit.Framework.Assert.AreEqual(new Path("../../foo/bar").ToString(), "../../foo/bar"
				);
			NUnit.Framework.Assert.AreEqual(new Path(".././../foo/bar").ToString(), "../../foo/bar"
				);
			NUnit.Framework.Assert.AreEqual(new Path("./foo/bar/baz").ToString(), "foo/bar/baz"
				);
			NUnit.Framework.Assert.AreEqual(new Path("/foo/bar/../../baz/boo").ToString(), "/baz/boo"
				);
			NUnit.Framework.Assert.AreEqual(new Path("foo/bar/").ToString(), "foo/bar");
			NUnit.Framework.Assert.AreEqual(new Path("foo/bar/../baz").ToString(), "foo/baz");
			NUnit.Framework.Assert.AreEqual(new Path("foo/bar/../../baz/boo").ToString(), "baz/boo"
				);
			// Test Path(Path,Path)
			NUnit.Framework.Assert.AreEqual(new Path("/foo/bar", "baz/boo").ToString(), "/foo/bar/baz/boo"
				);
			NUnit.Framework.Assert.AreEqual(new Path("foo/bar/", "baz/bud").ToString(), "foo/bar/baz/bud"
				);
			NUnit.Framework.Assert.AreEqual(new Path("/foo/bar", "../../boo/bud").ToString(), 
				"/boo/bud");
			NUnit.Framework.Assert.AreEqual(new Path("foo/bar", "../../boo/bud").ToString(), 
				"boo/bud");
			NUnit.Framework.Assert.AreEqual(new Path(".", "boo/bud").ToString(), "boo/bud");
			NUnit.Framework.Assert.AreEqual(new Path("/foo/bar/baz", "../../boo/bud").ToString
				(), "/foo/boo/bud");
			NUnit.Framework.Assert.AreEqual(new Path("foo/bar/baz", "../../boo/bud").ToString
				(), "foo/boo/bud");
			NUnit.Framework.Assert.AreEqual(new Path("../../", "../../boo/bud").ToString(), "../../../../boo/bud"
				);
			NUnit.Framework.Assert.AreEqual(new Path("../../foo", "../../../boo/bud").ToString
				(), "../../../../boo/bud");
			NUnit.Framework.Assert.AreEqual(new Path("../../foo/bar", "../boo/bud").ToString(
				), "../../foo/boo/bud");
			NUnit.Framework.Assert.AreEqual(new Path("foo/bar/baz", "../../..").ToString(), string.Empty
				);
			NUnit.Framework.Assert.AreEqual(new Path("foo/bar/baz", "../../../../..").ToString
				(), "../..");
		}

		/// <summary>Test that Windows paths are correctly handled</summary>
		/// <exception cref="Sharpen.URISyntaxException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestWindowsPaths()
		{
			if (!Path.Windows)
			{
				return;
			}
			NUnit.Framework.Assert.AreEqual(new Path("c:\\foo\\bar").ToString(), "c:/foo/bar"
				);
			NUnit.Framework.Assert.AreEqual(new Path("c:/foo/bar").ToString(), "c:/foo/bar");
			NUnit.Framework.Assert.AreEqual(new Path("/c:/foo/bar").ToString(), "c:/foo/bar");
			NUnit.Framework.Assert.AreEqual(new Path("file://c:/foo/bar").ToString(), "file://c:/foo/bar"
				);
		}

		/// <summary>Test invalid paths on Windows are correctly rejected</summary>
		/// <exception cref="Sharpen.URISyntaxException"/>
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
		/// <exception cref="Sharpen.URISyntaxException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestChildParentResolution()
		{
			Path parent = new Path("foo1://bar1/baz1");
			Path child = new Path("foo2://bar2/baz2");
			NUnit.Framework.Assert.AreEqual(child, new Path(parent, child));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestScheme()
		{
			NUnit.Framework.Assert.AreEqual("foo:/bar", new Path("foo:/", "/bar").ToString());
			NUnit.Framework.Assert.AreEqual("foo://bar/baz", new Path("foo://bar/", "/baz").ToString
				());
		}

		/// <exception cref="Sharpen.URISyntaxException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestURI()
		{
			URI uri = new URI("file:///bar#baz");
			Path path = new Path(uri);
			NUnit.Framework.Assert.IsTrue(uri.Equals(new URI(path.ToString())));
			FileSystem fs = path.GetFileSystem(new Configuration());
			NUnit.Framework.Assert.IsTrue(uri.Equals(new URI(fs.MakeQualified(path).ToString(
				))));
			// uri without hash
			URI uri2 = new URI("file:///bar/baz");
			NUnit.Framework.Assert.IsTrue(uri2.Equals(new URI(fs.MakeQualified(new Path(uri2)
				).ToString())));
			NUnit.Framework.Assert.AreEqual("foo://bar/baz#boo", new Path("foo://bar/", new Path
				(new URI("/baz#boo"))).ToString());
			NUnit.Framework.Assert.AreEqual("foo://bar/baz/fud#boo", new Path(new Path(new URI
				("foo://bar/baz#bud")), new Path(new URI("fud#boo"))).ToString());
			// if the child uri is absolute path
			NUnit.Framework.Assert.AreEqual("foo://bar/fud#boo", new Path(new Path(new URI("foo://bar/baz#bud"
				)), new Path(new URI("/fud#boo"))).ToString());
		}

		/// <summary>Test URIs created from Path objects</summary>
		/// <exception cref="Sharpen.URISyntaxException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestPathToUriConversion()
		{
			// Path differs from URI in that it ignores the query part..
			NUnit.Framework.Assert.AreEqual("? mark char in to URI", new URI(null, null, "/foo?bar"
				, null, null), new Path("/foo?bar").ToUri());
			NUnit.Framework.Assert.AreEqual("escape slashes chars in to URI", new URI(null, null
				, "/foo\"bar", null, null), new Path("/foo\"bar").ToUri());
			NUnit.Framework.Assert.AreEqual("spaces in chars to URI", new URI(null, null, "/foo bar"
				, null, null), new Path("/foo bar").ToUri());
			// therefore "foo?bar" is a valid Path, so a URI created from a Path
			// has path "foo?bar" where in a straight URI the path part is just "foo"
			NUnit.Framework.Assert.AreEqual("/foo?bar", new Path("http://localhost/foo?bar").
				ToUri().GetPath());
			NUnit.Framework.Assert.AreEqual("/foo", new URI("http://localhost/foo?bar").GetPath
				());
			// The path part handling in Path is equivalent to URI
			NUnit.Framework.Assert.AreEqual(new URI("/foo;bar").GetPath(), new Path("/foo;bar"
				).ToUri().GetPath());
			NUnit.Framework.Assert.AreEqual(new URI("/foo;bar"), new Path("/foo;bar").ToUri()
				);
			NUnit.Framework.Assert.AreEqual(new URI("/foo+bar"), new Path("/foo+bar").ToUri()
				);
			NUnit.Framework.Assert.AreEqual(new URI("/foo-bar"), new Path("/foo-bar").ToUri()
				);
			NUnit.Framework.Assert.AreEqual(new URI("/foo=bar"), new Path("/foo=bar").ToUri()
				);
			NUnit.Framework.Assert.AreEqual(new URI("/foo,bar"), new Path("/foo,bar").ToUri()
				);
		}

		/// <summary>Test reserved characters in URIs (and therefore Paths)</summary>
		/// <exception cref="Sharpen.URISyntaxException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestReservedCharacters()
		{
			// URI encodes the path
			NUnit.Framework.Assert.AreEqual("/foo%20bar", new URI(null, null, "/foo bar", null
				, null).GetRawPath());
			// URI#getPath decodes the path
			NUnit.Framework.Assert.AreEqual("/foo bar", new URI(null, null, "/foo bar", null, 
				null).GetPath());
			// URI#toString returns an encoded path
			NUnit.Framework.Assert.AreEqual("/foo%20bar", new URI(null, null, "/foo bar", null
				, null).ToString());
			NUnit.Framework.Assert.AreEqual("/foo%20bar", new Path("/foo bar").ToUri().ToString
				());
			// Reserved chars are not encoded
			NUnit.Framework.Assert.AreEqual("/foo;bar", new URI("/foo;bar").GetPath());
			NUnit.Framework.Assert.AreEqual("/foo;bar", new URI("/foo;bar").GetRawPath());
			NUnit.Framework.Assert.AreEqual("/foo+bar", new URI("/foo+bar").GetPath());
			NUnit.Framework.Assert.AreEqual("/foo+bar", new URI("/foo+bar").GetRawPath());
			// URI#getPath decodes the path part (and URL#getPath does not decode)
			NUnit.Framework.Assert.AreEqual("/foo bar", new Path("http://localhost/foo bar").
				ToUri().GetPath());
			NUnit.Framework.Assert.AreEqual("/foo%20bar", new Path("http://localhost/foo bar"
				).ToUri().ToURL().AbsolutePath);
			NUnit.Framework.Assert.AreEqual("/foo?bar", new URI("http", "localhost", "/foo?bar"
				, null, null).GetPath());
			NUnit.Framework.Assert.AreEqual("/foo%3Fbar", new URI("http", "localhost", "/foo?bar"
				, null, null).ToURL().AbsolutePath);
		}

		/// <exception cref="Sharpen.URISyntaxException"/>
		public virtual void TestMakeQualified()
		{
			URI defaultUri = new URI("hdfs://host1/dir1");
			URI wd = new URI("hdfs://host2/dir2");
			// The scheme from defaultUri is used but the path part is not
			NUnit.Framework.Assert.AreEqual(new Path("hdfs://host1/dir/file"), new Path("file"
				).MakeQualified(defaultUri, new Path("/dir")));
			// The defaultUri is only used if the path + wd has no scheme    
			NUnit.Framework.Assert.AreEqual(new Path("hdfs://host2/dir2/file"), new Path("file"
				).MakeQualified(defaultUri, new Path(wd)));
		}

		public virtual void TestGetName()
		{
			NUnit.Framework.Assert.AreEqual(string.Empty, new Path("/").GetName());
			NUnit.Framework.Assert.AreEqual("foo", new Path("foo").GetName());
			NUnit.Framework.Assert.AreEqual("foo", new Path("/foo").GetName());
			NUnit.Framework.Assert.AreEqual("foo", new Path("/foo/").GetName());
			NUnit.Framework.Assert.AreEqual("bar", new Path("/foo/bar").GetName());
			NUnit.Framework.Assert.AreEqual("bar", new Path("hdfs://host/foo/bar").GetName());
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
			NUnit.Framework.Assert.IsTrue(lfs.IsDirectory(testRoot));
			lfs.SetWorkingDirectory(testRoot);
			// create a couple dirs with file in them
			Path[] paths = new Path[] { new Path(testRoot, "*/f"), new Path(testRoot, "d1/f")
				, new Path(testRoot, "d2/f") };
			Arrays.Sort(paths);
			foreach (Path p in paths)
			{
				lfs.Create(p).Close();
				NUnit.Framework.Assert.IsTrue(lfs.Exists(p));
			}
			// try the non-globbed listStatus
			FileStatus[] stats = lfs.ListStatus(new Path(testRoot, "*"));
			NUnit.Framework.Assert.AreEqual(1, stats.Length);
			NUnit.Framework.Assert.AreEqual(new Path(testRoot, "*/f"), stats[0].GetPath());
			// ensure globStatus with "*" finds all dir contents
			stats = lfs.GlobStatus(new Path(testRoot, "*"));
			Arrays.Sort(stats);
			Path[] parentPaths = new Path[paths.Length];
			for (int i = 0; i < paths.Length; i++)
			{
				parentPaths[i] = paths[i].GetParent();
			}
			NUnit.Framework.Assert.AreEqual(MergeStatuses(parentPaths), MergeStatuses(stats));
			// ensure that globStatus with an escaped "\*" only finds "*"
			stats = lfs.GlobStatus(new Path(testRoot, "\\*"));
			NUnit.Framework.Assert.AreEqual(1, stats.Length);
			NUnit.Framework.Assert.AreEqual(new Path(testRoot, "*"), stats[0].GetPath());
			// try to glob the inner file for all dirs
			stats = lfs.GlobStatus(new Path(testRoot, "*/f"));
			NUnit.Framework.Assert.AreEqual(paths.Length, stats.Length);
			NUnit.Framework.Assert.AreEqual(MergeStatuses(paths), MergeStatuses(stats));
			// try to get the inner file for only the "*" dir
			stats = lfs.GlobStatus(new Path(testRoot, "\\*/f"));
			NUnit.Framework.Assert.AreEqual(1, stats.Length);
			NUnit.Framework.Assert.AreEqual(new Path(testRoot, "*/f"), stats[0].GetPath());
			// try to glob all the contents of the "*" dir
			stats = lfs.GlobStatus(new Path(testRoot, "\\*/*"));
			NUnit.Framework.Assert.AreEqual(1, stats.Length);
			NUnit.Framework.Assert.AreEqual(new Path(testRoot, "*/f"), stats[0].GetPath());
		}

		public virtual void TestMergePaths()
		{
			NUnit.Framework.Assert.AreEqual(new Path("/foo/bar"), Path.MergePaths(new Path("/foo"
				), new Path("/bar")));
			NUnit.Framework.Assert.AreEqual(new Path("/foo/bar/baz"), Path.MergePaths(new Path
				("/foo/bar"), new Path("/baz")));
			NUnit.Framework.Assert.AreEqual(new Path("/foo/bar/baz"), Path.MergePaths(new Path
				("/foo"), new Path("/bar/baz")));
			NUnit.Framework.Assert.AreEqual(new Path(Shell.Windows ? "/C:/foo/bar" : "/C:/foo/C:/bar"
				), Path.MergePaths(new Path("/C:/foo"), new Path("/C:/bar")));
			NUnit.Framework.Assert.AreEqual(new Path(Shell.Windows ? "/C:/bar" : "/C:/C:/bar"
				), Path.MergePaths(new Path("/C:/"), new Path("/C:/bar")));
			NUnit.Framework.Assert.AreEqual(new Path("/bar"), Path.MergePaths(new Path("/"), 
				new Path("/bar")));
			NUnit.Framework.Assert.AreEqual(new Path("viewfs:///foo/bar"), Path.MergePaths(new 
				Path("viewfs:///foo"), new Path("file:///bar")));
			NUnit.Framework.Assert.AreEqual(new Path("viewfs://vfsauthority/foo/bar"), Path.MergePaths
				(new Path("viewfs://vfsauthority/foo"), new Path("file://fileauthority/bar")));
		}

		public virtual void TestIsWindowsAbsolutePath()
		{
			if (!Shell.Windows)
			{
				return;
			}
			NUnit.Framework.Assert.IsTrue(Path.IsWindowsAbsolutePath("C:\\test", false));
			NUnit.Framework.Assert.IsTrue(Path.IsWindowsAbsolutePath("C:/test", false));
			NUnit.Framework.Assert.IsTrue(Path.IsWindowsAbsolutePath("/C:/test", true));
			NUnit.Framework.Assert.IsFalse(Path.IsWindowsAbsolutePath("/test", false));
			NUnit.Framework.Assert.IsFalse(Path.IsWindowsAbsolutePath("/test", true));
			NUnit.Framework.Assert.IsFalse(Path.IsWindowsAbsolutePath("C:test", false));
			NUnit.Framework.Assert.IsFalse(Path.IsWindowsAbsolutePath("/C:test", true));
		}
	}
}
