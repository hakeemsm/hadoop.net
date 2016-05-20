using Sharpen;

namespace org.apache.hadoop.fs
{
	public class TestPath : NUnit.Framework.TestCase
	{
		/// <summary>
		/// Merge a bunch of Path objects into a sorted semicolon-separated
		/// path string.
		/// </summary>
		public static string mergeStatuses(org.apache.hadoop.fs.Path[] paths)
		{
			string[] pathStrings = new string[paths.Length];
			int i = 0;
			foreach (org.apache.hadoop.fs.Path path in paths)
			{
				pathStrings[i++] = path.toUri().getPath();
			}
			java.util.Arrays.sort(pathStrings);
			return com.google.common.@base.Joiner.on(";").join(pathStrings);
		}

		/// <summary>
		/// Merge a bunch of FileStatus objects into a sorted semicolon-separated
		/// path string.
		/// </summary>
		public static string mergeStatuses(org.apache.hadoop.fs.FileStatus[] statuses)
		{
			org.apache.hadoop.fs.Path[] paths = new org.apache.hadoop.fs.Path[statuses.Length
				];
			int i = 0;
			foreach (org.apache.hadoop.fs.FileStatus status in statuses)
			{
				paths[i++] = status.getPath();
			}
			return mergeStatuses(paths);
		}

		public virtual void testToString()
		{
			toStringTest("/");
			toStringTest("/foo");
			toStringTest("/foo/bar");
			toStringTest("foo");
			toStringTest("foo/bar");
			toStringTest("/foo/bar#boo");
			toStringTest("foo/bar#boo");
			bool emptyException = false;
			try
			{
				toStringTest(string.Empty);
			}
			catch (System.ArgumentException)
			{
				// expect to receive an IllegalArgumentException
				emptyException = true;
			}
			NUnit.Framework.Assert.IsTrue(emptyException);
			if (org.apache.hadoop.fs.Path.WINDOWS)
			{
				toStringTest("c:");
				toStringTest("c:/");
				toStringTest("c:foo");
				toStringTest("c:foo/bar");
				toStringTest("c:foo/bar");
				toStringTest("c:/foo/bar");
				toStringTest("C:/foo/bar#boo");
				toStringTest("C:foo/bar#boo");
			}
		}

		private void toStringTest(string pathString)
		{
			NUnit.Framework.Assert.AreEqual(pathString, new org.apache.hadoop.fs.Path(pathString
				).ToString());
		}

		/// <exception cref="java.net.URISyntaxException"/>
		public virtual void testNormalize()
		{
			NUnit.Framework.Assert.AreEqual(string.Empty, new org.apache.hadoop.fs.Path(".").
				ToString());
			NUnit.Framework.Assert.AreEqual("..", new org.apache.hadoop.fs.Path("..").ToString
				());
			NUnit.Framework.Assert.AreEqual("/", new org.apache.hadoop.fs.Path("/").ToString(
				));
			NUnit.Framework.Assert.AreEqual("/", new org.apache.hadoop.fs.Path("//").ToString
				());
			NUnit.Framework.Assert.AreEqual("/", new org.apache.hadoop.fs.Path("///").ToString
				());
			NUnit.Framework.Assert.AreEqual("//foo/", new org.apache.hadoop.fs.Path("//foo/")
				.ToString());
			NUnit.Framework.Assert.AreEqual("//foo/", new org.apache.hadoop.fs.Path("//foo//"
				).ToString());
			NUnit.Framework.Assert.AreEqual("//foo/bar", new org.apache.hadoop.fs.Path("//foo//bar"
				).ToString());
			NUnit.Framework.Assert.AreEqual("/foo", new org.apache.hadoop.fs.Path("/foo/").ToString
				());
			NUnit.Framework.Assert.AreEqual("/foo", new org.apache.hadoop.fs.Path("/foo/").ToString
				());
			NUnit.Framework.Assert.AreEqual("foo", new org.apache.hadoop.fs.Path("foo/").ToString
				());
			NUnit.Framework.Assert.AreEqual("foo", new org.apache.hadoop.fs.Path("foo//").ToString
				());
			NUnit.Framework.Assert.AreEqual("foo/bar", new org.apache.hadoop.fs.Path("foo//bar"
				).ToString());
			NUnit.Framework.Assert.AreEqual("hdfs://foo/foo2/bar/baz/", new org.apache.hadoop.fs.Path
				(new java.net.URI("hdfs://foo//foo2///bar/baz///")).ToString());
			if (org.apache.hadoop.fs.Path.WINDOWS)
			{
				NUnit.Framework.Assert.AreEqual("c:/a/b", new org.apache.hadoop.fs.Path("c:\\a\\b"
					).ToString());
			}
		}

		public virtual void testIsAbsolute()
		{
			NUnit.Framework.Assert.IsTrue(new org.apache.hadoop.fs.Path("/").isAbsolute());
			NUnit.Framework.Assert.IsTrue(new org.apache.hadoop.fs.Path("/foo").isAbsolute());
			NUnit.Framework.Assert.IsFalse(new org.apache.hadoop.fs.Path("foo").isAbsolute());
			NUnit.Framework.Assert.IsFalse(new org.apache.hadoop.fs.Path("foo/bar").isAbsolute
				());
			NUnit.Framework.Assert.IsFalse(new org.apache.hadoop.fs.Path(".").isAbsolute());
			if (org.apache.hadoop.fs.Path.WINDOWS)
			{
				NUnit.Framework.Assert.IsTrue(new org.apache.hadoop.fs.Path("c:/a/b").isAbsolute(
					));
				NUnit.Framework.Assert.IsFalse(new org.apache.hadoop.fs.Path("c:a/b").isAbsolute(
					));
			}
		}

		public virtual void testParent()
		{
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/foo"), new org.apache.hadoop.fs.Path
				("/foo/bar").getParent());
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("foo"), new org.apache.hadoop.fs.Path
				("foo/bar").getParent());
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/"), new org.apache.hadoop.fs.Path
				("/foo").getParent());
			NUnit.Framework.Assert.AreEqual(null, new org.apache.hadoop.fs.Path("/").getParent
				());
			if (org.apache.hadoop.fs.Path.WINDOWS)
			{
				NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("c:/"), new org.apache.hadoop.fs.Path
					("c:/foo").getParent());
			}
		}

		public virtual void testChild()
		{
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("."), new org.apache.hadoop.fs.Path
				(".", "."));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/"), new org.apache.hadoop.fs.Path
				("/", "."));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/"), new org.apache.hadoop.fs.Path
				(".", "/"));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/foo"), new org.apache.hadoop.fs.Path
				("/", "foo"));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/foo/bar"), new org.apache.hadoop.fs.Path
				("/foo", "bar"));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/foo/bar/baz"), new 
				org.apache.hadoop.fs.Path("/foo/bar", "baz"));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/foo/bar/baz"), new 
				org.apache.hadoop.fs.Path("/foo", "bar/baz"));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("foo"), new org.apache.hadoop.fs.Path
				(".", "foo"));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("foo/bar"), new org.apache.hadoop.fs.Path
				("foo", "bar"));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("foo/bar/baz"), new 
				org.apache.hadoop.fs.Path("foo", "bar/baz"));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("foo/bar/baz"), new 
				org.apache.hadoop.fs.Path("foo/bar", "baz"));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/foo"), new org.apache.hadoop.fs.Path
				("/bar", "/foo"));
			if (org.apache.hadoop.fs.Path.WINDOWS)
			{
				NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("c:/foo"), new org.apache.hadoop.fs.Path
					("/bar", "c:/foo"));
				NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("c:/foo"), new org.apache.hadoop.fs.Path
					("d:/bar", "c:/foo"));
			}
		}

		public virtual void testPathThreeArgContructor()
		{
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("foo"), new org.apache.hadoop.fs.Path
				(null, null, "foo"));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("scheme:///foo"), new 
				org.apache.hadoop.fs.Path("scheme", null, "/foo"));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("scheme://authority/foo"
				), new org.apache.hadoop.fs.Path("scheme", "authority", "/foo"));
			if (org.apache.hadoop.fs.Path.WINDOWS)
			{
				NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("c:/foo/bar"), new 
					org.apache.hadoop.fs.Path(null, null, "c:/foo/bar"));
				NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("c:/foo/bar"), new 
					org.apache.hadoop.fs.Path(null, null, "/c:/foo/bar"));
			}
			else
			{
				NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("./a:b"), new org.apache.hadoop.fs.Path
					(null, null, "a:b"));
			}
			// Resolution tests
			if (org.apache.hadoop.fs.Path.WINDOWS)
			{
				NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("c:/foo/bar"), new 
					org.apache.hadoop.fs.Path("/fou", new org.apache.hadoop.fs.Path(null, null, "c:/foo/bar"
					)));
				NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("c:/foo/bar"), new 
					org.apache.hadoop.fs.Path("/fou", new org.apache.hadoop.fs.Path(null, null, "/c:/foo/bar"
					)));
				NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/foo/bar"), new org.apache.hadoop.fs.Path
					("/foo", new org.apache.hadoop.fs.Path(null, null, "bar")));
			}
			else
			{
				NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/foo/bar/a:b"), new 
					org.apache.hadoop.fs.Path("/foo/bar", new org.apache.hadoop.fs.Path(null, null, 
					"a:b")));
				NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/a:b"), new org.apache.hadoop.fs.Path
					("/foo/bar", new org.apache.hadoop.fs.Path(null, null, "/a:b")));
			}
		}

		public virtual void testEquals()
		{
			NUnit.Framework.Assert.IsFalse(new org.apache.hadoop.fs.Path("/").Equals(new org.apache.hadoop.fs.Path
				("/foo")));
		}

		public virtual void testDots()
		{
			// Test Path(String) 
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/foo/bar/baz").ToString
				(), "/foo/bar/baz");
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/foo/bar", ".").ToString
				(), "/foo/bar");
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/foo/bar/../baz").
				ToString(), "/foo/baz");
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/foo/bar/./baz").ToString
				(), "/foo/bar/baz");
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/foo/bar/baz/../../fud"
				).ToString(), "/foo/fud");
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/foo/bar/baz/.././../fud"
				).ToString(), "/foo/fud");
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("../../foo/bar").ToString
				(), "../../foo/bar");
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path(".././../foo/bar").
				ToString(), "../../foo/bar");
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("./foo/bar/baz").ToString
				(), "foo/bar/baz");
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/foo/bar/../../baz/boo"
				).ToString(), "/baz/boo");
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("foo/bar/").ToString
				(), "foo/bar");
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("foo/bar/../baz").ToString
				(), "foo/baz");
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("foo/bar/../../baz/boo"
				).ToString(), "baz/boo");
			// Test Path(Path,Path)
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/foo/bar", "baz/boo"
				).ToString(), "/foo/bar/baz/boo");
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("foo/bar/", "baz/bud"
				).ToString(), "foo/bar/baz/bud");
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/foo/bar", "../../boo/bud"
				).ToString(), "/boo/bud");
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("foo/bar", "../../boo/bud"
				).ToString(), "boo/bud");
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path(".", "boo/bud").ToString
				(), "boo/bud");
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/foo/bar/baz", "../../boo/bud"
				).ToString(), "/foo/boo/bud");
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("foo/bar/baz", "../../boo/bud"
				).ToString(), "foo/boo/bud");
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("../../", "../../boo/bud"
				).ToString(), "../../../../boo/bud");
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("../../foo", "../../../boo/bud"
				).ToString(), "../../../../boo/bud");
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("../../foo/bar", "../boo/bud"
				).ToString(), "../../foo/boo/bud");
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("foo/bar/baz", "../../.."
				).ToString(), string.Empty);
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("foo/bar/baz", "../../../../.."
				).ToString(), "../..");
		}

		/// <summary>Test that Windows paths are correctly handled</summary>
		/// <exception cref="java.net.URISyntaxException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testWindowsPaths()
		{
			if (!org.apache.hadoop.fs.Path.WINDOWS)
			{
				return;
			}
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("c:\\foo\\bar").ToString
				(), "c:/foo/bar");
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("c:/foo/bar").ToString
				(), "c:/foo/bar");
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/c:/foo/bar").ToString
				(), "c:/foo/bar");
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("file://c:/foo/bar"
				).ToString(), "file://c:/foo/bar");
		}

		/// <summary>Test invalid paths on Windows are correctly rejected</summary>
		/// <exception cref="java.net.URISyntaxException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testInvalidWindowsPaths()
		{
			if (!org.apache.hadoop.fs.Path.WINDOWS)
			{
				return;
			}
			string[] invalidPaths = new string[] { "hdfs:\\\\\\tmp" };
			foreach (string path in invalidPaths)
			{
				try
				{
					org.apache.hadoop.fs.Path item = new org.apache.hadoop.fs.Path(path);
					fail("Did not throw for invalid path " + path);
				}
				catch (System.ArgumentException)
				{
				}
			}
		}

		/// <summary>Test Path objects created from other Path objects</summary>
		/// <exception cref="java.net.URISyntaxException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testChildParentResolution()
		{
			org.apache.hadoop.fs.Path parent = new org.apache.hadoop.fs.Path("foo1://bar1/baz1"
				);
			org.apache.hadoop.fs.Path child = new org.apache.hadoop.fs.Path("foo2://bar2/baz2"
				);
			NUnit.Framework.Assert.AreEqual(child, new org.apache.hadoop.fs.Path(parent, child
				));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testScheme()
		{
			NUnit.Framework.Assert.AreEqual("foo:/bar", new org.apache.hadoop.fs.Path("foo:/"
				, "/bar").ToString());
			NUnit.Framework.Assert.AreEqual("foo://bar/baz", new org.apache.hadoop.fs.Path("foo://bar/"
				, "/baz").ToString());
		}

		/// <exception cref="java.net.URISyntaxException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testURI()
		{
			java.net.URI uri = new java.net.URI("file:///bar#baz");
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(uri);
			NUnit.Framework.Assert.IsTrue(uri.Equals(new java.net.URI(path.ToString())));
			org.apache.hadoop.fs.FileSystem fs = path.getFileSystem(new org.apache.hadoop.conf.Configuration
				());
			NUnit.Framework.Assert.IsTrue(uri.Equals(new java.net.URI(fs.makeQualified(path).
				ToString())));
			// uri without hash
			java.net.URI uri2 = new java.net.URI("file:///bar/baz");
			NUnit.Framework.Assert.IsTrue(uri2.Equals(new java.net.URI(fs.makeQualified(new org.apache.hadoop.fs.Path
				(uri2)).ToString())));
			NUnit.Framework.Assert.AreEqual("foo://bar/baz#boo", new org.apache.hadoop.fs.Path
				("foo://bar/", new org.apache.hadoop.fs.Path(new java.net.URI("/baz#boo"))).ToString
				());
			NUnit.Framework.Assert.AreEqual("foo://bar/baz/fud#boo", new org.apache.hadoop.fs.Path
				(new org.apache.hadoop.fs.Path(new java.net.URI("foo://bar/baz#bud")), new org.apache.hadoop.fs.Path
				(new java.net.URI("fud#boo"))).ToString());
			// if the child uri is absolute path
			NUnit.Framework.Assert.AreEqual("foo://bar/fud#boo", new org.apache.hadoop.fs.Path
				(new org.apache.hadoop.fs.Path(new java.net.URI("foo://bar/baz#bud")), new org.apache.hadoop.fs.Path
				(new java.net.URI("/fud#boo"))).ToString());
		}

		/// <summary>Test URIs created from Path objects</summary>
		/// <exception cref="java.net.URISyntaxException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testPathToUriConversion()
		{
			// Path differs from URI in that it ignores the query part..
			NUnit.Framework.Assert.AreEqual("? mark char in to URI", new java.net.URI(null, null
				, "/foo?bar", null, null), new org.apache.hadoop.fs.Path("/foo?bar").toUri());
			NUnit.Framework.Assert.AreEqual("escape slashes chars in to URI", new java.net.URI
				(null, null, "/foo\"bar", null, null), new org.apache.hadoop.fs.Path("/foo\"bar"
				).toUri());
			NUnit.Framework.Assert.AreEqual("spaces in chars to URI", new java.net.URI(null, 
				null, "/foo bar", null, null), new org.apache.hadoop.fs.Path("/foo bar").toUri()
				);
			// therefore "foo?bar" is a valid Path, so a URI created from a Path
			// has path "foo?bar" where in a straight URI the path part is just "foo"
			NUnit.Framework.Assert.AreEqual("/foo?bar", new org.apache.hadoop.fs.Path("http://localhost/foo?bar"
				).toUri().getPath());
			NUnit.Framework.Assert.AreEqual("/foo", new java.net.URI("http://localhost/foo?bar"
				).getPath());
			// The path part handling in Path is equivalent to URI
			NUnit.Framework.Assert.AreEqual(new java.net.URI("/foo;bar").getPath(), new org.apache.hadoop.fs.Path
				("/foo;bar").toUri().getPath());
			NUnit.Framework.Assert.AreEqual(new java.net.URI("/foo;bar"), new org.apache.hadoop.fs.Path
				("/foo;bar").toUri());
			NUnit.Framework.Assert.AreEqual(new java.net.URI("/foo+bar"), new org.apache.hadoop.fs.Path
				("/foo+bar").toUri());
			NUnit.Framework.Assert.AreEqual(new java.net.URI("/foo-bar"), new org.apache.hadoop.fs.Path
				("/foo-bar").toUri());
			NUnit.Framework.Assert.AreEqual(new java.net.URI("/foo=bar"), new org.apache.hadoop.fs.Path
				("/foo=bar").toUri());
			NUnit.Framework.Assert.AreEqual(new java.net.URI("/foo,bar"), new org.apache.hadoop.fs.Path
				("/foo,bar").toUri());
		}

		/// <summary>Test reserved characters in URIs (and therefore Paths)</summary>
		/// <exception cref="java.net.URISyntaxException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testReservedCharacters()
		{
			// URI encodes the path
			NUnit.Framework.Assert.AreEqual("/foo%20bar", new java.net.URI(null, null, "/foo bar"
				, null, null).getRawPath());
			// URI#getPath decodes the path
			NUnit.Framework.Assert.AreEqual("/foo bar", new java.net.URI(null, null, "/foo bar"
				, null, null).getPath());
			// URI#toString returns an encoded path
			NUnit.Framework.Assert.AreEqual("/foo%20bar", new java.net.URI(null, null, "/foo bar"
				, null, null).ToString());
			NUnit.Framework.Assert.AreEqual("/foo%20bar", new org.apache.hadoop.fs.Path("/foo bar"
				).toUri().ToString());
			// Reserved chars are not encoded
			NUnit.Framework.Assert.AreEqual("/foo;bar", new java.net.URI("/foo;bar").getPath(
				));
			NUnit.Framework.Assert.AreEqual("/foo;bar", new java.net.URI("/foo;bar").getRawPath
				());
			NUnit.Framework.Assert.AreEqual("/foo+bar", new java.net.URI("/foo+bar").getPath(
				));
			NUnit.Framework.Assert.AreEqual("/foo+bar", new java.net.URI("/foo+bar").getRawPath
				());
			// URI#getPath decodes the path part (and URL#getPath does not decode)
			NUnit.Framework.Assert.AreEqual("/foo bar", new org.apache.hadoop.fs.Path("http://localhost/foo bar"
				).toUri().getPath());
			NUnit.Framework.Assert.AreEqual("/foo%20bar", new org.apache.hadoop.fs.Path("http://localhost/foo bar"
				).toUri().toURL().getPath());
			NUnit.Framework.Assert.AreEqual("/foo?bar", new java.net.URI("http", "localhost", 
				"/foo?bar", null, null).getPath());
			NUnit.Framework.Assert.AreEqual("/foo%3Fbar", new java.net.URI("http", "localhost"
				, "/foo?bar", null, null).toURL().getPath());
		}

		/// <exception cref="java.net.URISyntaxException"/>
		public virtual void testMakeQualified()
		{
			java.net.URI defaultUri = new java.net.URI("hdfs://host1/dir1");
			java.net.URI wd = new java.net.URI("hdfs://host2/dir2");
			// The scheme from defaultUri is used but the path part is not
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("hdfs://host1/dir/file"
				), new org.apache.hadoop.fs.Path("file").makeQualified(defaultUri, new org.apache.hadoop.fs.Path
				("/dir")));
			// The defaultUri is only used if the path + wd has no scheme    
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("hdfs://host2/dir2/file"
				), new org.apache.hadoop.fs.Path("file").makeQualified(defaultUri, new org.apache.hadoop.fs.Path
				(wd)));
		}

		public virtual void testGetName()
		{
			NUnit.Framework.Assert.AreEqual(string.Empty, new org.apache.hadoop.fs.Path("/").
				getName());
			NUnit.Framework.Assert.AreEqual("foo", new org.apache.hadoop.fs.Path("foo").getName
				());
			NUnit.Framework.Assert.AreEqual("foo", new org.apache.hadoop.fs.Path("/foo").getName
				());
			NUnit.Framework.Assert.AreEqual("foo", new org.apache.hadoop.fs.Path("/foo/").getName
				());
			NUnit.Framework.Assert.AreEqual("bar", new org.apache.hadoop.fs.Path("/foo/bar").
				getName());
			NUnit.Framework.Assert.AreEqual("bar", new org.apache.hadoop.fs.Path("hdfs://host/foo/bar"
				).getName());
		}

		/// <exception cref="System.Exception"/>
		public virtual void testAvroReflect()
		{
			org.apache.hadoop.io.AvroTestUtil.testReflect(new org.apache.hadoop.fs.Path("foo"
				), "{\"type\":\"string\",\"java-class\":\"org.apache.hadoop.fs.Path\"}");
		}

		/// <exception cref="System.Exception"/>
		public virtual void testGlobEscapeStatus()
		{
			// This test is not meaningful on Windows where * is disallowed in file name.
			if (org.apache.hadoop.util.Shell.WINDOWS)
			{
				return;
			}
			org.apache.hadoop.fs.FileSystem lfs = org.apache.hadoop.fs.FileSystem.getLocal(new 
				org.apache.hadoop.conf.Configuration());
			org.apache.hadoop.fs.Path testRoot = lfs.makeQualified(new org.apache.hadoop.fs.Path
				(Sharpen.Runtime.getProperty("test.build.data", "test/build/data"), "testPathGlob"
				));
			lfs.delete(testRoot, true);
			lfs.mkdirs(testRoot);
			NUnit.Framework.Assert.IsTrue(lfs.isDirectory(testRoot));
			lfs.setWorkingDirectory(testRoot);
			// create a couple dirs with file in them
			org.apache.hadoop.fs.Path[] paths = new org.apache.hadoop.fs.Path[] { new org.apache.hadoop.fs.Path
				(testRoot, "*/f"), new org.apache.hadoop.fs.Path(testRoot, "d1/f"), new org.apache.hadoop.fs.Path
				(testRoot, "d2/f") };
			java.util.Arrays.sort(paths);
			foreach (org.apache.hadoop.fs.Path p in paths)
			{
				lfs.create(p).close();
				NUnit.Framework.Assert.IsTrue(lfs.exists(p));
			}
			// try the non-globbed listStatus
			org.apache.hadoop.fs.FileStatus[] stats = lfs.listStatus(new org.apache.hadoop.fs.Path
				(testRoot, "*"));
			NUnit.Framework.Assert.AreEqual(1, stats.Length);
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path(testRoot, "*/f"), stats
				[0].getPath());
			// ensure globStatus with "*" finds all dir contents
			stats = lfs.globStatus(new org.apache.hadoop.fs.Path(testRoot, "*"));
			java.util.Arrays.sort(stats);
			org.apache.hadoop.fs.Path[] parentPaths = new org.apache.hadoop.fs.Path[paths.Length
				];
			for (int i = 0; i < paths.Length; i++)
			{
				parentPaths[i] = paths[i].getParent();
			}
			NUnit.Framework.Assert.AreEqual(mergeStatuses(parentPaths), mergeStatuses(stats));
			// ensure that globStatus with an escaped "\*" only finds "*"
			stats = lfs.globStatus(new org.apache.hadoop.fs.Path(testRoot, "\\*"));
			NUnit.Framework.Assert.AreEqual(1, stats.Length);
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path(testRoot, "*"), stats
				[0].getPath());
			// try to glob the inner file for all dirs
			stats = lfs.globStatus(new org.apache.hadoop.fs.Path(testRoot, "*/f"));
			NUnit.Framework.Assert.AreEqual(paths.Length, stats.Length);
			NUnit.Framework.Assert.AreEqual(mergeStatuses(paths), mergeStatuses(stats));
			// try to get the inner file for only the "*" dir
			stats = lfs.globStatus(new org.apache.hadoop.fs.Path(testRoot, "\\*/f"));
			NUnit.Framework.Assert.AreEqual(1, stats.Length);
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path(testRoot, "*/f"), stats
				[0].getPath());
			// try to glob all the contents of the "*" dir
			stats = lfs.globStatus(new org.apache.hadoop.fs.Path(testRoot, "\\*/*"));
			NUnit.Framework.Assert.AreEqual(1, stats.Length);
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path(testRoot, "*/f"), stats
				[0].getPath());
		}

		public virtual void testMergePaths()
		{
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/foo/bar"), org.apache.hadoop.fs.Path
				.mergePaths(new org.apache.hadoop.fs.Path("/foo"), new org.apache.hadoop.fs.Path
				("/bar")));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/foo/bar/baz"), org.apache.hadoop.fs.Path
				.mergePaths(new org.apache.hadoop.fs.Path("/foo/bar"), new org.apache.hadoop.fs.Path
				("/baz")));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/foo/bar/baz"), org.apache.hadoop.fs.Path
				.mergePaths(new org.apache.hadoop.fs.Path("/foo"), new org.apache.hadoop.fs.Path
				("/bar/baz")));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path(org.apache.hadoop.util.Shell
				.WINDOWS ? "/C:/foo/bar" : "/C:/foo/C:/bar"), org.apache.hadoop.fs.Path.mergePaths
				(new org.apache.hadoop.fs.Path("/C:/foo"), new org.apache.hadoop.fs.Path("/C:/bar"
				)));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path(org.apache.hadoop.util.Shell
				.WINDOWS ? "/C:/bar" : "/C:/C:/bar"), org.apache.hadoop.fs.Path.mergePaths(new org.apache.hadoop.fs.Path
				("/C:/"), new org.apache.hadoop.fs.Path("/C:/bar")));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/bar"), org.apache.hadoop.fs.Path
				.mergePaths(new org.apache.hadoop.fs.Path("/"), new org.apache.hadoop.fs.Path("/bar"
				)));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("viewfs:///foo/bar"
				), org.apache.hadoop.fs.Path.mergePaths(new org.apache.hadoop.fs.Path("viewfs:///foo"
				), new org.apache.hadoop.fs.Path("file:///bar")));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("viewfs://vfsauthority/foo/bar"
				), org.apache.hadoop.fs.Path.mergePaths(new org.apache.hadoop.fs.Path("viewfs://vfsauthority/foo"
				), new org.apache.hadoop.fs.Path("file://fileauthority/bar")));
		}

		public virtual void testIsWindowsAbsolutePath()
		{
			if (!org.apache.hadoop.util.Shell.WINDOWS)
			{
				return;
			}
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.Path.isWindowsAbsolutePath("C:\\test"
				, false));
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.Path.isWindowsAbsolutePath("C:/test"
				, false));
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.Path.isWindowsAbsolutePath("/C:/test"
				, true));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.Path.isWindowsAbsolutePath("/test"
				, false));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.Path.isWindowsAbsolutePath("/test"
				, true));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.Path.isWindowsAbsolutePath("C:test"
				, false));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.Path.isWindowsAbsolutePath("/C:test"
				, true));
		}
	}
}
