using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	public class TestGlobPaths
	{
		private static readonly UserGroupInformation unprivilegedUser = UserGroupInformation
			.CreateUserForTesting("myuser", new string[] { "mygroup" });

		internal class RegexPathFilter : PathFilter
		{
			private readonly string regex;

			public RegexPathFilter(string regex)
			{
				this.regex = regex;
			}

			public virtual bool Accept(Path path)
			{
				return path.ToString().Matches(regex);
			}
		}

		private static MiniDFSCluster dfsCluster;

		private static FileSystem fs;

		private static FileSystem privilegedFs;

		private static FileContext fc;

		private static FileContext privilegedFc;

		private const int NumOfPaths = 4;

		private static string UserDir;

		private readonly Path[] path = new Path[NumOfPaths];

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetUp()
		{
			Configuration conf = new HdfsConfiguration();
			dfsCluster = new MiniDFSCluster.Builder(conf).Build();
			privilegedFs = FileSystem.Get(conf);
			privilegedFc = FileContext.GetFileContext(conf);
			// allow unpriviledged user ability to create paths
			privilegedFs.SetPermission(new Path("/"), FsPermission.CreateImmutable((short)0x1ff
				));
			UserGroupInformation.SetLoginUser(unprivilegedUser);
			fs = FileSystem.Get(conf);
			fc = FileContext.GetFileContext(conf);
			UserDir = fs.GetHomeDirectory().ToUri().GetPath().ToString();
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TearDown()
		{
			if (dfsCluster != null)
			{
				dfsCluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestMultiGlob()
		{
			FileStatus[] status;
			/*
			*  /dir1/subdir1
			*  /dir1/subdir1/f1
			*  /dir1/subdir1/f2
			*  /dir1/subdir2/f1
			*  /dir2/subdir1
			*  /dir2/subdir2
			*  /dir2/subdir2/f1
			*  /dir3/f1
			*  /dir3/f1
			*  /dir3/f2(dir)
			*  /dir3/subdir2(file)
			*  /dir3/subdir3
			*  /dir3/subdir3/f1
			*  /dir3/subdir3/f1/f1
			*  /dir3/subdir3/f3
			*  /dir4
			*/
			Path d1 = new Path(UserDir, "dir1");
			Path d11 = new Path(d1, "subdir1");
			Path d12 = new Path(d1, "subdir2");
			Path f111 = new Path(d11, "f1");
			fs.CreateNewFile(f111);
			Path f112 = new Path(d11, "f2");
			fs.CreateNewFile(f112);
			Path f121 = new Path(d12, "f1");
			fs.CreateNewFile(f121);
			Path d2 = new Path(UserDir, "dir2");
			Path d21 = new Path(d2, "subdir1");
			fs.Mkdirs(d21);
			Path d22 = new Path(d2, "subdir2");
			Path f221 = new Path(d22, "f1");
			fs.CreateNewFile(f221);
			Path d3 = new Path(UserDir, "dir3");
			Path f31 = new Path(d3, "f1");
			fs.CreateNewFile(f31);
			Path d32 = new Path(d3, "f2");
			fs.Mkdirs(d32);
			Path f32 = new Path(d3, "subdir2");
			// fake as a subdir!
			fs.CreateNewFile(f32);
			Path d33 = new Path(d3, "subdir3");
			Path f333 = new Path(d33, "f3");
			fs.CreateNewFile(f333);
			Path d331 = new Path(d33, "f1");
			Path f3311 = new Path(d331, "f1");
			fs.CreateNewFile(f3311);
			Path d4 = new Path(UserDir, "dir4");
			fs.Mkdirs(d4);
			/*
			* basic
			*/
			Path root = new Path(UserDir);
			status = fs.GlobStatus(root);
			CheckStatus(status, root);
			status = fs.GlobStatus(new Path(UserDir, "x"));
			NUnit.Framework.Assert.IsNull(status);
			status = fs.GlobStatus(new Path("x"));
			NUnit.Framework.Assert.IsNull(status);
			status = fs.GlobStatus(new Path(UserDir, "x/x"));
			NUnit.Framework.Assert.IsNull(status);
			status = fs.GlobStatus(new Path("x/x"));
			NUnit.Framework.Assert.IsNull(status);
			status = fs.GlobStatus(new Path(UserDir, "*"));
			CheckStatus(status, d1, d2, d3, d4);
			status = fs.GlobStatus(new Path("*"));
			CheckStatus(status, d1, d2, d3, d4);
			status = fs.GlobStatus(new Path(UserDir, "*/x"));
			CheckStatus(status);
			status = fs.GlobStatus(new Path("*/x"));
			CheckStatus(status);
			status = fs.GlobStatus(new Path(UserDir, "x/*"));
			CheckStatus(status);
			status = fs.GlobStatus(new Path("x/*"));
			CheckStatus(status);
			// make sure full pattern is scanned instead of bailing early with undef
			status = fs.GlobStatus(new Path(UserDir, "x/x/x/*"));
			CheckStatus(status);
			status = fs.GlobStatus(new Path("x/x/x/*"));
			CheckStatus(status);
			status = fs.GlobStatus(new Path(UserDir, "*/*"));
			CheckStatus(status, d11, d12, d21, d22, f31, d32, f32, d33);
			status = fs.GlobStatus(new Path("*/*"));
			CheckStatus(status, d11, d12, d21, d22, f31, d32, f32, d33);
			/*
			* one level deep
			*/
			status = fs.GlobStatus(new Path(UserDir, "dir*/*"));
			CheckStatus(status, d11, d12, d21, d22, f31, d32, f32, d33);
			status = fs.GlobStatus(new Path("dir*/*"));
			CheckStatus(status, d11, d12, d21, d22, f31, d32, f32, d33);
			status = fs.GlobStatus(new Path(UserDir, "dir*/subdir*"));
			CheckStatus(status, d11, d12, d21, d22, f32, d33);
			status = fs.GlobStatus(new Path("dir*/subdir*"));
			CheckStatus(status, d11, d12, d21, d22, f32, d33);
			status = fs.GlobStatus(new Path(UserDir, "dir*/f*"));
			CheckStatus(status, f31, d32);
			status = fs.GlobStatus(new Path("dir*/f*"));
			CheckStatus(status, f31, d32);
			/*
			* subdir1 globs
			*/
			status = fs.GlobStatus(new Path(UserDir, "dir*/subdir1"));
			CheckStatus(status, d11, d21);
			status = fs.GlobStatus(new Path(UserDir, "dir*/subdir1/*"));
			CheckStatus(status, f111, f112);
			status = fs.GlobStatus(new Path(UserDir, "dir*/subdir1/*/*"));
			CheckStatus(status);
			status = fs.GlobStatus(new Path(UserDir, "dir*/subdir1/x"));
			CheckStatus(status);
			status = fs.GlobStatus(new Path(UserDir, "dir*/subdir1/x*"));
			CheckStatus(status);
			/*
			* subdir2 globs
			*/
			status = fs.GlobStatus(new Path(UserDir, "dir*/subdir2"));
			CheckStatus(status, d12, d22, f32);
			status = fs.GlobStatus(new Path(UserDir, "dir*/subdir2/*"));
			CheckStatus(status, f121, f221);
			status = fs.GlobStatus(new Path(UserDir, "dir*/subdir2/*/*"));
			CheckStatus(status);
			/*
			* subdir3 globs
			*/
			status = fs.GlobStatus(new Path(UserDir, "dir*/subdir3"));
			CheckStatus(status, d33);
			status = fs.GlobStatus(new Path(UserDir, "dir*/subdir3/*"));
			CheckStatus(status, d331, f333);
			status = fs.GlobStatus(new Path(UserDir, "dir*/subdir3/*/*"));
			CheckStatus(status, f3311);
			status = fs.GlobStatus(new Path(UserDir, "dir*/subdir3/*/*/*"));
			CheckStatus(status);
			/*
			* file1 single dir globs
			*/
			status = fs.GlobStatus(new Path(UserDir, "dir*/subdir1/f1"));
			CheckStatus(status, f111);
			status = fs.GlobStatus(new Path(UserDir, "dir*/subdir1/f1*"));
			CheckStatus(status, f111);
			status = fs.GlobStatus(new Path(UserDir, "dir*/subdir1/f1/*"));
			CheckStatus(status);
			status = fs.GlobStatus(new Path(UserDir, "dir*/subdir1/f1*/*"));
			CheckStatus(status);
			/*
			* file1 multi-dir globs
			*/
			status = fs.GlobStatus(new Path(UserDir, "dir*/subdir*/f1"));
			CheckStatus(status, f111, f121, f221, d331);
			status = fs.GlobStatus(new Path(UserDir, "dir*/subdir*/f1*"));
			CheckStatus(status, f111, f121, f221, d331);
			status = fs.GlobStatus(new Path(UserDir, "dir*/subdir*/f1/*"));
			CheckStatus(status, f3311);
			status = fs.GlobStatus(new Path(UserDir, "dir*/subdir*/f1*/*"));
			CheckStatus(status, f3311);
			status = fs.GlobStatus(new Path(UserDir, "dir*/subdir*/f1*/*"));
			CheckStatus(status, f3311);
			status = fs.GlobStatus(new Path(UserDir, "dir*/subdir*/f1*/x"));
			CheckStatus(status);
			status = fs.GlobStatus(new Path(UserDir, "dir*/subdir*/f1*/*/*"));
			CheckStatus(status);
			/*
			*  file glob multiple files
			*/
			status = fs.GlobStatus(new Path(UserDir, "dir*/subdir*"));
			CheckStatus(status, d11, d12, d21, d22, f32, d33);
			status = fs.GlobStatus(new Path(UserDir, "dir*/subdir*/*"));
			CheckStatus(status, f111, f112, f121, f221, d331, f333);
			status = fs.GlobStatus(new Path(UserDir, "dir*/subdir*/f*"));
			CheckStatus(status, f111, f112, f121, f221, d331, f333);
			status = fs.GlobStatus(new Path(UserDir, "dir*/subdir*/f*/*"));
			CheckStatus(status, f3311);
			status = fs.GlobStatus(new Path(UserDir, "dir*/subdir*/*/f1"));
			CheckStatus(status, f3311);
			status = fs.GlobStatus(new Path(UserDir, "dir*/subdir*/*/*"));
			CheckStatus(status, f3311);
			// doesn't exist
			status = fs.GlobStatus(new Path(UserDir, "dir*/subdir1/f3"));
			CheckStatus(status);
			status = fs.GlobStatus(new Path(UserDir, "dir*/subdir1/f3*"));
			CheckStatus(status);
			status = fs.GlobStatus(new Path("{x}"));
			CheckStatus(status);
			status = fs.GlobStatus(new Path("{x,y}"));
			CheckStatus(status);
			status = fs.GlobStatus(new Path("dir*/{x,y}"));
			CheckStatus(status);
			status = fs.GlobStatus(new Path("dir*/{f1,y}"));
			CheckStatus(status, f31);
			status = fs.GlobStatus(new Path("{x,y}"));
			CheckStatus(status);
			status = fs.GlobStatus(new Path("/{x/x,y/y}"));
			CheckStatus(status);
			status = fs.GlobStatus(new Path("{x/x,y/y}"));
			CheckStatus(status);
			status = fs.GlobStatus(new Path(Path.CurDir));
			CheckStatus(status, new Path(UserDir));
			status = fs.GlobStatus(new Path(UserDir + "{/dir1}"));
			CheckStatus(status, d1);
			status = fs.GlobStatus(new Path(UserDir + "{/dir*}"));
			CheckStatus(status, d1, d2, d3, d4);
			status = fs.GlobStatus(new Path(Path.Separator), trueFilter);
			CheckStatus(status, new Path(Path.Separator));
			status = fs.GlobStatus(new Path(Path.CurDir), trueFilter);
			CheckStatus(status, new Path(UserDir));
			status = fs.GlobStatus(d1, trueFilter);
			CheckStatus(status, d1);
			status = fs.GlobStatus(new Path(UserDir), trueFilter);
			CheckStatus(status, new Path(UserDir));
			status = fs.GlobStatus(new Path(UserDir, "*"), trueFilter);
			CheckStatus(status, d1, d2, d3, d4);
			status = fs.GlobStatus(new Path("/x/*"), trueFilter);
			CheckStatus(status);
			status = fs.GlobStatus(new Path("/x"), trueFilter);
			NUnit.Framework.Assert.IsNull(status);
			status = fs.GlobStatus(new Path("/x/x"), trueFilter);
			NUnit.Framework.Assert.IsNull(status);
			/*
			* false filter
			*/
			PathFilter falseFilter = new _PathFilter_387();
			status = fs.GlobStatus(new Path(Path.Separator), falseFilter);
			NUnit.Framework.Assert.IsNull(status);
			status = fs.GlobStatus(new Path(Path.CurDir), falseFilter);
			NUnit.Framework.Assert.IsNull(status);
			status = fs.GlobStatus(new Path(UserDir), falseFilter);
			NUnit.Framework.Assert.IsNull(status);
			status = fs.GlobStatus(new Path(UserDir, "*"), falseFilter);
			CheckStatus(status);
			status = fs.GlobStatus(new Path("/x/*"), falseFilter);
			CheckStatus(status);
			status = fs.GlobStatus(new Path("/x"), falseFilter);
			NUnit.Framework.Assert.IsNull(status);
			status = fs.GlobStatus(new Path("/x/x"), falseFilter);
			NUnit.Framework.Assert.IsNull(status);
			CleanupDFS();
		}

		private sealed class _PathFilter_387 : PathFilter
		{
			public _PathFilter_387()
			{
			}

			public bool Accept(Path path)
			{
				return false;
			}
		}

		private void CheckStatus(FileStatus[] status, params Path[] expectedMatches)
		{
			NUnit.Framework.Assert.IsNotNull(status);
			string[] paths = new string[status.Length];
			for (int i = 0; i < status.Length; i++)
			{
				paths[i] = GetPathFromStatus(status[i]);
			}
			string got = StringUtils.Join(paths, "\n");
			string expected = StringUtils.Join(expectedMatches, "\n");
			NUnit.Framework.Assert.AreEqual(expected, got);
		}

		private string GetPathFromStatus(FileStatus status)
		{
			return status.GetPath().ToUri().GetPath();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestPathFilter()
		{
			try
			{
				string[] files = new string[] { UserDir + "/a", UserDir + "/a/b" };
				Path[] matchedPath = PrepareTesting(UserDir + "/*/*", files, new TestGlobPaths.RegexPathFilter
					("^.*" + Sharpen.Pattern.Quote(UserDir) + "/a/b"));
				NUnit.Framework.Assert.AreEqual(1, matchedPath.Length);
				NUnit.Framework.Assert.AreEqual(path[1], matchedPath[0]);
			}
			finally
			{
				CleanupDFS();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestPathFilterWithFixedLastComponent()
		{
			try
			{
				string[] files = new string[] { UserDir + "/a", UserDir + "/a/b", UserDir + "/c", 
					UserDir + "/c/b" };
				Path[] matchedPath = PrepareTesting(UserDir + "/*/b", files, new TestGlobPaths.RegexPathFilter
					("^.*" + Sharpen.Pattern.Quote(UserDir) + "/a/b"));
				NUnit.Framework.Assert.AreEqual(matchedPath.Length, 1);
				NUnit.Framework.Assert.AreEqual(matchedPath[0], path[1]);
			}
			finally
			{
				CleanupDFS();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void PTestLiteral()
		{
			try
			{
				string[] files = new string[] { UserDir + "/a2c", UserDir + "/abc.d" };
				Path[] matchedPath = PrepareTesting(UserDir + "/abc.d", files);
				NUnit.Framework.Assert.AreEqual(matchedPath.Length, 1);
				NUnit.Framework.Assert.AreEqual(matchedPath[0], path[1]);
			}
			finally
			{
				CleanupDFS();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void PTestEscape()
		{
			// Skip the test case on Windows because backslash will be treated as a
			// path separator instead of an escaping character on Windows.
			Assume.AssumeTrue(!Path.Windows);
			try
			{
				string[] files = new string[] { UserDir + "/ab\\[c.d" };
				Path[] matchedPath = PrepareTesting(UserDir + "/ab\\[c.d", files);
				NUnit.Framework.Assert.AreEqual(matchedPath.Length, 1);
				NUnit.Framework.Assert.AreEqual(matchedPath[0], path[0]);
			}
			finally
			{
				CleanupDFS();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void PTestAny()
		{
			try
			{
				string[] files = new string[] { UserDir + "/abc", UserDir + "/a2c", UserDir + "/a.c"
					, UserDir + "/abcd" };
				Path[] matchedPath = PrepareTesting(UserDir + "/a?c", files);
				NUnit.Framework.Assert.AreEqual(matchedPath.Length, 3);
				NUnit.Framework.Assert.AreEqual(matchedPath[0], path[2]);
				NUnit.Framework.Assert.AreEqual(matchedPath[1], path[1]);
				NUnit.Framework.Assert.AreEqual(matchedPath[2], path[0]);
			}
			finally
			{
				CleanupDFS();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void PTestClosure1()
		{
			try
			{
				string[] files = new string[] { UserDir + "/a", UserDir + "/abc", UserDir + "/abc.p"
					, UserDir + "/bacd" };
				Path[] matchedPath = PrepareTesting(UserDir + "/a*", files);
				NUnit.Framework.Assert.AreEqual(matchedPath.Length, 3);
				NUnit.Framework.Assert.AreEqual(matchedPath[0], path[0]);
				NUnit.Framework.Assert.AreEqual(matchedPath[1], path[1]);
				NUnit.Framework.Assert.AreEqual(matchedPath[2], path[2]);
			}
			finally
			{
				CleanupDFS();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void PTestClosure2()
		{
			try
			{
				string[] files = new string[] { UserDir + "/a.", UserDir + "/a.txt", UserDir + "/a.old.java"
					, UserDir + "/.java" };
				Path[] matchedPath = PrepareTesting(UserDir + "/a.*", files);
				NUnit.Framework.Assert.AreEqual(matchedPath.Length, 3);
				NUnit.Framework.Assert.AreEqual(matchedPath[0], path[0]);
				NUnit.Framework.Assert.AreEqual(matchedPath[1], path[2]);
				NUnit.Framework.Assert.AreEqual(matchedPath[2], path[1]);
			}
			finally
			{
				CleanupDFS();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void PTestClosure3()
		{
			try
			{
				string[] files = new string[] { UserDir + "/a.txt.x", UserDir + "/ax", UserDir + 
					"/ab37x", UserDir + "/bacd" };
				Path[] matchedPath = PrepareTesting(UserDir + "/a*x", files);
				NUnit.Framework.Assert.AreEqual(matchedPath.Length, 3);
				NUnit.Framework.Assert.AreEqual(matchedPath[0], path[0]);
				NUnit.Framework.Assert.AreEqual(matchedPath[1], path[2]);
				NUnit.Framework.Assert.AreEqual(matchedPath[2], path[1]);
			}
			finally
			{
				CleanupDFS();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void PTestClosure4()
		{
			try
			{
				string[] files = new string[] { UserDir + "/dir1/file1", UserDir + "/dir2/file2", 
					UserDir + "/dir3/file1" };
				Path[] matchedPath = PrepareTesting(UserDir + "/*/file1", files);
				NUnit.Framework.Assert.AreEqual(matchedPath.Length, 2);
				NUnit.Framework.Assert.AreEqual(matchedPath[0], path[0]);
				NUnit.Framework.Assert.AreEqual(matchedPath[1], path[2]);
			}
			finally
			{
				CleanupDFS();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void PTestClosure5()
		{
			try
			{
				string[] files = new string[] { UserDir + "/dir1/file1", UserDir + "/file1" };
				Path[] matchedPath = PrepareTesting(UserDir + "/*/file1", files);
				NUnit.Framework.Assert.AreEqual(matchedPath.Length, 1);
				NUnit.Framework.Assert.AreEqual(matchedPath[0], path[0]);
			}
			finally
			{
				CleanupDFS();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void PTestSet()
		{
			try
			{
				string[] files = new string[] { UserDir + "/a.c", UserDir + "/a.cpp", UserDir + "/a.hlp"
					, UserDir + "/a.hxy" };
				Path[] matchedPath = PrepareTesting(UserDir + "/a.[ch]??", files);
				NUnit.Framework.Assert.AreEqual(matchedPath.Length, 3);
				NUnit.Framework.Assert.AreEqual(matchedPath[0], path[1]);
				NUnit.Framework.Assert.AreEqual(matchedPath[1], path[2]);
				NUnit.Framework.Assert.AreEqual(matchedPath[2], path[3]);
			}
			finally
			{
				CleanupDFS();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void PTestRange()
		{
			try
			{
				string[] files = new string[] { UserDir + "/a.d", UserDir + "/a.e", UserDir + "/a.f"
					, UserDir + "/a.h" };
				Path[] matchedPath = PrepareTesting(UserDir + "/a.[d-fm]", files);
				NUnit.Framework.Assert.AreEqual(matchedPath.Length, 3);
				NUnit.Framework.Assert.AreEqual(matchedPath[0], path[0]);
				NUnit.Framework.Assert.AreEqual(matchedPath[1], path[1]);
				NUnit.Framework.Assert.AreEqual(matchedPath[2], path[2]);
			}
			finally
			{
				CleanupDFS();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void PTestSetExcl()
		{
			try
			{
				string[] files = new string[] { UserDir + "/a.d", UserDir + "/a.e", UserDir + "/a.0"
					, UserDir + "/a.h" };
				Path[] matchedPath = PrepareTesting(UserDir + "/a.[^a-cg-z0-9]", files);
				NUnit.Framework.Assert.AreEqual(matchedPath.Length, 2);
				NUnit.Framework.Assert.AreEqual(matchedPath[0], path[0]);
				NUnit.Framework.Assert.AreEqual(matchedPath[1], path[1]);
			}
			finally
			{
				CleanupDFS();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void PTestCombination()
		{
			try
			{
				string[] files = new string[] { "/user/aa/a.c", "/user/bb/a.cpp", "/user1/cc/b.hlp"
					, "/user/dd/a.hxy" };
				Path[] matchedPath = PrepareTesting("/use?/*/a.[ch]{lp,xy}", files);
				NUnit.Framework.Assert.AreEqual(matchedPath.Length, 1);
				NUnit.Framework.Assert.AreEqual(matchedPath[0], path[3]);
			}
			finally
			{
				CleanupDFS();
			}
		}

		/* Test {xx,yy} */
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void PTestCurlyBracket()
		{
			Path[] matchedPath;
			string[] files;
			try
			{
				files = new string[] { UserDir + "/a.abcxx", UserDir + "/a.abxy", UserDir + "/a.hlp"
					, UserDir + "/a.jhyy" };
				matchedPath = PrepareTesting(UserDir + "/a.{abc,jh}??", files);
				NUnit.Framework.Assert.AreEqual(matchedPath.Length, 2);
				NUnit.Framework.Assert.AreEqual(matchedPath[0], path[0]);
				NUnit.Framework.Assert.AreEqual(matchedPath[1], path[3]);
			}
			finally
			{
				CleanupDFS();
			}
			// nested curlies
			try
			{
				files = new string[] { UserDir + "/a.abcxx", UserDir + "/a.abdxy", UserDir + "/a.hlp"
					, UserDir + "/a.jhyy" };
				matchedPath = PrepareTesting(UserDir + "/a.{ab{c,d},jh}??", files);
				NUnit.Framework.Assert.AreEqual(matchedPath.Length, 3);
				NUnit.Framework.Assert.AreEqual(matchedPath[0], path[0]);
				NUnit.Framework.Assert.AreEqual(matchedPath[1], path[1]);
				NUnit.Framework.Assert.AreEqual(matchedPath[2], path[3]);
			}
			finally
			{
				CleanupDFS();
			}
			// cross-component curlies
			try
			{
				files = new string[] { UserDir + "/a/b", UserDir + "/a/d", UserDir + "/c/b", UserDir
					 + "/c/d" };
				matchedPath = PrepareTesting(UserDir + "/{a/b,c/d}", files);
				NUnit.Framework.Assert.AreEqual(matchedPath.Length, 2);
				NUnit.Framework.Assert.AreEqual(matchedPath[0], path[0]);
				NUnit.Framework.Assert.AreEqual(matchedPath[1], path[3]);
			}
			finally
			{
				CleanupDFS();
			}
			// cross-component absolute curlies
			try
			{
				files = new string[] { "/a/b", "/a/d", "/c/b", "/c/d" };
				matchedPath = PrepareTesting("{/a/b,/c/d}", files);
				NUnit.Framework.Assert.AreEqual(matchedPath.Length, 2);
				NUnit.Framework.Assert.AreEqual(matchedPath[0], path[0]);
				NUnit.Framework.Assert.AreEqual(matchedPath[1], path[3]);
			}
			finally
			{
				CleanupDFS();
			}
			try
			{
				// test standalone }
				files = new string[] { UserDir + "/}bc", UserDir + "/}c" };
				matchedPath = PrepareTesting(UserDir + "/}{a,b}c", files);
				NUnit.Framework.Assert.AreEqual(matchedPath.Length, 1);
				NUnit.Framework.Assert.AreEqual(matchedPath[0], path[0]);
				// test {b}
				matchedPath = PrepareTesting(UserDir + "/}{b}c", files);
				NUnit.Framework.Assert.AreEqual(matchedPath.Length, 1);
				NUnit.Framework.Assert.AreEqual(matchedPath[0], path[0]);
				// test {}
				matchedPath = PrepareTesting(UserDir + "/}{}bc", files);
				NUnit.Framework.Assert.AreEqual(matchedPath.Length, 1);
				NUnit.Framework.Assert.AreEqual(matchedPath[0], path[0]);
				// test {,}
				matchedPath = PrepareTesting(UserDir + "/}{,}bc", files);
				NUnit.Framework.Assert.AreEqual(matchedPath.Length, 1);
				NUnit.Framework.Assert.AreEqual(matchedPath[0], path[0]);
				// test {b,}
				matchedPath = PrepareTesting(UserDir + "/}{b,}c", files);
				NUnit.Framework.Assert.AreEqual(matchedPath.Length, 2);
				NUnit.Framework.Assert.AreEqual(matchedPath[0], path[0]);
				NUnit.Framework.Assert.AreEqual(matchedPath[1], path[1]);
				// test {,b}
				matchedPath = PrepareTesting(UserDir + "/}{,b}c", files);
				NUnit.Framework.Assert.AreEqual(matchedPath.Length, 2);
				NUnit.Framework.Assert.AreEqual(matchedPath[0], path[0]);
				NUnit.Framework.Assert.AreEqual(matchedPath[1], path[1]);
				// test a combination of {} and ?
				matchedPath = PrepareTesting(UserDir + "/}{ac,?}", files);
				NUnit.Framework.Assert.AreEqual(matchedPath.Length, 1);
				NUnit.Framework.Assert.AreEqual(matchedPath[0], path[1]);
				// test ill-formed curly
				bool hasException = false;
				try
				{
					PrepareTesting(UserDir + "}{bc", files);
				}
				catch (IOException e)
				{
					NUnit.Framework.Assert.IsTrue(e.Message.StartsWith("Illegal file pattern:"));
					hasException = true;
				}
				NUnit.Framework.Assert.IsTrue(hasException);
			}
			finally
			{
				CleanupDFS();
			}
		}

		/* test that a path name can contain Java regex special characters */
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void PTestJavaRegexSpecialChars()
		{
			try
			{
				string[] files = new string[] { UserDir + "/($.|+)bc", UserDir + "/abc" };
				Path[] matchedPath = PrepareTesting(UserDir + "/($.|+)*", files);
				NUnit.Framework.Assert.AreEqual(matchedPath.Length, 1);
				NUnit.Framework.Assert.AreEqual(matchedPath[0], path[0]);
			}
			finally
			{
				CleanupDFS();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private Path[] PrepareTesting(string pattern, string[] files)
		{
			for (int i = 0; i < Math.Min(NumOfPaths, files.Length); i++)
			{
				path[i] = fs.MakeQualified(new Path(files[i]));
				if (!fs.Mkdirs(path[i]))
				{
					throw new IOException("Mkdirs failed to create " + path[i].ToString());
				}
			}
			Path patternPath = new Path(pattern);
			Path[] globResults = FileUtil.Stat2Paths(fs.GlobStatus(patternPath), patternPath);
			for (int i_1 = 0; i_1 < globResults.Length; i_1++)
			{
				globResults[i_1] = globResults[i_1].MakeQualified(fs.GetUri(), fs.GetWorkingDirectory
					());
			}
			return globResults;
		}

		/// <exception cref="System.IO.IOException"/>
		private Path[] PrepareTesting(string pattern, string[] files, PathFilter filter)
		{
			for (int i = 0; i < Math.Min(NumOfPaths, files.Length); i++)
			{
				path[i] = fs.MakeQualified(new Path(files[i]));
				if (!fs.Mkdirs(path[i]))
				{
					throw new IOException("Mkdirs failed to create " + path[i].ToString());
				}
			}
			Path patternPath = new Path(pattern);
			Path[] globResults = FileUtil.Stat2Paths(fs.GlobStatus(patternPath, filter), patternPath
				);
			for (int i_1 = 0; i_1 < globResults.Length; i_1++)
			{
				globResults[i_1] = globResults[i_1].MakeQualified(fs.GetUri(), fs.GetWorkingDirectory
					());
			}
			return globResults;
		}

		/// <exception cref="System.IO.IOException"/>
		private void CleanupDFS()
		{
			fs.Delete(new Path(UserDir), true);
		}

		/// <summary>A glob test that can be run on either FileContext or FileSystem.</summary>
		private abstract class FSTestWrapperGlobTest
		{
			internal FSTestWrapperGlobTest(TestGlobPaths _enclosing, bool useFc)
			{
				this._enclosing = _enclosing;
				if (useFc)
				{
					this.privWrap = new FileContextTestWrapper(TestGlobPaths.privilegedFc);
					this.wrap = new FileContextTestWrapper(TestGlobPaths.fc);
				}
				else
				{
					this.privWrap = new FileSystemTestWrapper(TestGlobPaths.privilegedFs);
					this.wrap = new FileSystemTestWrapper(TestGlobPaths.fs);
				}
			}

			/// <exception cref="System.Exception"/>
			internal abstract void Run();

			internal readonly FSTestWrapper privWrap;

			internal readonly FSTestWrapper wrap;

			private readonly TestGlobPaths _enclosing;
		}

		/// <summary>Run a glob test on FileSystem.</summary>
		/// <exception cref="System.Exception"/>
		private void TestOnFileSystem(TestGlobPaths.FSTestWrapperGlobTest test)
		{
			try
			{
				fc.Mkdir(new Path(UserDir), FsPermission.GetDefault(), true);
				test.Run();
			}
			finally
			{
				fc.Delete(new Path(UserDir), true);
			}
		}

		/// <summary>Run a glob test on FileContext.</summary>
		/// <exception cref="System.Exception"/>
		private void TestOnFileContext(TestGlobPaths.FSTestWrapperGlobTest test)
		{
			try
			{
				fs.Mkdirs(new Path(UserDir));
				test.Run();
			}
			finally
			{
				CleanupDFS();
			}
		}

		/// <summary>Accept all paths.</summary>
		private class AcceptAllPathFilter : PathFilter
		{
			public virtual bool Accept(Path path)
			{
				return true;
			}
		}

		private static readonly PathFilter trueFilter = new TestGlobPaths.AcceptAllPathFilter
			();

		/// <summary>Accept only paths ending in Z.</summary>
		private class AcceptPathsEndingInZ : PathFilter
		{
			public virtual bool Accept(Path path)
			{
				string stringPath = path.ToUri().GetPath();
				return stringPath.EndsWith("z");
			}
		}

		/// <summary>Test globbing through symlinks.</summary>
		private class TestGlobWithSymlinks : TestGlobPaths.FSTestWrapperGlobTest
		{
			internal TestGlobWithSymlinks(TestGlobPaths _enclosing, bool useFc)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			internal override void Run()
			{
				// Test that globbing through a symlink to a directory yields a path
				// containing that symlink.
				this.wrap.Mkdir(new Path(TestGlobPaths.UserDir + "/alpha"), FsPermission.GetDirDefault
					(), false);
				this.wrap.CreateSymlink(new Path(TestGlobPaths.UserDir + "/alpha"), new Path(TestGlobPaths
					.UserDir + "/alphaLink"), false);
				this.wrap.Mkdir(new Path(TestGlobPaths.UserDir + "/alphaLink/beta"), FsPermission
					.GetDirDefault(), false);
				// Test simple glob
				FileStatus[] statuses = this.wrap.GlobStatus(new Path(TestGlobPaths.UserDir + "/alpha/*"
					), new TestGlobPaths.AcceptAllPathFilter());
				NUnit.Framework.Assert.AreEqual(1, statuses.Length);
				NUnit.Framework.Assert.AreEqual(TestGlobPaths.UserDir + "/alpha/beta", statuses[0
					].GetPath().ToUri().GetPath());
				// Test glob through symlink
				statuses = this.wrap.GlobStatus(new Path(TestGlobPaths.UserDir + "/alphaLink/*"), 
					new TestGlobPaths.AcceptAllPathFilter());
				NUnit.Framework.Assert.AreEqual(1, statuses.Length);
				NUnit.Framework.Assert.AreEqual(TestGlobPaths.UserDir + "/alphaLink/beta", statuses
					[0].GetPath().ToUri().GetPath());
				// If the terminal path component in a globbed path is a symlink,
				// we don't dereference that link.
				this.wrap.CreateSymlink(new Path("beta"), new Path(TestGlobPaths.UserDir + "/alphaLink/betaLink"
					), false);
				statuses = this.wrap.GlobStatus(new Path(TestGlobPaths.UserDir + "/alpha/betaLi*"
					), new TestGlobPaths.AcceptAllPathFilter());
				NUnit.Framework.Assert.AreEqual(1, statuses.Length);
				NUnit.Framework.Assert.AreEqual(TestGlobPaths.UserDir + "/alpha/betaLink", statuses
					[0].GetPath().ToUri().GetPath());
			}

			private readonly TestGlobPaths _enclosing;
			// todo: test symlink-to-symlink-to-dir, etc.
		}

		/// <exception cref="System.Exception"/>
		[Ignore]
		[NUnit.Framework.Test]
		public virtual void TestGlobWithSymlinksOnFS()
		{
			TestOnFileSystem(new TestGlobPaths.TestGlobWithSymlinks(this, false));
		}

		/// <exception cref="System.Exception"/>
		[Ignore]
		[NUnit.Framework.Test]
		public virtual void TestGlobWithSymlinksOnFC()
		{
			TestOnFileContext(new TestGlobPaths.TestGlobWithSymlinks(this, true));
		}

		/// <summary>Test globbing symlinks to symlinks.</summary>
		/// <remarks>
		/// Test globbing symlinks to symlinks.
		/// Also test globbing dangling symlinks.  It should NOT throw any exceptions!
		/// </remarks>
		private class TestGlobWithSymlinksToSymlinks : TestGlobPaths.FSTestWrapperGlobTest
		{
			internal TestGlobWithSymlinksToSymlinks(TestGlobPaths _enclosing, bool useFc)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			internal override void Run()
			{
				// Test that globbing through a symlink to a symlink to a directory
				// fully resolves
				this.wrap.Mkdir(new Path(TestGlobPaths.UserDir + "/alpha"), FsPermission.GetDirDefault
					(), false);
				this.wrap.CreateSymlink(new Path(TestGlobPaths.UserDir + "/alpha"), new Path(TestGlobPaths
					.UserDir + "/alphaLink"), false);
				this.wrap.CreateSymlink(new Path(TestGlobPaths.UserDir + "/alphaLink"), new Path(
					TestGlobPaths.UserDir + "/alphaLinkLink"), false);
				this.wrap.Mkdir(new Path(TestGlobPaths.UserDir + "/alpha/beta"), FsPermission.GetDirDefault
					(), false);
				// Test glob through symlink to a symlink to a directory
				FileStatus[] statuses = this.wrap.GlobStatus(new Path(TestGlobPaths.UserDir + "/alphaLinkLink"
					), new TestGlobPaths.AcceptAllPathFilter());
				NUnit.Framework.Assert.AreEqual(1, statuses.Length);
				NUnit.Framework.Assert.AreEqual(TestGlobPaths.UserDir + "/alphaLinkLink", statuses
					[0].GetPath().ToUri().GetPath());
				statuses = this.wrap.GlobStatus(new Path(TestGlobPaths.UserDir + "/alphaLinkLink/*"
					), new TestGlobPaths.AcceptAllPathFilter());
				NUnit.Framework.Assert.AreEqual(1, statuses.Length);
				NUnit.Framework.Assert.AreEqual(TestGlobPaths.UserDir + "/alphaLinkLink/beta", statuses
					[0].GetPath().ToUri().GetPath());
				// Test glob of dangling symlink (theta does not actually exist)
				this.wrap.CreateSymlink(new Path(TestGlobPaths.UserDir + "theta"), new Path(TestGlobPaths
					.UserDir + "/alpha/kappa"), false);
				statuses = this.wrap.GlobStatus(new Path(TestGlobPaths.UserDir + "/alpha/kappa/kappa"
					), new TestGlobPaths.AcceptAllPathFilter());
				NUnit.Framework.Assert.IsNull(statuses);
				// Test glob of symlinks
				this.wrap.CreateFile(TestGlobPaths.UserDir + "/alpha/beta/gamma");
				this.wrap.CreateSymlink(new Path(TestGlobPaths.UserDir + "gamma"), new Path(TestGlobPaths
					.UserDir + "/alpha/beta/gammaLink"), false);
				this.wrap.CreateSymlink(new Path(TestGlobPaths.UserDir + "gammaLink"), new Path(TestGlobPaths
					.UserDir + "/alpha/beta/gammaLinkLink"), false);
				this.wrap.CreateSymlink(new Path(TestGlobPaths.UserDir + "gammaLinkLink"), new Path
					(TestGlobPaths.UserDir + "/alpha/beta/gammaLinkLinkLink"), false);
				statuses = this.wrap.GlobStatus(new Path(TestGlobPaths.UserDir + "/alpha/*/gammaLinkLinkLink"
					), new TestGlobPaths.AcceptAllPathFilter());
				NUnit.Framework.Assert.AreEqual(1, statuses.Length);
				NUnit.Framework.Assert.AreEqual(TestGlobPaths.UserDir + "/alpha/beta/gammaLinkLinkLink"
					, statuses[0].GetPath().ToUri().GetPath());
				statuses = this.wrap.GlobStatus(new Path(TestGlobPaths.UserDir + "/alpha/beta/*")
					, new TestGlobPaths.AcceptAllPathFilter());
				NUnit.Framework.Assert.AreEqual(TestGlobPaths.UserDir + "/alpha/beta/gamma;" + TestGlobPaths
					.UserDir + "/alpha/beta/gammaLink;" + TestGlobPaths.UserDir + "/alpha/beta/gammaLinkLink;"
					 + TestGlobPaths.UserDir + "/alpha/beta/gammaLinkLinkLink", TestPath.MergeStatuses
					(statuses));
				// Let's create two symlinks that point to each other, and glob on them.
				this.wrap.CreateSymlink(new Path(TestGlobPaths.UserDir + "tweedledee"), new Path(
					TestGlobPaths.UserDir + "/tweedledum"), false);
				this.wrap.CreateSymlink(new Path(TestGlobPaths.UserDir + "tweedledum"), new Path(
					TestGlobPaths.UserDir + "/tweedledee"), false);
				statuses = this.wrap.GlobStatus(new Path(TestGlobPaths.UserDir + "/tweedledee/unobtainium"
					), new TestGlobPaths.AcceptAllPathFilter());
				NUnit.Framework.Assert.IsNull(statuses);
			}

			private readonly TestGlobPaths _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[Ignore]
		[NUnit.Framework.Test]
		public virtual void TestGlobWithSymlinksToSymlinksOnFS()
		{
			TestOnFileSystem(new TestGlobPaths.TestGlobWithSymlinksToSymlinks(this, false));
		}

		/// <exception cref="System.Exception"/>
		[Ignore]
		[NUnit.Framework.Test]
		public virtual void TestGlobWithSymlinksToSymlinksOnFC()
		{
			TestOnFileContext(new TestGlobPaths.TestGlobWithSymlinksToSymlinks(this, true));
		}

		/// <summary>Test globbing symlinks with a custom PathFilter</summary>
		private class TestGlobSymlinksWithCustomPathFilter : TestGlobPaths.FSTestWrapperGlobTest
		{
			internal TestGlobSymlinksWithCustomPathFilter(TestGlobPaths _enclosing, bool useFc
				)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			internal override void Run()
			{
				// Test that globbing through a symlink to a symlink to a directory
				// fully resolves
				this.wrap.Mkdir(new Path(TestGlobPaths.UserDir + "/alpha"), FsPermission.GetDirDefault
					(), false);
				this.wrap.CreateSymlink(new Path(TestGlobPaths.UserDir + "/alpha"), new Path(TestGlobPaths
					.UserDir + "/alphaLinkz"), false);
				this.wrap.Mkdir(new Path(TestGlobPaths.UserDir + "/alpha/beta"), FsPermission.GetDirDefault
					(), false);
				this.wrap.Mkdir(new Path(TestGlobPaths.UserDir + "/alpha/betaz"), FsPermission.GetDirDefault
					(), false);
				// Test glob through symlink to a symlink to a directory, with a
				// PathFilter
				FileStatus[] statuses = this.wrap.GlobStatus(new Path(TestGlobPaths.UserDir + "/alpha/beta"
					), new TestGlobPaths.AcceptPathsEndingInZ());
				NUnit.Framework.Assert.IsNull(statuses);
				statuses = this.wrap.GlobStatus(new Path(TestGlobPaths.UserDir + "/alphaLinkz/betaz"
					), new TestGlobPaths.AcceptPathsEndingInZ());
				NUnit.Framework.Assert.AreEqual(1, statuses.Length);
				NUnit.Framework.Assert.AreEqual(TestGlobPaths.UserDir + "/alphaLinkz/betaz", statuses
					[0].GetPath().ToUri().GetPath());
				statuses = this.wrap.GlobStatus(new Path(TestGlobPaths.UserDir + "/*/*"), new TestGlobPaths.AcceptPathsEndingInZ
					());
				NUnit.Framework.Assert.AreEqual(TestGlobPaths.UserDir + "/alpha/betaz;" + TestGlobPaths
					.UserDir + "/alphaLinkz/betaz", TestPath.MergeStatuses(statuses));
				statuses = this.wrap.GlobStatus(new Path(TestGlobPaths.UserDir + "/*/*"), new TestGlobPaths.AcceptAllPathFilter
					());
				NUnit.Framework.Assert.AreEqual(TestGlobPaths.UserDir + "/alpha/beta;" + TestGlobPaths
					.UserDir + "/alpha/betaz;" + TestGlobPaths.UserDir + "/alphaLinkz/beta;" + TestGlobPaths
					.UserDir + "/alphaLinkz/betaz", TestPath.MergeStatuses(statuses));
			}

			private readonly TestGlobPaths _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[Ignore]
		[NUnit.Framework.Test]
		public virtual void TestGlobSymlinksWithCustomPathFilterOnFS()
		{
			TestOnFileSystem(new TestGlobPaths.TestGlobSymlinksWithCustomPathFilter(this, false
				));
		}

		/// <exception cref="System.Exception"/>
		[Ignore]
		[NUnit.Framework.Test]
		public virtual void TestGlobSymlinksWithCustomPathFilterOnFC()
		{
			TestOnFileContext(new TestGlobPaths.TestGlobSymlinksWithCustomPathFilter(this, true
				));
		}

		/// <summary>Test that globStatus fills in the scheme even when it is not provided.</summary>
		private class TestGlobFillsInScheme : TestGlobPaths.FSTestWrapperGlobTest
		{
			internal TestGlobFillsInScheme(TestGlobPaths _enclosing, bool useFc)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			internal override void Run()
			{
				// Verify that the default scheme is hdfs, when we don't supply one.
				this.wrap.Mkdir(new Path(TestGlobPaths.UserDir + "/alpha"), FsPermission.GetDirDefault
					(), false);
				this.wrap.CreateSymlink(new Path(TestGlobPaths.UserDir + "/alpha"), new Path(TestGlobPaths
					.UserDir + "/alphaLink"), false);
				FileStatus[] statuses = this.wrap.GlobStatus(new Path(TestGlobPaths.UserDir + "/alphaLink"
					), new TestGlobPaths.AcceptAllPathFilter());
				NUnit.Framework.Assert.AreEqual(1, statuses.Length);
				Path path = statuses[0].GetPath();
				NUnit.Framework.Assert.AreEqual(TestGlobPaths.UserDir + "/alpha", path.ToUri().GetPath
					());
				NUnit.Framework.Assert.AreEqual("hdfs", path.ToUri().GetScheme());
				// FileContext can list a file:/// URI.
				// Since everyone should have the root directory, we list that.
				statuses = TestGlobPaths.fc.Util().GlobStatus(new Path("file:///"), new TestGlobPaths.AcceptAllPathFilter
					());
				NUnit.Framework.Assert.AreEqual(1, statuses.Length);
				Path filePath = statuses[0].GetPath();
				NUnit.Framework.Assert.AreEqual("file", filePath.ToUri().GetScheme());
				NUnit.Framework.Assert.AreEqual("/", filePath.ToUri().GetPath());
				// The FileSystem should have scheme 'hdfs'
				NUnit.Framework.Assert.AreEqual("hdfs", TestGlobPaths.fs.GetScheme());
			}

			private readonly TestGlobPaths _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGlobFillsInSchemeOnFS()
		{
			TestOnFileSystem(new TestGlobPaths.TestGlobFillsInScheme(this, false));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGlobFillsInSchemeOnFC()
		{
			TestOnFileContext(new TestGlobPaths.TestGlobFillsInScheme(this, true));
		}

		/// <summary>Test that globStatus works with relative paths.</summary>
		private class TestRelativePath : TestGlobPaths.FSTestWrapperGlobTest
		{
			internal TestRelativePath(TestGlobPaths _enclosing, bool useFc)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			internal override void Run()
			{
				string[] files = new string[] { "a", "abc", "abc.p", "bacd" };
				Path[] path = new Path[files.Length];
				for (int i = 0; i < files.Length; i++)
				{
					path[i] = this.wrap.MakeQualified(new Path(files[i]));
					this.wrap.Mkdir(path[i], FsPermission.GetDirDefault(), true);
				}
				Path patternPath = new Path("a*");
				Path[] globResults = FileUtil.Stat2Paths(this.wrap.GlobStatus(patternPath, new TestGlobPaths.AcceptAllPathFilter
					()), patternPath);
				for (int i_1 = 0; i_1 < globResults.Length; i_1++)
				{
					globResults[i_1] = this.wrap.MakeQualified(globResults[i_1]);
				}
				NUnit.Framework.Assert.AreEqual(globResults.Length, 3);
				// The default working directory for FileSystem is the user's home
				// directory.  For FileContext, the default is based on the UNIX user that
				// started the jvm.  This is arguably a bug (see HADOOP-10944 for
				// details).  We work around it here by explicitly calling
				// getWorkingDirectory and going from there.
				string pwd = this.wrap.GetWorkingDirectory().ToUri().GetPath();
				NUnit.Framework.Assert.AreEqual(pwd + "/a;" + pwd + "/abc;" + pwd + "/abc.p", TestPath
					.MergeStatuses(globResults));
			}

			private readonly TestGlobPaths _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRelativePathOnFS()
		{
			TestOnFileSystem(new TestGlobPaths.TestRelativePath(this, false));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRelativePathOnFC()
		{
			TestOnFileContext(new TestGlobPaths.TestRelativePath(this, true));
		}

		/// <summary>
		/// Test that trying to glob through a directory we don't have permission
		/// to list fails with AccessControlException rather than succeeding or
		/// throwing any other exception.
		/// </summary>
		private class TestGlobAccessDenied : TestGlobPaths.FSTestWrapperGlobTest
		{
			internal TestGlobAccessDenied(TestGlobPaths _enclosing, bool useFc)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			internal override void Run()
			{
				this.privWrap.Mkdir(new Path("/nopermission/val"), new FsPermission((short)0x1ff)
					, true);
				this.privWrap.Mkdir(new Path("/norestrictions/val"), new FsPermission((short)0x1ff
					), true);
				this.privWrap.SetPermission(new Path("/nopermission"), new FsPermission((short)0)
					);
				try
				{
					this.wrap.GlobStatus(new Path("/no*/*"), new TestGlobPaths.AcceptAllPathFilter());
					NUnit.Framework.Assert.Fail("expected to get an AccessControlException when " + "globbing through a directory we don't have permissions "
						 + "to list.");
				}
				catch (AccessControlException)
				{
				}
				NUnit.Framework.Assert.AreEqual("/norestrictions/val", TestPath.MergeStatuses(this
					.wrap.GlobStatus(new Path("/norestrictions/*"), new TestGlobPaths.AcceptAllPathFilter
					())));
			}

			private readonly TestGlobPaths _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGlobAccessDeniedOnFS()
		{
			TestOnFileSystem(new TestGlobPaths.TestGlobAccessDenied(this, false));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGlobAccessDeniedOnFC()
		{
			TestOnFileContext(new TestGlobPaths.TestGlobAccessDenied(this, true));
		}

		/// <summary>Test that trying to list a reserved path on HDFS via the globber works.</summary>
		private class TestReservedHdfsPaths : TestGlobPaths.FSTestWrapperGlobTest
		{
			internal TestReservedHdfsPaths(TestGlobPaths _enclosing, bool useFc)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			internal override void Run()
			{
				string reservedRoot = "/.reserved/.inodes/" + INodeId.RootInodeId;
				NUnit.Framework.Assert.AreEqual(reservedRoot, TestPath.MergeStatuses(this.wrap.GlobStatus
					(new Path(reservedRoot), new TestGlobPaths.AcceptAllPathFilter())));
				// These inodes don't show up via listStatus.
				NUnit.Framework.Assert.AreEqual(string.Empty, TestPath.MergeStatuses(this.wrap.GlobStatus
					(new Path("/.reserved/*"), new TestGlobPaths.AcceptAllPathFilter())));
			}

			private readonly TestGlobPaths _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReservedHdfsPathsOnFS()
		{
			TestOnFileSystem(new TestGlobPaths.TestReservedHdfsPaths(this, false));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReservedHdfsPathsOnFC()
		{
			TestOnFileContext(new TestGlobPaths.TestReservedHdfsPaths(this, true));
		}

		/// <summary>Test trying to glob the root.</summary>
		/// <remarks>Test trying to glob the root.  Regression test for HDFS-5888.</remarks>
		private class TestGlobRoot : TestGlobPaths.FSTestWrapperGlobTest
		{
			internal TestGlobRoot(TestGlobPaths _enclosing, bool useFc)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			internal override void Run()
			{
				Path rootPath = new Path("/");
				FileStatus oldRootStatus = this.wrap.GetFileStatus(rootPath);
				string newOwner = UUID.RandomUUID().ToString();
				this.privWrap.SetOwner(new Path("/"), newOwner, null);
				FileStatus[] status = this.wrap.GlobStatus(rootPath, new TestGlobPaths.AcceptAllPathFilter
					());
				NUnit.Framework.Assert.AreEqual(1, status.Length);
				NUnit.Framework.Assert.AreEqual(newOwner, status[0].GetOwner());
				this.privWrap.SetOwner(new Path("/"), oldRootStatus.GetOwner(), null);
			}

			private readonly TestGlobPaths _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGlobRootOnFS()
		{
			TestOnFileSystem(new TestGlobPaths.TestGlobRoot(this, false));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGlobRootOnFC()
		{
			TestOnFileContext(new TestGlobPaths.TestGlobRoot(this, true));
		}

		/// <summary>Test glob expressions that don't appear at the end of the path.</summary>
		/// <remarks>
		/// Test glob expressions that don't appear at the end of the path.  Regression
		/// test for HADOOP-10957.
		/// </remarks>
		private class TestNonTerminalGlobs : TestGlobPaths.FSTestWrapperGlobTest
		{
			internal TestNonTerminalGlobs(TestGlobPaths _enclosing, bool useFc)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			internal override void Run()
			{
				try
				{
					this.privWrap.Mkdir(new Path("/filed_away/alpha"), new FsPermission((short)0x1ff)
						, true);
					this.privWrap.CreateFile(new Path("/filed"), 0);
					FileStatus[] statuses = this.wrap.GlobStatus(new Path("/filed*/alpha"), new TestGlobPaths.AcceptAllPathFilter
						());
					NUnit.Framework.Assert.AreEqual(1, statuses.Length);
					NUnit.Framework.Assert.AreEqual("/filed_away/alpha", statuses[0].GetPath().ToUri(
						).GetPath());
					this.privWrap.Mkdir(new Path("/filed_away/alphabet"), new FsPermission((short)0x1ff
						), true);
					this.privWrap.Mkdir(new Path("/filed_away/alphabet/abc"), new FsPermission((short
						)0x1ff), true);
					statuses = this.wrap.GlobStatus(new Path("/filed*/alph*/*b*"), new TestGlobPaths.AcceptAllPathFilter
						());
					NUnit.Framework.Assert.AreEqual(1, statuses.Length);
					NUnit.Framework.Assert.AreEqual("/filed_away/alphabet/abc", statuses[0].GetPath()
						.ToUri().GetPath());
				}
				finally
				{
					this.privWrap.Delete(new Path("/filed"), true);
					this.privWrap.Delete(new Path("/filed_away"), true);
				}
			}

			private readonly TestGlobPaths _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNonTerminalGlobsOnFS()
		{
			TestOnFileSystem(new TestGlobPaths.TestNonTerminalGlobs(this, false));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNonTerminalGlobsOnFC()
		{
			TestOnFileContext(new TestGlobPaths.TestNonTerminalGlobs(this, true));
		}
	}
}
