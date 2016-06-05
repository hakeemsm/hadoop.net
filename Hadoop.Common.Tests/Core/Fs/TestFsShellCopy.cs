using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	public class TestFsShellCopy
	{
		internal static Configuration conf;

		internal static FsShell shell;

		internal static LocalFileSystem lfs;

		internal static Path testRootDir;

		internal static Path srcPath;

		internal static Path dstPath;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void Setup()
		{
			conf = new Configuration();
			shell = new FsShell(conf);
			lfs = FileSystem.GetLocal(conf);
			testRootDir = lfs.MakeQualified(new Path(Runtime.GetProperty("test.build.data", "test/build/data"
				), "testShellCopy"));
			lfs.Mkdirs(testRootDir);
			srcPath = new Path(testRootDir, "srcFile");
			dstPath = new Path(testRootDir, "dstFile");
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void PrepFiles()
		{
			lfs.SetVerifyChecksum(true);
			lfs.SetWriteChecksum(true);
			lfs.Delete(srcPath, true);
			lfs.Delete(dstPath, true);
			FSDataOutputStream @out = lfs.Create(srcPath);
			@out.WriteChars("hi");
			@out.Close();
			Assert.True(lfs.Exists(lfs.GetChecksumFile(srcPath)));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCopyNoCrc()
		{
			ShellRun(0, "-get", srcPath.ToString(), dstPath.ToString());
			CheckPath(dstPath, false);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCopyCrc()
		{
			ShellRun(0, "-get", "-crc", srcPath.ToString(), dstPath.ToString());
			CheckPath(dstPath, true);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCorruptedCopyCrc()
		{
			FSDataOutputStream @out = lfs.GetRawFileSystem().Create(srcPath);
			@out.WriteChars("bang");
			@out.Close();
			ShellRun(1, "-get", srcPath.ToString(), dstPath.ToString());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCorruptedCopyIgnoreCrc()
		{
			ShellRun(0, "-get", "-ignoreCrc", srcPath.ToString(), dstPath.ToString());
			CheckPath(dstPath, false);
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckPath(Path p, bool expectChecksum)
		{
			Assert.True(lfs.Exists(p));
			bool hasChecksum = lfs.Exists(lfs.GetChecksumFile(p));
			Assert.Equal(expectChecksum, hasChecksum);
		}

		/// <exception cref="System.Exception"/>
		private void ShellRun(int n, params string[] args)
		{
			Assert.Equal(n, shell.Run(args));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCopyFileFromLocal()
		{
			Path testRoot = new Path(testRootDir, "testPutFile");
			lfs.Delete(testRoot, true);
			lfs.Mkdirs(testRoot);
			Path targetDir = new Path(testRoot, "target");
			Path filePath = new Path(testRoot, new Path("srcFile"));
			lfs.Create(filePath).Close();
			CheckPut(filePath, targetDir, false);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCopyDirFromLocal()
		{
			Path testRoot = new Path(testRootDir, "testPutDir");
			lfs.Delete(testRoot, true);
			lfs.Mkdirs(testRoot);
			Path targetDir = new Path(testRoot, "target");
			Path dirPath = new Path(testRoot, new Path("srcDir"));
			lfs.Mkdirs(dirPath);
			lfs.Create(new Path(dirPath, "srcFile")).Close();
			CheckPut(dirPath, targetDir, false);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCopyFileFromWindowsLocalPath()
		{
			Assume.AssumeTrue(Path.Windows);
			string windowsTestRootPath = (new FilePath(testRootDir.ToUri().GetPath().ToString
				())).GetAbsolutePath();
			Path testRoot = new Path(windowsTestRootPath, "testPutFile");
			lfs.Delete(testRoot, true);
			lfs.Mkdirs(testRoot);
			Path targetDir = new Path(testRoot, "target");
			Path filePath = new Path(testRoot, new Path("srcFile"));
			lfs.Create(filePath).Close();
			CheckPut(filePath, targetDir, true);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCopyDirFromWindowsLocalPath()
		{
			Assume.AssumeTrue(Path.Windows);
			string windowsTestRootPath = (new FilePath(testRootDir.ToUri().GetPath().ToString
				())).GetAbsolutePath();
			Path testRoot = new Path(windowsTestRootPath, "testPutDir");
			lfs.Delete(testRoot, true);
			lfs.Mkdirs(testRoot);
			Path targetDir = new Path(testRoot, "target");
			Path dirPath = new Path(testRoot, new Path("srcDir"));
			lfs.Mkdirs(dirPath);
			lfs.Create(new Path(dirPath, "srcFile")).Close();
			CheckPut(dirPath, targetDir, true);
		}

		/// <exception cref="System.Exception"/>
		private void CheckPut(Path srcPath, Path targetDir, bool useWindowsPath)
		{
			lfs.Delete(targetDir, true);
			lfs.Mkdirs(targetDir);
			lfs.SetWorkingDirectory(targetDir);
			Path dstPath = new Path("path");
			Path childPath = new Path(dstPath, "childPath");
			lfs.SetWorkingDirectory(targetDir);
			// copy to new file, then again
			PrepPut(dstPath, false, false);
			CheckPut(0, srcPath, dstPath, useWindowsPath);
			if (lfs.IsFile(srcPath))
			{
				CheckPut(1, srcPath, dstPath, useWindowsPath);
			}
			else
			{
				// directory works because it copies into the dir
				// clear contents so the check won't think there are extra paths
				PrepPut(dstPath, true, true);
				CheckPut(0, srcPath, dstPath, useWindowsPath);
			}
			// copy to non-existent subdir
			PrepPut(childPath, false, false);
			CheckPut(1, srcPath, dstPath, useWindowsPath);
			// copy into dir, then with another name
			PrepPut(dstPath, true, true);
			CheckPut(0, srcPath, dstPath, useWindowsPath);
			PrepPut(childPath, true, true);
			CheckPut(0, srcPath, childPath, useWindowsPath);
			// try to put to pwd with existing dir
			PrepPut(targetDir, true, true);
			CheckPut(0, srcPath, null, useWindowsPath);
			PrepPut(targetDir, true, true);
			CheckPut(0, srcPath, new Path("."), useWindowsPath);
			// try to put to pwd with non-existent cwd
			PrepPut(dstPath, false, true);
			lfs.SetWorkingDirectory(dstPath);
			CheckPut(1, srcPath, null, useWindowsPath);
			PrepPut(dstPath, false, true);
			CheckPut(1, srcPath, new Path("."), useWindowsPath);
		}

		/// <exception cref="System.IO.IOException"/>
		private void PrepPut(Path dst, bool create, bool isDir)
		{
			lfs.Delete(dst, true);
			NUnit.Framework.Assert.IsFalse(lfs.Exists(dst));
			if (create)
			{
				if (isDir)
				{
					lfs.Mkdirs(dst);
					Assert.True(lfs.IsDirectory(dst));
				}
				else
				{
					lfs.Mkdirs(new Path(dst.GetName()));
					lfs.Create(dst).Close();
					Assert.True(lfs.IsFile(dst));
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private void CheckPut(int exitCode, Path src, Path dest, bool useWindowsPath)
		{
			string[] argv = null;
			string srcPath = src.ToString();
			if (useWindowsPath)
			{
				srcPath = (new FilePath(srcPath)).GetAbsolutePath();
			}
			if (dest != null)
			{
				argv = new string[] { "-put", srcPath, PathAsString(dest) };
			}
			else
			{
				argv = new string[] { "-put", srcPath };
				dest = new Path(Path.CurDir);
			}
			Path target;
			if (lfs.Exists(dest))
			{
				if (lfs.IsDirectory(dest))
				{
					target = new Path(PathAsString(dest), src.GetName());
				}
				else
				{
					target = dest;
				}
			}
			else
			{
				target = new Path(lfs.GetWorkingDirectory(), dest);
			}
			bool targetExists = lfs.Exists(target);
			Path parent = lfs.MakeQualified(target).GetParent();
			System.Console.Out.WriteLine("COPY src[" + src.GetName() + "] -> [" + dest + "] as ["
				 + target + "]");
			string[] lsArgv = new string[] { "-ls", "-R", PathAsString(parent) };
			shell.Run(lsArgv);
			int gotExit = shell.Run(argv);
			System.Console.Out.WriteLine("copy exit:" + gotExit);
			lsArgv = new string[] { "-ls", "-R", PathAsString(parent) };
			shell.Run(lsArgv);
			if (exitCode == 0)
			{
				Assert.True(lfs.Exists(target));
				Assert.True(lfs.IsFile(src) == lfs.IsFile(target));
				Assert.Equal(1, lfs.ListStatus(lfs.MakeQualified(target).GetParent
					()).Length);
			}
			else
			{
				Assert.Equal(targetExists, lfs.Exists(target));
			}
			Assert.Equal(exitCode, gotExit);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRepresentsDir()
		{
			Path subdirDstPath = new Path(dstPath, srcPath.GetName());
			string[] argv = null;
			lfs.Delete(dstPath, true);
			NUnit.Framework.Assert.IsFalse(lfs.Exists(dstPath));
			argv = new string[] { "-put", srcPath.ToString(), dstPath.ToString() };
			Assert.Equal(0, shell.Run(argv));
			Assert.True(lfs.Exists(dstPath) && lfs.IsFile(dstPath));
			lfs.Delete(dstPath, true);
			NUnit.Framework.Assert.IsFalse(lfs.Exists(dstPath));
			// since dst path looks like a dir, it should not copy the file and
			// rename it to what looks like a directory
			lfs.Delete(dstPath, true);
			// make copy fail
			foreach (string suffix in new string[] { "/", "/." })
			{
				argv = new string[] { "-put", srcPath.ToString(), dstPath.ToString() + suffix };
				Assert.Equal(1, shell.Run(argv));
				NUnit.Framework.Assert.IsFalse(lfs.Exists(dstPath));
				NUnit.Framework.Assert.IsFalse(lfs.Exists(subdirDstPath));
			}
			// since dst path looks like a dir, it should not copy the file and
			// rename it to what looks like a directory
			foreach (string suffix_1 in new string[] { "/", "/." })
			{
				// empty out the directory and create to make copy succeed
				lfs.Delete(dstPath, true);
				lfs.Mkdirs(dstPath);
				argv = new string[] { "-put", srcPath.ToString(), dstPath.ToString() + suffix_1 };
				Assert.Equal(0, shell.Run(argv));
				Assert.True(lfs.Exists(subdirDstPath));
				Assert.True(lfs.IsFile(subdirDstPath));
			}
			// ensure .. is interpreted as a dir
			string dotdotDst = dstPath + "/foo/..";
			lfs.Delete(dstPath, true);
			lfs.Mkdirs(new Path(dstPath, "foo"));
			argv = new string[] { "-put", srcPath.ToString(), dotdotDst };
			Assert.Equal(0, shell.Run(argv));
			Assert.True(lfs.Exists(subdirDstPath));
			Assert.True(lfs.IsFile(subdirDstPath));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCopyMerge()
		{
			Path root = new Path(testRootDir, "TestMerge");
			Path f1 = new Path(root, "f1");
			Path f2 = new Path(root, "f2");
			Path f3 = new Path(root, "f3");
			Path fnf = new Path(root, "fnf");
			Path d = new Path(root, "dir");
			Path df1 = new Path(d, "df1");
			Path df2 = new Path(d, "df2");
			Path df3 = new Path(d, "df3");
			CreateFile(f1, f2, f3, df1, df2, df3);
			int exit;
			// one file, kind of silly
			exit = shell.Run(new string[] { "-getmerge", f1.ToString(), "out" });
			Assert.Equal(0, exit);
			Assert.Equal("f1", ReadFile("out"));
			exit = shell.Run(new string[] { "-getmerge", fnf.ToString(), "out" });
			Assert.Equal(1, exit);
			NUnit.Framework.Assert.IsFalse(lfs.Exists(new Path("out")));
			// two files
			exit = shell.Run(new string[] { "-getmerge", f1.ToString(), f2.ToString(), "out" }
				);
			Assert.Equal(0, exit);
			Assert.Equal("f1f2", ReadFile("out"));
			// two files, preserves order
			exit = shell.Run(new string[] { "-getmerge", f2.ToString(), f1.ToString(), "out" }
				);
			Assert.Equal(0, exit);
			Assert.Equal("f2f1", ReadFile("out"));
			// two files
			exit = shell.Run(new string[] { "-getmerge", "-nl", f1.ToString(), f2.ToString(), 
				"out" });
			Assert.Equal(0, exit);
			Assert.Equal("f1\nf2\n", ReadFile("out"));
			// glob three files
			shell.Run(new string[] { "-getmerge", "-nl", new Path(root, "f*").ToString(), "out"
				 });
			Assert.Equal(0, exit);
			Assert.Equal("f1\nf2\nf3\n", ReadFile("out"));
			// directory with 3 files, should skip subdir
			shell.Run(new string[] { "-getmerge", "-nl", root.ToString(), "out" });
			Assert.Equal(0, exit);
			Assert.Equal("f1\nf2\nf3\n", ReadFile("out"));
			// subdir
			shell.Run(new string[] { "-getmerge", "-nl", d.ToString(), "out" });
			Assert.Equal(0, exit);
			Assert.Equal("df1\ndf2\ndf3\n", ReadFile("out"));
			// file, dir, file
			shell.Run(new string[] { "-getmerge", "-nl", f1.ToString(), d.ToString(), f2.ToString
				(), "out" });
			Assert.Equal(0, exit);
			Assert.Equal("f1\ndf1\ndf2\ndf3\nf2\n", ReadFile("out"));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestMoveFileFromLocal()
		{
			Path testRoot = new Path(testRootDir, "testPutFile");
			lfs.Delete(testRoot, true);
			lfs.Mkdirs(testRoot);
			Path target = new Path(testRoot, "target");
			Path srcFile = new Path(testRoot, new Path("srcFile"));
			lfs.CreateNewFile(srcFile);
			int exit = shell.Run(new string[] { "-moveFromLocal", srcFile.ToString(), target.
				ToString() });
			Assert.Equal(0, exit);
			NUnit.Framework.Assert.IsFalse(lfs.Exists(srcFile));
			Assert.True(lfs.Exists(target));
			Assert.True(lfs.IsFile(target));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestMoveDirFromLocal()
		{
			Path testRoot = new Path(testRootDir, "testPutDir");
			lfs.Delete(testRoot, true);
			lfs.Mkdirs(testRoot);
			Path srcDir = new Path(testRoot, "srcDir");
			lfs.Mkdirs(srcDir);
			Path targetDir = new Path(testRoot, "target");
			int exit = shell.Run(new string[] { "-moveFromLocal", srcDir.ToString(), targetDir
				.ToString() });
			Assert.Equal(0, exit);
			NUnit.Framework.Assert.IsFalse(lfs.Exists(srcDir));
			Assert.True(lfs.Exists(targetDir));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestMoveDirFromLocalDestExists()
		{
			Path testRoot = new Path(testRootDir, "testPutDir");
			lfs.Delete(testRoot, true);
			lfs.Mkdirs(testRoot);
			Path srcDir = new Path(testRoot, "srcDir");
			lfs.Mkdirs(srcDir);
			Path targetDir = new Path(testRoot, "target");
			lfs.Mkdirs(targetDir);
			int exit = shell.Run(new string[] { "-moveFromLocal", srcDir.ToString(), targetDir
				.ToString() });
			Assert.Equal(0, exit);
			NUnit.Framework.Assert.IsFalse(lfs.Exists(srcDir));
			Assert.True(lfs.Exists(new Path(targetDir, srcDir.GetName())));
			lfs.Mkdirs(srcDir);
			exit = shell.Run(new string[] { "-moveFromLocal", srcDir.ToString(), targetDir.ToString
				() });
			Assert.Equal(1, exit);
			Assert.True(lfs.Exists(srcDir));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestMoveFromWindowsLocalPath()
		{
			Assume.AssumeTrue(Path.Windows);
			Path testRoot = new Path(testRootDir, "testPutFile");
			lfs.Delete(testRoot, true);
			lfs.Mkdirs(testRoot);
			Path target = new Path(testRoot, "target");
			Path srcFile = new Path(testRoot, new Path("srcFile"));
			lfs.CreateNewFile(srcFile);
			string winSrcFile = (new FilePath(srcFile.ToUri().GetPath().ToString())).GetAbsolutePath
				();
			ShellRun(0, "-moveFromLocal", winSrcFile, target.ToString());
			NUnit.Framework.Assert.IsFalse(lfs.Exists(srcFile));
			Assert.True(lfs.Exists(target));
			Assert.True(lfs.IsFile(target));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGetWindowsLocalPath()
		{
			Assume.AssumeTrue(Path.Windows);
			string winDstFile = (new FilePath(dstPath.ToUri().GetPath().ToString())).GetAbsolutePath
				();
			ShellRun(0, "-get", srcPath.ToString(), winDstFile);
			CheckPath(dstPath, false);
		}

		/// <exception cref="System.IO.IOException"/>
		private void CreateFile(params Path[] paths)
		{
			foreach (Path path in paths)
			{
				FSDataOutputStream @out = lfs.Create(path);
				@out.Write(Sharpen.Runtime.GetBytesForString(path.GetName()));
				@out.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private string ReadFile(string @out)
		{
			Path path = new Path(@out);
			FileStatus stat = lfs.GetFileStatus(path);
			FSDataInputStream @in = lfs.Open(path);
			byte[] buffer = new byte[(int)stat.GetLen()];
			@in.ReadFully(buffer);
			@in.Close();
			lfs.Delete(path, false);
			return Sharpen.Runtime.GetStringForBytes(buffer);
		}

		// path handles "." rather oddly
		private string PathAsString(Path p)
		{
			string s = (p == null) ? Path.CurDir : p.ToString();
			return s.IsEmpty() ? Path.CurDir : s;
		}
	}
}
