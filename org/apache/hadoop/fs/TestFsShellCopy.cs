using Sharpen;

namespace org.apache.hadoop.fs
{
	public class TestFsShellCopy
	{
		internal static org.apache.hadoop.conf.Configuration conf;

		internal static org.apache.hadoop.fs.FsShell shell;

		internal static org.apache.hadoop.fs.LocalFileSystem lfs;

		internal static org.apache.hadoop.fs.Path testRootDir;

		internal static org.apache.hadoop.fs.Path srcPath;

		internal static org.apache.hadoop.fs.Path dstPath;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.BeforeClass]
		public static void setup()
		{
			conf = new org.apache.hadoop.conf.Configuration();
			shell = new org.apache.hadoop.fs.FsShell(conf);
			lfs = org.apache.hadoop.fs.FileSystem.getLocal(conf);
			testRootDir = lfs.makeQualified(new org.apache.hadoop.fs.Path(Sharpen.Runtime.getProperty
				("test.build.data", "test/build/data"), "testShellCopy"));
			lfs.mkdirs(testRootDir);
			srcPath = new org.apache.hadoop.fs.Path(testRootDir, "srcFile");
			dstPath = new org.apache.hadoop.fs.Path(testRootDir, "dstFile");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void prepFiles()
		{
			lfs.setVerifyChecksum(true);
			lfs.setWriteChecksum(true);
			lfs.delete(srcPath, true);
			lfs.delete(dstPath, true);
			org.apache.hadoop.fs.FSDataOutputStream @out = lfs.create(srcPath);
			@out.writeChars("hi");
			@out.close();
			NUnit.Framework.Assert.IsTrue(lfs.exists(lfs.getChecksumFile(srcPath)));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCopyNoCrc()
		{
			shellRun(0, "-get", srcPath.ToString(), dstPath.ToString());
			checkPath(dstPath, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCopyCrc()
		{
			shellRun(0, "-get", "-crc", srcPath.ToString(), dstPath.ToString());
			checkPath(dstPath, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCorruptedCopyCrc()
		{
			org.apache.hadoop.fs.FSDataOutputStream @out = lfs.getRawFileSystem().create(srcPath
				);
			@out.writeChars("bang");
			@out.close();
			shellRun(1, "-get", srcPath.ToString(), dstPath.ToString());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCorruptedCopyIgnoreCrc()
		{
			shellRun(0, "-get", "-ignoreCrc", srcPath.ToString(), dstPath.ToString());
			checkPath(dstPath, false);
		}

		/// <exception cref="System.IO.IOException"/>
		private void checkPath(org.apache.hadoop.fs.Path p, bool expectChecksum)
		{
			NUnit.Framework.Assert.IsTrue(lfs.exists(p));
			bool hasChecksum = lfs.exists(lfs.getChecksumFile(p));
			NUnit.Framework.Assert.AreEqual(expectChecksum, hasChecksum);
		}

		/// <exception cref="System.Exception"/>
		private void shellRun(int n, params string[] args)
		{
			NUnit.Framework.Assert.AreEqual(n, shell.run(args));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCopyFileFromLocal()
		{
			org.apache.hadoop.fs.Path testRoot = new org.apache.hadoop.fs.Path(testRootDir, "testPutFile"
				);
			lfs.delete(testRoot, true);
			lfs.mkdirs(testRoot);
			org.apache.hadoop.fs.Path targetDir = new org.apache.hadoop.fs.Path(testRoot, "target"
				);
			org.apache.hadoop.fs.Path filePath = new org.apache.hadoop.fs.Path(testRoot, new 
				org.apache.hadoop.fs.Path("srcFile"));
			lfs.create(filePath).close();
			checkPut(filePath, targetDir, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCopyDirFromLocal()
		{
			org.apache.hadoop.fs.Path testRoot = new org.apache.hadoop.fs.Path(testRootDir, "testPutDir"
				);
			lfs.delete(testRoot, true);
			lfs.mkdirs(testRoot);
			org.apache.hadoop.fs.Path targetDir = new org.apache.hadoop.fs.Path(testRoot, "target"
				);
			org.apache.hadoop.fs.Path dirPath = new org.apache.hadoop.fs.Path(testRoot, new org.apache.hadoop.fs.Path
				("srcDir"));
			lfs.mkdirs(dirPath);
			lfs.create(new org.apache.hadoop.fs.Path(dirPath, "srcFile")).close();
			checkPut(dirPath, targetDir, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCopyFileFromWindowsLocalPath()
		{
			NUnit.Framework.Assume.assumeTrue(org.apache.hadoop.fs.Path.WINDOWS);
			string windowsTestRootPath = (new java.io.File(testRootDir.toUri().getPath().ToString
				())).getAbsolutePath();
			org.apache.hadoop.fs.Path testRoot = new org.apache.hadoop.fs.Path(windowsTestRootPath
				, "testPutFile");
			lfs.delete(testRoot, true);
			lfs.mkdirs(testRoot);
			org.apache.hadoop.fs.Path targetDir = new org.apache.hadoop.fs.Path(testRoot, "target"
				);
			org.apache.hadoop.fs.Path filePath = new org.apache.hadoop.fs.Path(testRoot, new 
				org.apache.hadoop.fs.Path("srcFile"));
			lfs.create(filePath).close();
			checkPut(filePath, targetDir, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCopyDirFromWindowsLocalPath()
		{
			NUnit.Framework.Assume.assumeTrue(org.apache.hadoop.fs.Path.WINDOWS);
			string windowsTestRootPath = (new java.io.File(testRootDir.toUri().getPath().ToString
				())).getAbsolutePath();
			org.apache.hadoop.fs.Path testRoot = new org.apache.hadoop.fs.Path(windowsTestRootPath
				, "testPutDir");
			lfs.delete(testRoot, true);
			lfs.mkdirs(testRoot);
			org.apache.hadoop.fs.Path targetDir = new org.apache.hadoop.fs.Path(testRoot, "target"
				);
			org.apache.hadoop.fs.Path dirPath = new org.apache.hadoop.fs.Path(testRoot, new org.apache.hadoop.fs.Path
				("srcDir"));
			lfs.mkdirs(dirPath);
			lfs.create(new org.apache.hadoop.fs.Path(dirPath, "srcFile")).close();
			checkPut(dirPath, targetDir, true);
		}

		/// <exception cref="System.Exception"/>
		private void checkPut(org.apache.hadoop.fs.Path srcPath, org.apache.hadoop.fs.Path
			 targetDir, bool useWindowsPath)
		{
			lfs.delete(targetDir, true);
			lfs.mkdirs(targetDir);
			lfs.setWorkingDirectory(targetDir);
			org.apache.hadoop.fs.Path dstPath = new org.apache.hadoop.fs.Path("path");
			org.apache.hadoop.fs.Path childPath = new org.apache.hadoop.fs.Path(dstPath, "childPath"
				);
			lfs.setWorkingDirectory(targetDir);
			// copy to new file, then again
			prepPut(dstPath, false, false);
			checkPut(0, srcPath, dstPath, useWindowsPath);
			if (lfs.isFile(srcPath))
			{
				checkPut(1, srcPath, dstPath, useWindowsPath);
			}
			else
			{
				// directory works because it copies into the dir
				// clear contents so the check won't think there are extra paths
				prepPut(dstPath, true, true);
				checkPut(0, srcPath, dstPath, useWindowsPath);
			}
			// copy to non-existent subdir
			prepPut(childPath, false, false);
			checkPut(1, srcPath, dstPath, useWindowsPath);
			// copy into dir, then with another name
			prepPut(dstPath, true, true);
			checkPut(0, srcPath, dstPath, useWindowsPath);
			prepPut(childPath, true, true);
			checkPut(0, srcPath, childPath, useWindowsPath);
			// try to put to pwd with existing dir
			prepPut(targetDir, true, true);
			checkPut(0, srcPath, null, useWindowsPath);
			prepPut(targetDir, true, true);
			checkPut(0, srcPath, new org.apache.hadoop.fs.Path("."), useWindowsPath);
			// try to put to pwd with non-existent cwd
			prepPut(dstPath, false, true);
			lfs.setWorkingDirectory(dstPath);
			checkPut(1, srcPath, null, useWindowsPath);
			prepPut(dstPath, false, true);
			checkPut(1, srcPath, new org.apache.hadoop.fs.Path("."), useWindowsPath);
		}

		/// <exception cref="System.IO.IOException"/>
		private void prepPut(org.apache.hadoop.fs.Path dst, bool create, bool isDir)
		{
			lfs.delete(dst, true);
			NUnit.Framework.Assert.IsFalse(lfs.exists(dst));
			if (create)
			{
				if (isDir)
				{
					lfs.mkdirs(dst);
					NUnit.Framework.Assert.IsTrue(lfs.isDirectory(dst));
				}
				else
				{
					lfs.mkdirs(new org.apache.hadoop.fs.Path(dst.getName()));
					lfs.create(dst).close();
					NUnit.Framework.Assert.IsTrue(lfs.isFile(dst));
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private void checkPut(int exitCode, org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
			 dest, bool useWindowsPath)
		{
			string[] argv = null;
			string srcPath = src.ToString();
			if (useWindowsPath)
			{
				srcPath = (new java.io.File(srcPath)).getAbsolutePath();
			}
			if (dest != null)
			{
				argv = new string[] { "-put", srcPath, pathAsString(dest) };
			}
			else
			{
				argv = new string[] { "-put", srcPath };
				dest = new org.apache.hadoop.fs.Path(org.apache.hadoop.fs.Path.CUR_DIR);
			}
			org.apache.hadoop.fs.Path target;
			if (lfs.exists(dest))
			{
				if (lfs.isDirectory(dest))
				{
					target = new org.apache.hadoop.fs.Path(pathAsString(dest), src.getName());
				}
				else
				{
					target = dest;
				}
			}
			else
			{
				target = new org.apache.hadoop.fs.Path(lfs.getWorkingDirectory(), dest);
			}
			bool targetExists = lfs.exists(target);
			org.apache.hadoop.fs.Path parent = lfs.makeQualified(target).getParent();
			System.Console.Out.WriteLine("COPY src[" + src.getName() + "] -> [" + dest + "] as ["
				 + target + "]");
			string[] lsArgv = new string[] { "-ls", "-R", pathAsString(parent) };
			shell.run(lsArgv);
			int gotExit = shell.run(argv);
			System.Console.Out.WriteLine("copy exit:" + gotExit);
			lsArgv = new string[] { "-ls", "-R", pathAsString(parent) };
			shell.run(lsArgv);
			if (exitCode == 0)
			{
				NUnit.Framework.Assert.IsTrue(lfs.exists(target));
				NUnit.Framework.Assert.IsTrue(lfs.isFile(src) == lfs.isFile(target));
				NUnit.Framework.Assert.AreEqual(1, lfs.listStatus(lfs.makeQualified(target).getParent
					()).Length);
			}
			else
			{
				NUnit.Framework.Assert.AreEqual(targetExists, lfs.exists(target));
			}
			NUnit.Framework.Assert.AreEqual(exitCode, gotExit);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRepresentsDir()
		{
			org.apache.hadoop.fs.Path subdirDstPath = new org.apache.hadoop.fs.Path(dstPath, 
				srcPath.getName());
			string[] argv = null;
			lfs.delete(dstPath, true);
			NUnit.Framework.Assert.IsFalse(lfs.exists(dstPath));
			argv = new string[] { "-put", srcPath.ToString(), dstPath.ToString() };
			NUnit.Framework.Assert.AreEqual(0, shell.run(argv));
			NUnit.Framework.Assert.IsTrue(lfs.exists(dstPath) && lfs.isFile(dstPath));
			lfs.delete(dstPath, true);
			NUnit.Framework.Assert.IsFalse(lfs.exists(dstPath));
			// since dst path looks like a dir, it should not copy the file and
			// rename it to what looks like a directory
			lfs.delete(dstPath, true);
			// make copy fail
			foreach (string suffix in new string[] { "/", "/." })
			{
				argv = new string[] { "-put", srcPath.ToString(), dstPath.ToString() + suffix };
				NUnit.Framework.Assert.AreEqual(1, shell.run(argv));
				NUnit.Framework.Assert.IsFalse(lfs.exists(dstPath));
				NUnit.Framework.Assert.IsFalse(lfs.exists(subdirDstPath));
			}
			// since dst path looks like a dir, it should not copy the file and
			// rename it to what looks like a directory
			foreach (string suffix_1 in new string[] { "/", "/." })
			{
				// empty out the directory and create to make copy succeed
				lfs.delete(dstPath, true);
				lfs.mkdirs(dstPath);
				argv = new string[] { "-put", srcPath.ToString(), dstPath.ToString() + suffix_1 };
				NUnit.Framework.Assert.AreEqual(0, shell.run(argv));
				NUnit.Framework.Assert.IsTrue(lfs.exists(subdirDstPath));
				NUnit.Framework.Assert.IsTrue(lfs.isFile(subdirDstPath));
			}
			// ensure .. is interpreted as a dir
			string dotdotDst = dstPath + "/foo/..";
			lfs.delete(dstPath, true);
			lfs.mkdirs(new org.apache.hadoop.fs.Path(dstPath, "foo"));
			argv = new string[] { "-put", srcPath.ToString(), dotdotDst };
			NUnit.Framework.Assert.AreEqual(0, shell.run(argv));
			NUnit.Framework.Assert.IsTrue(lfs.exists(subdirDstPath));
			NUnit.Framework.Assert.IsTrue(lfs.isFile(subdirDstPath));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCopyMerge()
		{
			org.apache.hadoop.fs.Path root = new org.apache.hadoop.fs.Path(testRootDir, "TestMerge"
				);
			org.apache.hadoop.fs.Path f1 = new org.apache.hadoop.fs.Path(root, "f1");
			org.apache.hadoop.fs.Path f2 = new org.apache.hadoop.fs.Path(root, "f2");
			org.apache.hadoop.fs.Path f3 = new org.apache.hadoop.fs.Path(root, "f3");
			org.apache.hadoop.fs.Path fnf = new org.apache.hadoop.fs.Path(root, "fnf");
			org.apache.hadoop.fs.Path d = new org.apache.hadoop.fs.Path(root, "dir");
			org.apache.hadoop.fs.Path df1 = new org.apache.hadoop.fs.Path(d, "df1");
			org.apache.hadoop.fs.Path df2 = new org.apache.hadoop.fs.Path(d, "df2");
			org.apache.hadoop.fs.Path df3 = new org.apache.hadoop.fs.Path(d, "df3");
			createFile(f1, f2, f3, df1, df2, df3);
			int exit;
			// one file, kind of silly
			exit = shell.run(new string[] { "-getmerge", f1.ToString(), "out" });
			NUnit.Framework.Assert.AreEqual(0, exit);
			NUnit.Framework.Assert.AreEqual("f1", readFile("out"));
			exit = shell.run(new string[] { "-getmerge", fnf.ToString(), "out" });
			NUnit.Framework.Assert.AreEqual(1, exit);
			NUnit.Framework.Assert.IsFalse(lfs.exists(new org.apache.hadoop.fs.Path("out")));
			// two files
			exit = shell.run(new string[] { "-getmerge", f1.ToString(), f2.ToString(), "out" }
				);
			NUnit.Framework.Assert.AreEqual(0, exit);
			NUnit.Framework.Assert.AreEqual("f1f2", readFile("out"));
			// two files, preserves order
			exit = shell.run(new string[] { "-getmerge", f2.ToString(), f1.ToString(), "out" }
				);
			NUnit.Framework.Assert.AreEqual(0, exit);
			NUnit.Framework.Assert.AreEqual("f2f1", readFile("out"));
			// two files
			exit = shell.run(new string[] { "-getmerge", "-nl", f1.ToString(), f2.ToString(), 
				"out" });
			NUnit.Framework.Assert.AreEqual(0, exit);
			NUnit.Framework.Assert.AreEqual("f1\nf2\n", readFile("out"));
			// glob three files
			shell.run(new string[] { "-getmerge", "-nl", new org.apache.hadoop.fs.Path(root, 
				"f*").ToString(), "out" });
			NUnit.Framework.Assert.AreEqual(0, exit);
			NUnit.Framework.Assert.AreEqual("f1\nf2\nf3\n", readFile("out"));
			// directory with 3 files, should skip subdir
			shell.run(new string[] { "-getmerge", "-nl", root.ToString(), "out" });
			NUnit.Framework.Assert.AreEqual(0, exit);
			NUnit.Framework.Assert.AreEqual("f1\nf2\nf3\n", readFile("out"));
			// subdir
			shell.run(new string[] { "-getmerge", "-nl", d.ToString(), "out" });
			NUnit.Framework.Assert.AreEqual(0, exit);
			NUnit.Framework.Assert.AreEqual("df1\ndf2\ndf3\n", readFile("out"));
			// file, dir, file
			shell.run(new string[] { "-getmerge", "-nl", f1.ToString(), d.ToString(), f2.ToString
				(), "out" });
			NUnit.Framework.Assert.AreEqual(0, exit);
			NUnit.Framework.Assert.AreEqual("f1\ndf1\ndf2\ndf3\nf2\n", readFile("out"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testMoveFileFromLocal()
		{
			org.apache.hadoop.fs.Path testRoot = new org.apache.hadoop.fs.Path(testRootDir, "testPutFile"
				);
			lfs.delete(testRoot, true);
			lfs.mkdirs(testRoot);
			org.apache.hadoop.fs.Path target = new org.apache.hadoop.fs.Path(testRoot, "target"
				);
			org.apache.hadoop.fs.Path srcFile = new org.apache.hadoop.fs.Path(testRoot, new org.apache.hadoop.fs.Path
				("srcFile"));
			lfs.createNewFile(srcFile);
			int exit = shell.run(new string[] { "-moveFromLocal", srcFile.ToString(), target.
				ToString() });
			NUnit.Framework.Assert.AreEqual(0, exit);
			NUnit.Framework.Assert.IsFalse(lfs.exists(srcFile));
			NUnit.Framework.Assert.IsTrue(lfs.exists(target));
			NUnit.Framework.Assert.IsTrue(lfs.isFile(target));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testMoveDirFromLocal()
		{
			org.apache.hadoop.fs.Path testRoot = new org.apache.hadoop.fs.Path(testRootDir, "testPutDir"
				);
			lfs.delete(testRoot, true);
			lfs.mkdirs(testRoot);
			org.apache.hadoop.fs.Path srcDir = new org.apache.hadoop.fs.Path(testRoot, "srcDir"
				);
			lfs.mkdirs(srcDir);
			org.apache.hadoop.fs.Path targetDir = new org.apache.hadoop.fs.Path(testRoot, "target"
				);
			int exit = shell.run(new string[] { "-moveFromLocal", srcDir.ToString(), targetDir
				.ToString() });
			NUnit.Framework.Assert.AreEqual(0, exit);
			NUnit.Framework.Assert.IsFalse(lfs.exists(srcDir));
			NUnit.Framework.Assert.IsTrue(lfs.exists(targetDir));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testMoveDirFromLocalDestExists()
		{
			org.apache.hadoop.fs.Path testRoot = new org.apache.hadoop.fs.Path(testRootDir, "testPutDir"
				);
			lfs.delete(testRoot, true);
			lfs.mkdirs(testRoot);
			org.apache.hadoop.fs.Path srcDir = new org.apache.hadoop.fs.Path(testRoot, "srcDir"
				);
			lfs.mkdirs(srcDir);
			org.apache.hadoop.fs.Path targetDir = new org.apache.hadoop.fs.Path(testRoot, "target"
				);
			lfs.mkdirs(targetDir);
			int exit = shell.run(new string[] { "-moveFromLocal", srcDir.ToString(), targetDir
				.ToString() });
			NUnit.Framework.Assert.AreEqual(0, exit);
			NUnit.Framework.Assert.IsFalse(lfs.exists(srcDir));
			NUnit.Framework.Assert.IsTrue(lfs.exists(new org.apache.hadoop.fs.Path(targetDir, 
				srcDir.getName())));
			lfs.mkdirs(srcDir);
			exit = shell.run(new string[] { "-moveFromLocal", srcDir.ToString(), targetDir.ToString
				() });
			NUnit.Framework.Assert.AreEqual(1, exit);
			NUnit.Framework.Assert.IsTrue(lfs.exists(srcDir));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testMoveFromWindowsLocalPath()
		{
			NUnit.Framework.Assume.assumeTrue(org.apache.hadoop.fs.Path.WINDOWS);
			org.apache.hadoop.fs.Path testRoot = new org.apache.hadoop.fs.Path(testRootDir, "testPutFile"
				);
			lfs.delete(testRoot, true);
			lfs.mkdirs(testRoot);
			org.apache.hadoop.fs.Path target = new org.apache.hadoop.fs.Path(testRoot, "target"
				);
			org.apache.hadoop.fs.Path srcFile = new org.apache.hadoop.fs.Path(testRoot, new org.apache.hadoop.fs.Path
				("srcFile"));
			lfs.createNewFile(srcFile);
			string winSrcFile = (new java.io.File(srcFile.toUri().getPath().ToString())).getAbsolutePath
				();
			shellRun(0, "-moveFromLocal", winSrcFile, target.ToString());
			NUnit.Framework.Assert.IsFalse(lfs.exists(srcFile));
			NUnit.Framework.Assert.IsTrue(lfs.exists(target));
			NUnit.Framework.Assert.IsTrue(lfs.isFile(target));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGetWindowsLocalPath()
		{
			NUnit.Framework.Assume.assumeTrue(org.apache.hadoop.fs.Path.WINDOWS);
			string winDstFile = (new java.io.File(dstPath.toUri().getPath().ToString())).getAbsolutePath
				();
			shellRun(0, "-get", srcPath.ToString(), winDstFile);
			checkPath(dstPath, false);
		}

		/// <exception cref="System.IO.IOException"/>
		private void createFile(params org.apache.hadoop.fs.Path[] paths)
		{
			foreach (org.apache.hadoop.fs.Path path in paths)
			{
				org.apache.hadoop.fs.FSDataOutputStream @out = lfs.create(path);
				@out.write(Sharpen.Runtime.getBytesForString(path.getName()));
				@out.close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private string readFile(string @out)
		{
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(@out);
			org.apache.hadoop.fs.FileStatus stat = lfs.getFileStatus(path);
			org.apache.hadoop.fs.FSDataInputStream @in = lfs.open(path);
			byte[] buffer = new byte[(int)stat.getLen()];
			@in.readFully(buffer);
			@in.close();
			lfs.delete(path, false);
			return Sharpen.Runtime.getStringForBytes(buffer);
		}

		// path handles "." rather oddly
		private string pathAsString(org.apache.hadoop.fs.Path p)
		{
			string s = (p == null) ? org.apache.hadoop.fs.Path.CUR_DIR : p.ToString();
			return s.isEmpty() ? org.apache.hadoop.fs.Path.CUR_DIR : s;
		}
	}
}
