using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Hdfs.Tools;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Org.Hamcrest;
using Org.Hamcrest.Core;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>This class tests commands from DFSShell.</summary>
	public class TestDFSShell
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.TestDFSShell
			));

		private static readonly AtomicInteger counter = new AtomicInteger();

		private readonly int Success = 0;

		private readonly int Error = 1;

		internal static readonly string TestRootDir = PathUtils.GetTestDirName(typeof(Org.Apache.Hadoop.Hdfs.TestDFSShell
			));

		private const string RawA1 = "raw.a1";

		private const string TrustedA1 = "trusted.a1";

		private const string UserA1 = "user.a1";

		private static readonly byte[] RawA1Value = new byte[] { unchecked((int)(0x32)), 
			unchecked((int)(0x32)), unchecked((int)(0x32)) };

		private static readonly byte[] TrustedA1Value = new byte[] { unchecked((int)(0x31
			)), unchecked((int)(0x31)), unchecked((int)(0x31)) };

		private static readonly byte[] UserA1Value = new byte[] { unchecked((int)(0x31)), 
			unchecked((int)(0x32)), unchecked((int)(0x33)) };

		/// <exception cref="System.IO.IOException"/>
		internal static Path WriteFile(FileSystem fs, Path f)
		{
			DataOutputStream @out = fs.Create(f);
			@out.WriteBytes("dhruba: " + f);
			@out.Close();
			NUnit.Framework.Assert.IsTrue(fs.Exists(f));
			return f;
		}

		/// <exception cref="System.IO.IOException"/>
		internal static Path Mkdir(FileSystem fs, Path p)
		{
			NUnit.Framework.Assert.IsTrue(fs.Mkdirs(p));
			NUnit.Framework.Assert.IsTrue(fs.Exists(p));
			NUnit.Framework.Assert.IsTrue(fs.GetFileStatus(p).IsDirectory());
			return p;
		}

		/// <exception cref="System.IO.IOException"/>
		internal static FilePath CreateLocalFile(FilePath f)
		{
			NUnit.Framework.Assert.IsTrue(!f.Exists());
			PrintWriter @out = new PrintWriter(f);
			@out.Write("createLocalFile: " + f.GetAbsolutePath());
			@out.Flush();
			@out.Close();
			NUnit.Framework.Assert.IsTrue(f.Exists());
			NUnit.Framework.Assert.IsTrue(f.IsFile());
			return f;
		}

		/// <exception cref="System.IO.IOException"/>
		internal static FilePath CreateLocalFileWithRandomData(int fileLength, FilePath f
			)
		{
			NUnit.Framework.Assert.IsTrue(!f.Exists());
			f.CreateNewFile();
			FileOutputStream @out = new FileOutputStream(f.ToString());
			byte[] buffer = new byte[fileLength];
			@out.Write(buffer);
			@out.Flush();
			@out.Close();
			return f;
		}

		internal static void Show(string s)
		{
			System.Console.Out.WriteLine(Sharpen.Thread.CurrentThread().GetStackTrace()[2] + 
				" " + s);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestZeroSizeFile()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			FileSystem fs = cluster.GetFileSystem();
			NUnit.Framework.Assert.IsTrue("Not a HDFS: " + fs.GetUri(), fs is DistributedFileSystem
				);
			DistributedFileSystem dfs = (DistributedFileSystem)fs;
			try
			{
				//create a zero size file
				FilePath f1 = new FilePath(TestRootDir, "f1");
				NUnit.Framework.Assert.IsTrue(!f1.Exists());
				NUnit.Framework.Assert.IsTrue(f1.CreateNewFile());
				NUnit.Framework.Assert.IsTrue(f1.Exists());
				NUnit.Framework.Assert.IsTrue(f1.IsFile());
				NUnit.Framework.Assert.AreEqual(0L, f1.Length());
				//copy to remote
				Path root = Mkdir(dfs, new Path("/test/zeroSizeFile"));
				Path remotef = new Path(root, "dst");
				Show("copy local " + f1 + " to remote " + remotef);
				dfs.CopyFromLocalFile(false, false, new Path(f1.GetPath()), remotef);
				//getBlockSize() should not throw exception
				Show("Block size = " + dfs.GetFileStatus(remotef).GetBlockSize());
				//copy back
				FilePath f2 = new FilePath(TestRootDir, "f2");
				NUnit.Framework.Assert.IsTrue(!f2.Exists());
				dfs.CopyToLocalFile(remotef, new Path(f2.GetPath()));
				NUnit.Framework.Assert.IsTrue(f2.Exists());
				NUnit.Framework.Assert.IsTrue(f2.IsFile());
				NUnit.Framework.Assert.AreEqual(0L, f2.Length());
				f1.Delete();
				f2.Delete();
			}
			finally
			{
				try
				{
					dfs.Close();
				}
				catch (Exception)
				{
				}
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRecursiveRm()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			FileSystem fs = cluster.GetFileSystem();
			NUnit.Framework.Assert.IsTrue("Not a HDFS: " + fs.GetUri(), fs is DistributedFileSystem
				);
			try
			{
				fs.Mkdirs(new Path(new Path("parent"), "child"));
				try
				{
					fs.Delete(new Path("parent"), false);
					System.Diagnostics.Debug.Assert((false));
				}
				catch (IOException)
				{
				}
				// should never reach here.
				//should have thrown an exception
				try
				{
					fs.Delete(new Path("parent"), true);
				}
				catch (IOException)
				{
					System.Diagnostics.Debug.Assert((false));
				}
			}
			finally
			{
				try
				{
					fs.Close();
				}
				catch (IOException)
				{
				}
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestDu()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			DistributedFileSystem fs = cluster.GetFileSystem();
			TextWriter psBackup = System.Console.Out;
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			TextWriter psOut = new TextWriter(@out);
			Runtime.SetOut(psOut);
			FsShell shell = new FsShell();
			shell.SetConf(conf);
			try
			{
				Path myPath = new Path("/test/dir");
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(myPath));
				NUnit.Framework.Assert.IsTrue(fs.Exists(myPath));
				Path myFile = new Path("/test/dir/file");
				WriteFile(fs, myFile);
				NUnit.Framework.Assert.IsTrue(fs.Exists(myFile));
				Path myFile2 = new Path("/test/dir/file2");
				WriteFile(fs, myFile2);
				NUnit.Framework.Assert.IsTrue(fs.Exists(myFile2));
				long myFileLength = fs.GetFileStatus(myFile).GetLen();
				long myFile2Length = fs.GetFileStatus(myFile2).GetLen();
				string[] args = new string[2];
				args[0] = "-du";
				args[1] = "/test/dir";
				int val = -1;
				try
				{
					val = shell.Run(args);
				}
				catch (Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from DFSShell.run " + e.GetLocalizedMessage
						());
				}
				NUnit.Framework.Assert.IsTrue(val == 0);
				string returnString = @out.ToString();
				@out.Reset();
				// Check if size matchs as expected
				Assert.AssertThat(returnString, StringContains.ContainsString(myFileLength.ToString
					()));
				Assert.AssertThat(returnString, StringContains.ContainsString(myFile2Length.ToString
					()));
				// Check that -du -s reports the state of the snapshot
				string snapshotName = "ss1";
				Path snapshotPath = new Path(myPath, ".snapshot/" + snapshotName);
				fs.AllowSnapshot(myPath);
				Assert.AssertThat(fs.CreateSnapshot(myPath, snapshotName), CoreMatchers.Is(snapshotPath
					));
				Assert.AssertThat(fs.Delete(myFile, false), CoreMatchers.Is(true));
				Assert.AssertThat(fs.Exists(myFile), CoreMatchers.Is(false));
				args = new string[3];
				args[0] = "-du";
				args[1] = "-s";
				args[2] = snapshotPath.ToString();
				val = -1;
				try
				{
					val = shell.Run(args);
				}
				catch (Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from DFSShell.run " + e.GetLocalizedMessage
						());
				}
				Assert.AssertThat(val, CoreMatchers.Is(0));
				returnString = @out.ToString();
				@out.Reset();
				long combinedLength = myFileLength + myFile2Length;
				Assert.AssertThat(returnString, StringContains.ContainsString(combinedLength.ToString
					()));
			}
			finally
			{
				Runtime.SetOut(psBackup);
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestPut()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			FileSystem fs = cluster.GetFileSystem();
			NUnit.Framework.Assert.IsTrue("Not a HDFS: " + fs.GetUri(), fs is DistributedFileSystem
				);
			DistributedFileSystem dfs = (DistributedFileSystem)fs;
			try
			{
				// remove left over crc files:
				new FilePath(TestRootDir, ".f1.crc").Delete();
				new FilePath(TestRootDir, ".f2.crc").Delete();
				FilePath f1 = CreateLocalFile(new FilePath(TestRootDir, "f1"));
				FilePath f2 = CreateLocalFile(new FilePath(TestRootDir, "f2"));
				Path root = Mkdir(dfs, new Path("/test/put"));
				Path dst = new Path(root, "dst");
				Show("begin");
				Sharpen.Thread copy2ndFileThread = new _Thread_295(f2, dst, dfs);
				//should not be here, must got IOException
				//use SecurityManager to pause the copying of f1 and begin copying f2
				SecurityManager sm = Runtime.GetSecurityManager();
				System.Console.Out.WriteLine("SecurityManager = " + sm);
				Runtime.SetSecurityManager(new _SecurityManager_313(copy2ndFileThread));
				//pause at FileUtil.copyContent
				Show("copy local " + f1 + " to remote " + dst);
				dfs.CopyFromLocalFile(false, false, new Path(f1.GetPath()), dst);
				Show("done");
				try
				{
					copy2ndFileThread.Join();
				}
				catch (Exception)
				{
				}
				Runtime.SetSecurityManager(sm);
				// copy multiple files to destination directory
				Path destmultiple = Mkdir(dfs, new Path("/test/putmultiple"));
				Path[] srcs = new Path[2];
				srcs[0] = new Path(f1.GetPath());
				srcs[1] = new Path(f2.GetPath());
				dfs.CopyFromLocalFile(false, false, srcs, destmultiple);
				srcs[0] = new Path(destmultiple, "f1");
				srcs[1] = new Path(destmultiple, "f2");
				NUnit.Framework.Assert.IsTrue(dfs.Exists(srcs[0]));
				NUnit.Framework.Assert.IsTrue(dfs.Exists(srcs[1]));
				// move multiple files to destination directory
				Path destmultiple2 = Mkdir(dfs, new Path("/test/movemultiple"));
				srcs[0] = new Path(f1.GetPath());
				srcs[1] = new Path(f2.GetPath());
				dfs.MoveFromLocalFile(srcs, destmultiple2);
				NUnit.Framework.Assert.IsFalse(f1.Exists());
				NUnit.Framework.Assert.IsFalse(f2.Exists());
				srcs[0] = new Path(destmultiple2, "f1");
				srcs[1] = new Path(destmultiple2, "f2");
				NUnit.Framework.Assert.IsTrue(dfs.Exists(srcs[0]));
				NUnit.Framework.Assert.IsTrue(dfs.Exists(srcs[1]));
				f1.Delete();
				f2.Delete();
			}
			finally
			{
				try
				{
					dfs.Close();
				}
				catch (Exception)
				{
				}
				cluster.Shutdown();
			}
		}

		private sealed class _Thread_295 : Sharpen.Thread
		{
			public _Thread_295(FilePath f2, Path dst, DistributedFileSystem dfs)
			{
				this.f2 = f2;
				this.dst = dst;
				this.dfs = dfs;
			}

			public override void Run()
			{
				try
				{
					Org.Apache.Hadoop.Hdfs.TestDFSShell.Show("copy local " + f2 + " to remote " + dst
						);
					dfs.CopyFromLocalFile(false, false, new Path(f2.GetPath()), dst);
				}
				catch (IOException ioe)
				{
					Org.Apache.Hadoop.Hdfs.TestDFSShell.Show("good " + StringUtils.StringifyException
						(ioe));
					return;
				}
				NUnit.Framework.Assert.IsTrue(false);
			}

			private readonly FilePath f2;

			private readonly Path dst;

			private readonly DistributedFileSystem dfs;
		}

		private sealed class _SecurityManager_313 : SecurityManager
		{
			public _SecurityManager_313(Sharpen.Thread copy2ndFileThread)
			{
				this.copy2ndFileThread = copy2ndFileThread;
				this.firstTime = true;
			}

			private bool firstTime;

			public override void CheckPermission(Permission perm)
			{
				if (this.firstTime)
				{
					Sharpen.Thread t = Sharpen.Thread.CurrentThread();
					if (!t.ToString().Contains("DataNode"))
					{
						string s = string.Empty + Arrays.AsList(t.GetStackTrace());
						if (s.Contains("FileUtil.copyContent"))
						{
							this.firstTime = false;
							copy2ndFileThread.Start();
							try
							{
								Sharpen.Thread.Sleep(5000);
							}
							catch (Exception)
							{
							}
						}
					}
				}
			}

			private readonly Sharpen.Thread copy2ndFileThread;
		}

		/// <summary>check command error outputs and exit statuses.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestErrOutPut()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			TextWriter bak = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
				FileSystem srcFs = cluster.GetFileSystem();
				Path root = new Path("/nonexistentfile");
				bak = System.Console.Error;
				ByteArrayOutputStream @out = new ByteArrayOutputStream();
				TextWriter tmp = new TextWriter(@out);
				Runtime.SetErr(tmp);
				string[] argv = new string[2];
				argv[0] = "-cat";
				argv[1] = root.ToUri().GetPath();
				int ret = ToolRunner.Run(new FsShell(), argv);
				NUnit.Framework.Assert.AreEqual(" -cat returned 1 ", 1, ret);
				string returned = @out.ToString();
				NUnit.Framework.Assert.IsTrue("cat does not print exceptions ", (returned.LastIndexOf
					("Exception") == -1));
				@out.Reset();
				argv[0] = "-rm";
				argv[1] = root.ToString();
				FsShell shell = new FsShell();
				shell.SetConf(conf);
				ret = ToolRunner.Run(shell, argv);
				NUnit.Framework.Assert.AreEqual(" -rm returned 1 ", 1, ret);
				returned = @out.ToString();
				@out.Reset();
				NUnit.Framework.Assert.IsTrue("rm prints reasonable error ", (returned.LastIndexOf
					("No such file or directory") != -1));
				argv[0] = "-rmr";
				argv[1] = root.ToString();
				ret = ToolRunner.Run(shell, argv);
				NUnit.Framework.Assert.AreEqual(" -rmr returned 1", 1, ret);
				returned = @out.ToString();
				NUnit.Framework.Assert.IsTrue("rmr prints reasonable error ", (returned.LastIndexOf
					("No such file or directory") != -1));
				@out.Reset();
				argv[0] = "-du";
				argv[1] = "/nonexistentfile";
				ret = ToolRunner.Run(shell, argv);
				returned = @out.ToString();
				NUnit.Framework.Assert.IsTrue(" -du prints reasonable error ", (returned.LastIndexOf
					("No such file or directory") != -1));
				@out.Reset();
				argv[0] = "-dus";
				argv[1] = "/nonexistentfile";
				ret = ToolRunner.Run(shell, argv);
				returned = @out.ToString();
				NUnit.Framework.Assert.IsTrue(" -dus prints reasonable error", (returned.LastIndexOf
					("No such file or directory") != -1));
				@out.Reset();
				argv[0] = "-ls";
				argv[1] = "/nonexistenfile";
				ret = ToolRunner.Run(shell, argv);
				returned = @out.ToString();
				NUnit.Framework.Assert.IsTrue(" -ls does not return Found 0 items", (returned.LastIndexOf
					("Found 0") == -1));
				@out.Reset();
				argv[0] = "-ls";
				argv[1] = "/nonexistentfile";
				ret = ToolRunner.Run(shell, argv);
				NUnit.Framework.Assert.AreEqual(" -lsr should fail ", 1, ret);
				@out.Reset();
				srcFs.Mkdirs(new Path("/testdir"));
				argv[0] = "-ls";
				argv[1] = "/testdir";
				ret = ToolRunner.Run(shell, argv);
				returned = @out.ToString();
				NUnit.Framework.Assert.IsTrue(" -ls does not print out anything ", (returned.LastIndexOf
					("Found 0") == -1));
				@out.Reset();
				argv[0] = "-ls";
				argv[1] = "/user/nonxistant/*";
				ret = ToolRunner.Run(shell, argv);
				NUnit.Framework.Assert.AreEqual(" -ls on nonexistent glob returns 1", 1, ret);
				@out.Reset();
				argv[0] = "-mkdir";
				argv[1] = "/testdir";
				ret = ToolRunner.Run(shell, argv);
				returned = @out.ToString();
				NUnit.Framework.Assert.AreEqual(" -mkdir returned 1 ", 1, ret);
				NUnit.Framework.Assert.IsTrue(" -mkdir returned File exists", (returned.LastIndexOf
					("File exists") != -1));
				Path testFile = new Path("/testfile");
				OutputStream outtmp = srcFs.Create(testFile);
				outtmp.Write(Sharpen.Runtime.GetBytesForString(testFile.ToString()));
				outtmp.Close();
				@out.Reset();
				argv[0] = "-mkdir";
				argv[1] = "/testfile";
				ret = ToolRunner.Run(shell, argv);
				returned = @out.ToString();
				NUnit.Framework.Assert.AreEqual(" -mkdir returned 1", 1, ret);
				NUnit.Framework.Assert.IsTrue(" -mkdir returned this is a file ", (returned.LastIndexOf
					("not a directory") != -1));
				@out.Reset();
				argv = new string[3];
				argv[0] = "-mv";
				argv[1] = "/testfile";
				argv[2] = "file";
				ret = ToolRunner.Run(shell, argv);
				NUnit.Framework.Assert.AreEqual("mv failed to rename", 1, ret);
				@out.Reset();
				argv = new string[3];
				argv[0] = "-mv";
				argv[1] = "/testfile";
				argv[2] = "/testfiletest";
				ret = ToolRunner.Run(shell, argv);
				returned = @out.ToString();
				NUnit.Framework.Assert.IsTrue("no output from rename", (returned.LastIndexOf("Renamed"
					) == -1));
				@out.Reset();
				argv[0] = "-mv";
				argv[1] = "/testfile";
				argv[2] = "/testfiletmp";
				ret = ToolRunner.Run(shell, argv);
				returned = @out.ToString();
				NUnit.Framework.Assert.IsTrue(" unix like output", (returned.LastIndexOf("No such file or"
					) != -1));
				@out.Reset();
				argv = new string[1];
				argv[0] = "-du";
				srcFs.Mkdirs(srcFs.GetHomeDirectory());
				ret = ToolRunner.Run(shell, argv);
				returned = @out.ToString();
				NUnit.Framework.Assert.AreEqual(" no error ", 0, ret);
				NUnit.Framework.Assert.IsTrue("empty path specified", (returned.LastIndexOf("empty string"
					) == -1));
				@out.Reset();
				argv = new string[3];
				argv[0] = "-test";
				argv[1] = "-d";
				argv[2] = "/no/such/dir";
				ret = ToolRunner.Run(shell, argv);
				returned = @out.ToString();
				NUnit.Framework.Assert.AreEqual(" -test -d wrong result ", 1, ret);
				NUnit.Framework.Assert.IsTrue(returned.IsEmpty());
			}
			finally
			{
				if (bak != null)
				{
					Runtime.SetErr(bak);
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestURIPaths()
		{
			Configuration srcConf = new HdfsConfiguration();
			Configuration dstConf = new HdfsConfiguration();
			MiniDFSCluster srcCluster = null;
			MiniDFSCluster dstCluster = null;
			FilePath bak = new FilePath(PathUtils.GetTestDir(GetType()), "dfs_tmp_uri");
			bak.Mkdirs();
			try
			{
				srcCluster = new MiniDFSCluster.Builder(srcConf).NumDataNodes(2).Build();
				dstConf.Set(MiniDFSCluster.HdfsMinidfsBasedir, bak.GetAbsolutePath());
				dstCluster = new MiniDFSCluster.Builder(dstConf).NumDataNodes(2).Build();
				FileSystem srcFs = srcCluster.GetFileSystem();
				FileSystem dstFs = dstCluster.GetFileSystem();
				FsShell shell = new FsShell();
				shell.SetConf(srcConf);
				//check for ls
				string[] argv = new string[2];
				argv[0] = "-ls";
				argv[1] = dstFs.GetUri().ToString() + "/";
				int ret = ToolRunner.Run(shell, argv);
				NUnit.Framework.Assert.AreEqual("ls works on remote uri ", 0, ret);
				//check for rm -r 
				dstFs.Mkdirs(new Path("/hadoopdir"));
				argv = new string[2];
				argv[0] = "-rmr";
				argv[1] = dstFs.GetUri().ToString() + "/hadoopdir";
				ret = ToolRunner.Run(shell, argv);
				NUnit.Framework.Assert.AreEqual("-rmr works on remote uri " + argv[1], 0, ret);
				//check du 
				argv[0] = "-du";
				argv[1] = dstFs.GetUri().ToString() + "/";
				ret = ToolRunner.Run(shell, argv);
				NUnit.Framework.Assert.AreEqual("du works on remote uri ", 0, ret);
				//check put
				FilePath furi = new FilePath(TestRootDir, "furi");
				CreateLocalFile(furi);
				argv = new string[3];
				argv[0] = "-put";
				argv[1] = furi.ToURI().ToString();
				argv[2] = dstFs.GetUri().ToString() + "/furi";
				ret = ToolRunner.Run(shell, argv);
				NUnit.Framework.Assert.AreEqual(" put is working ", 0, ret);
				//check cp 
				argv[0] = "-cp";
				argv[1] = dstFs.GetUri().ToString() + "/furi";
				argv[2] = srcFs.GetUri().ToString() + "/furi";
				ret = ToolRunner.Run(shell, argv);
				NUnit.Framework.Assert.AreEqual(" cp is working ", 0, ret);
				NUnit.Framework.Assert.IsTrue(srcFs.Exists(new Path("/furi")));
				//check cat 
				argv = new string[2];
				argv[0] = "-cat";
				argv[1] = dstFs.GetUri().ToString() + "/furi";
				ret = ToolRunner.Run(shell, argv);
				NUnit.Framework.Assert.AreEqual(" cat is working ", 0, ret);
				//check chown
				dstFs.Delete(new Path("/furi"), true);
				dstFs.Delete(new Path("/hadoopdir"), true);
				string file = "/tmp/chownTest";
				Path path = new Path(file);
				Path parent = new Path("/tmp");
				Path root = new Path("/");
				Org.Apache.Hadoop.Hdfs.TestDFSShell.WriteFile(dstFs, path);
				RunCmd(shell, "-chgrp", "-R", "herbivores", dstFs.GetUri().ToString() + "/*");
				ConfirmOwner(null, "herbivores", dstFs, parent, path);
				RunCmd(shell, "-chown", "-R", ":reptiles", dstFs.GetUri().ToString() + "/");
				ConfirmOwner(null, "reptiles", dstFs, root, parent, path);
				//check if default hdfs:/// works 
				argv[0] = "-cat";
				argv[1] = "hdfs:///furi";
				ret = ToolRunner.Run(shell, argv);
				NUnit.Framework.Assert.AreEqual(" default works for cat", 0, ret);
				argv[0] = "-ls";
				argv[1] = "hdfs:///";
				ret = ToolRunner.Run(shell, argv);
				NUnit.Framework.Assert.AreEqual("default works for ls ", 0, ret);
				argv[0] = "-rmr";
				argv[1] = "hdfs:///furi";
				ret = ToolRunner.Run(shell, argv);
				NUnit.Framework.Assert.AreEqual("default works for rm/rmr", 0, ret);
			}
			finally
			{
				if (null != srcCluster)
				{
					srcCluster.Shutdown();
				}
				if (null != dstCluster)
				{
					dstCluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestText()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
				FileSystem dfs = cluster.GetFileSystem();
				TextTest(new Path("/texttest").MakeQualified(dfs.GetUri(), dfs.GetWorkingDirectory
					()), conf);
				conf.Set("fs.defaultFS", dfs.GetUri().ToString());
				FileSystem lfs = FileSystem.GetLocal(conf);
				TextTest(new Path(TestRootDir, "texttest").MakeQualified(lfs.GetUri(), lfs.GetWorkingDirectory
					()), conf);
			}
			finally
			{
				if (null != cluster)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private void TextTest(Path root, Configuration conf)
		{
			TextWriter bak = null;
			try
			{
				FileSystem fs = root.GetFileSystem(conf);
				fs.Mkdirs(root);
				// Test the gzip type of files. Magic detection.
				OutputStream zout = new GZIPOutputStream(fs.Create(new Path(root, "file.gz")));
				Random r = new Random();
				bak = System.Console.Out;
				ByteArrayOutputStream file = new ByteArrayOutputStream();
				for (int i = 0; i < 1024; ++i)
				{
					char c = char.ForDigit(r.Next(26) + 10, 36);
					file.Write(c);
					zout.Write(c);
				}
				zout.Close();
				ByteArrayOutputStream @out = new ByteArrayOutputStream();
				Runtime.SetOut(new TextWriter(@out));
				string[] argv = new string[2];
				argv[0] = "-text";
				argv[1] = new Path(root, "file.gz").ToString();
				int ret = ToolRunner.Run(new FsShell(conf), argv);
				NUnit.Framework.Assert.AreEqual("'-text " + argv[1] + " returned " + ret, 0, ret);
				NUnit.Framework.Assert.IsTrue("Output doesn't match input", Arrays.Equals(file.ToByteArray
					(), @out.ToByteArray()));
				// Create a sequence file with a gz extension, to test proper
				// container detection. Magic detection.
				SequenceFile.Writer writer = SequenceFile.CreateWriter(conf, SequenceFile.Writer.
					File(new Path(root, "file.gz")), SequenceFile.Writer.KeyClass(typeof(Text)), SequenceFile.Writer
					.ValueClass(typeof(Text)));
				writer.Append(new Text("Foo"), new Text("Bar"));
				writer.Close();
				@out = new ByteArrayOutputStream();
				Runtime.SetOut(new TextWriter(@out));
				argv = new string[2];
				argv[0] = "-text";
				argv[1] = new Path(root, "file.gz").ToString();
				ret = ToolRunner.Run(new FsShell(conf), argv);
				NUnit.Framework.Assert.AreEqual("'-text " + argv[1] + " returned " + ret, 0, ret);
				NUnit.Framework.Assert.IsTrue("Output doesn't match input", Arrays.Equals(Sharpen.Runtime.GetBytesForString
					("Foo\tBar\n"), @out.ToByteArray()));
				@out.Reset();
				// Test deflate. Extension-based detection.
				OutputStream dout = new DeflaterOutputStream(fs.Create(new Path(root, "file.deflate"
					)));
				byte[] outbytes = Sharpen.Runtime.GetBytesForString("foo");
				dout.Write(outbytes);
				dout.Close();
				@out = new ByteArrayOutputStream();
				Runtime.SetOut(new TextWriter(@out));
				argv = new string[2];
				argv[0] = "-text";
				argv[1] = new Path(root, "file.deflate").ToString();
				ret = ToolRunner.Run(new FsShell(conf), argv);
				NUnit.Framework.Assert.AreEqual("'-text " + argv[1] + " returned " + ret, 0, ret);
				NUnit.Framework.Assert.IsTrue("Output doesn't match input", Arrays.Equals(outbytes
					, @out.ToByteArray()));
				@out.Reset();
				// Test a simple codec. Extension based detection. We use
				// Bzip2 cause its non-native.
				CompressionCodec codec = ReflectionUtils.NewInstance<BZip2Codec>(conf);
				string extension = codec.GetDefaultExtension();
				Path p = new Path(root, "file." + extension);
				OutputStream fout = new DataOutputStream(codec.CreateOutputStream(fs.Create(p, true
					)));
				byte[] writebytes = Sharpen.Runtime.GetBytesForString("foo");
				fout.Write(writebytes);
				fout.Close();
				@out = new ByteArrayOutputStream();
				Runtime.SetOut(new TextWriter(@out));
				argv = new string[2];
				argv[0] = "-text";
				argv[1] = new Path(root, p).ToString();
				ret = ToolRunner.Run(new FsShell(conf), argv);
				NUnit.Framework.Assert.AreEqual("'-text " + argv[1] + " returned " + ret, 0, ret);
				NUnit.Framework.Assert.IsTrue("Output doesn't match input", Arrays.Equals(writebytes
					, @out.ToByteArray()));
				@out.Reset();
			}
			finally
			{
				if (null != bak)
				{
					Runtime.SetOut(bak);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCopyToLocal()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			FileSystem fs = cluster.GetFileSystem();
			NUnit.Framework.Assert.IsTrue("Not a HDFS: " + fs.GetUri(), fs is DistributedFileSystem
				);
			DistributedFileSystem dfs = (DistributedFileSystem)fs;
			FsShell shell = new FsShell();
			shell.SetConf(conf);
			try
			{
				string root = CreateTree(dfs, "copyToLocal");
				{
					// Verify copying the tree
					try
					{
						NUnit.Framework.Assert.AreEqual(0, RunCmd(shell, "-copyToLocal", root + "*", TestRootDir
							));
					}
					catch (Exception e)
					{
						System.Console.Error.WriteLine("Exception raised from DFSShell.run " + e.GetLocalizedMessage
							());
					}
					FilePath localroot = new FilePath(TestRootDir, "copyToLocal");
					FilePath localroot2 = new FilePath(TestRootDir, "copyToLocal2");
					FilePath f1 = new FilePath(localroot, "f1");
					NUnit.Framework.Assert.IsTrue("Copying failed.", f1.IsFile());
					FilePath f2 = new FilePath(localroot, "f2");
					NUnit.Framework.Assert.IsTrue("Copying failed.", f2.IsFile());
					FilePath sub = new FilePath(localroot, "sub");
					NUnit.Framework.Assert.IsTrue("Copying failed.", sub.IsDirectory());
					FilePath f3 = new FilePath(sub, "f3");
					NUnit.Framework.Assert.IsTrue("Copying failed.", f3.IsFile());
					FilePath f4 = new FilePath(sub, "f4");
					NUnit.Framework.Assert.IsTrue("Copying failed.", f4.IsFile());
					FilePath f5 = new FilePath(localroot2, "f1");
					NUnit.Framework.Assert.IsTrue("Copying failed.", f5.IsFile());
					f1.Delete();
					f2.Delete();
					f3.Delete();
					f4.Delete();
					f5.Delete();
					sub.Delete();
				}
				{
					// Verify copying non existing sources do not create zero byte
					// destination files
					string[] args = new string[] { "-copyToLocal", "nosuchfile", TestRootDir };
					try
					{
						NUnit.Framework.Assert.AreEqual(1, shell.Run(args));
					}
					catch (Exception e)
					{
						System.Console.Error.WriteLine("Exception raised from DFSShell.run " + e.GetLocalizedMessage
							());
					}
					FilePath f6 = new FilePath(TestRootDir, "nosuchfile");
					NUnit.Framework.Assert.IsTrue(!f6.Exists());
				}
			}
			finally
			{
				try
				{
					dfs.Close();
				}
				catch (Exception)
				{
				}
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static string CreateTree(FileSystem fs, string name)
		{
			// create a tree
			//   ROOT
			//   |- f1
			//   |- f2
			//   + sub
			//      |- f3
			//      |- f4
			//   ROOT2
			//   |- f1
			string path = "/test/" + name;
			Path root = Mkdir(fs, new Path(path));
			Path sub = Mkdir(fs, new Path(root, "sub"));
			Path root2 = Mkdir(fs, new Path(path + "2"));
			WriteFile(fs, new Path(root, "f1"));
			WriteFile(fs, new Path(root, "f2"));
			WriteFile(fs, new Path(sub, "f3"));
			WriteFile(fs, new Path(sub, "f4"));
			WriteFile(fs, new Path(root2, "f1"));
			Mkdir(fs, new Path(root2, "sub"));
			return path;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCount()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			DistributedFileSystem dfs = cluster.GetFileSystem();
			FsShell shell = new FsShell();
			shell.SetConf(conf);
			try
			{
				string root = CreateTree(dfs, "count");
				// Verify the counts
				RunCount(root, 2, 4, shell);
				RunCount(root + "2", 2, 1, shell);
				RunCount(root + "2/f1", 0, 1, shell);
				RunCount(root + "2/sub", 1, 0, shell);
				FileSystem localfs = FileSystem.GetLocal(conf);
				Path localpath = new Path(TestRootDir, "testcount");
				localpath = localpath.MakeQualified(localfs.GetUri(), localfs.GetWorkingDirectory
					());
				localfs.Mkdirs(localpath);
				string localstr = localpath.ToString();
				System.Console.Out.WriteLine("localstr=" + localstr);
				RunCount(localstr, 1, 0, shell);
				NUnit.Framework.Assert.AreEqual(0, RunCmd(shell, "-count", root, localstr));
			}
			finally
			{
				try
				{
					dfs.Close();
				}
				catch (Exception)
				{
				}
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void RunCount(string path, long dirs, long files, FsShell shell)
		{
			ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			TextWriter @out = new TextWriter(bytes);
			TextWriter oldOut = System.Console.Out;
			Runtime.SetOut(@out);
			Scanner @in = null;
			string results = null;
			try
			{
				RunCmd(shell, "-count", path);
				results = bytes.ToString();
				@in = new Scanner(results);
				NUnit.Framework.Assert.AreEqual(dirs, @in.NextLong());
				NUnit.Framework.Assert.AreEqual(files, @in.NextLong());
			}
			finally
			{
				if (@in != null)
				{
					@in.Close();
				}
				IOUtils.CloseStream(@out);
				Runtime.SetOut(oldOut);
				System.Console.Out.WriteLine("results:\n" + results);
			}
		}

		//throws IOException instead of Exception as shell.run() does.
		/// <exception cref="System.IO.IOException"/>
		private static int RunCmd(FsShell shell, params string[] args)
		{
			StringBuilder cmdline = new StringBuilder("RUN:");
			foreach (string arg in args)
			{
				cmdline.Append(" " + arg);
			}
			Log.Info(cmdline.ToString());
			try
			{
				int exitCode;
				exitCode = shell.Run(args);
				Log.Info("RUN: " + args[0] + " exit=" + exitCode);
				return exitCode;
			}
			catch (IOException e)
			{
				Log.Error("RUN: " + args[0] + " IOException=" + e.Message);
				throw;
			}
			catch (RuntimeException e)
			{
				Log.Error("RUN: " + args[0] + " RuntimeException=" + e.Message);
				throw;
			}
			catch (Exception e)
			{
				Log.Error("RUN: " + args[0] + " Exception=" + e.Message);
				throw new IOException(StringUtils.StringifyException(e));
			}
		}

		/// <summary>Test chmod.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void TestChmod(Configuration conf, FileSystem fs, string chmodDir
			)
		{
			FsShell shell = new FsShell();
			shell.SetConf(conf);
			try
			{
				//first make dir
				Path dir = new Path(chmodDir);
				fs.Delete(dir, true);
				fs.Mkdirs(dir);
				ConfirmPermissionChange("u+rwx,g=rw,o-rwx", "rwxrw----", fs, shell, dir);
				/* Setting */
				/* Should give */
				//create an empty file
				Path file = new Path(chmodDir, "file");
				Org.Apache.Hadoop.Hdfs.TestDFSShell.WriteFile(fs, file);
				//test octal mode
				ConfirmPermissionChange("644", "rw-r--r--", fs, shell, file);
				//test recursive
				RunCmd(shell, "-chmod", "-R", "a+rwX", chmodDir);
				NUnit.Framework.Assert.AreEqual("rwxrwxrwx", fs.GetFileStatus(dir).GetPermission(
					).ToString());
				NUnit.Framework.Assert.AreEqual("rw-rw-rw-", fs.GetFileStatus(file).GetPermission
					().ToString());
				// Skip "sticky bit" tests on Windows.
				//
				if (!Path.Windows)
				{
					// test sticky bit on directories
					Path dir2 = new Path(dir, "stickybit");
					fs.Mkdirs(dir2);
					Log.Info("Testing sticky bit on: " + dir2);
					Log.Info("Sticky bit directory initial mode: " + fs.GetFileStatus(dir2).GetPermission
						());
					ConfirmPermissionChange("u=rwx,g=rx,o=rx", "rwxr-xr-x", fs, shell, dir2);
					ConfirmPermissionChange("+t", "rwxr-xr-t", fs, shell, dir2);
					ConfirmPermissionChange("-t", "rwxr-xr-x", fs, shell, dir2);
					ConfirmPermissionChange("=t", "--------T", fs, shell, dir2);
					ConfirmPermissionChange("0000", "---------", fs, shell, dir2);
					ConfirmPermissionChange("1666", "rw-rw-rwT", fs, shell, dir2);
					ConfirmPermissionChange("777", "rwxrwxrwt", fs, shell, dir2);
					fs.Delete(dir2, true);
				}
				else
				{
					Log.Info("Skipped sticky bit tests on Windows");
				}
				fs.Delete(dir, true);
			}
			finally
			{
				try
				{
					fs.Close();
					shell.Close();
				}
				catch (IOException)
				{
				}
			}
		}

		// Apply a new permission to a path and confirm that the new permission
		// is the one you were expecting
		/// <exception cref="System.IO.IOException"/>
		private void ConfirmPermissionChange(string toApply, string expected, FileSystem 
			fs, FsShell shell, Path dir2)
		{
			Log.Info("Confirming permission change of " + toApply + " to " + expected);
			RunCmd(shell, "-chmod", toApply, dir2.ToString());
			string result = fs.GetFileStatus(dir2).GetPermission().ToString();
			Log.Info("Permission change result: " + result);
			NUnit.Framework.Assert.AreEqual(expected, result);
		}

		/// <exception cref="System.IO.IOException"/>
		private void ConfirmOwner(string owner, string group, FileSystem fs, params Path[]
			 paths)
		{
			foreach (Path path in paths)
			{
				if (owner != null)
				{
					NUnit.Framework.Assert.AreEqual(owner, fs.GetFileStatus(path).GetOwner());
				}
				if (group != null)
				{
					NUnit.Framework.Assert.AreEqual(group, fs.GetFileStatus(path).GetGroup());
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFilePermissions()
		{
			Configuration conf = new HdfsConfiguration();
			//test chmod on local fs
			FileSystem fs = FileSystem.GetLocal(conf);
			TestChmod(conf, fs, (new FilePath(TestRootDir, "chmodTest")).GetAbsolutePath());
			conf.Set(DFSConfigKeys.DfsPermissionsEnabledKey, "true");
			//test chmod on DFS
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			fs = cluster.GetFileSystem();
			TestChmod(conf, fs, "/tmp/chmodTest");
			// test chown and chgrp on DFS:
			FsShell shell = new FsShell();
			shell.SetConf(conf);
			fs = cluster.GetFileSystem();
			/* For dfs, I am the super user and I can change owner of any file to
			* anything. "-R" option is already tested by chmod test above.
			*/
			string file = "/tmp/chownTest";
			Path path = new Path(file);
			Path parent = new Path("/tmp");
			Path root = new Path("/");
			Org.Apache.Hadoop.Hdfs.TestDFSShell.WriteFile(fs, path);
			RunCmd(shell, "-chgrp", "-R", "herbivores", "/*", "unknownFile*");
			ConfirmOwner(null, "herbivores", fs, parent, path);
			RunCmd(shell, "-chgrp", "mammals", file);
			ConfirmOwner(null, "mammals", fs, path);
			RunCmd(shell, "-chown", "-R", ":reptiles", "/");
			ConfirmOwner(null, "reptiles", fs, root, parent, path);
			RunCmd(shell, "-chown", "python:", "/nonExistentFile", file);
			ConfirmOwner("python", "reptiles", fs, path);
			RunCmd(shell, "-chown", "-R", "hadoop:toys", "unknownFile", "/");
			ConfirmOwner("hadoop", "toys", fs, root, parent, path);
			// Test different characters in names
			RunCmd(shell, "-chown", "hdfs.user", file);
			ConfirmOwner("hdfs.user", null, fs, path);
			RunCmd(shell, "-chown", "_Hdfs.User-10:_hadoop.users--", file);
			ConfirmOwner("_Hdfs.User-10", "_hadoop.users--", fs, path);
			RunCmd(shell, "-chown", "hdfs/hadoop-core@apache.org:asf-projects", file);
			ConfirmOwner("hdfs/hadoop-core@apache.org", "asf-projects", fs, path);
			RunCmd(shell, "-chgrp", "hadoop-core@apache.org/100", file);
			ConfirmOwner(null, "hadoop-core@apache.org/100", fs, path);
			cluster.Shutdown();
		}

		/// <summary>Tests various options of DFSShell.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestDFSShell()
		{
			Configuration conf = new HdfsConfiguration();
			/* This tests some properties of ChecksumFileSystem as well.
			* Make sure that we create ChecksumDFS */
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			FileSystem fs = cluster.GetFileSystem();
			NUnit.Framework.Assert.IsTrue("Not a HDFS: " + fs.GetUri(), fs is DistributedFileSystem
				);
			DistributedFileSystem fileSys = (DistributedFileSystem)fs;
			FsShell shell = new FsShell();
			shell.SetConf(conf);
			try
			{
				// First create a new directory with mkdirs
				Path myPath = new Path("/test/mkdirs");
				NUnit.Framework.Assert.IsTrue(fileSys.Mkdirs(myPath));
				NUnit.Framework.Assert.IsTrue(fileSys.Exists(myPath));
				NUnit.Framework.Assert.IsTrue(fileSys.Mkdirs(myPath));
				// Second, create a file in that directory.
				Path myFile = new Path("/test/mkdirs/myFile");
				WriteFile(fileSys, myFile);
				NUnit.Framework.Assert.IsTrue(fileSys.Exists(myFile));
				Path myFile2 = new Path("/test/mkdirs/myFile2");
				WriteFile(fileSys, myFile2);
				NUnit.Framework.Assert.IsTrue(fileSys.Exists(myFile2));
				{
					// Verify that rm with a pattern
					string[] args = new string[2];
					args[0] = "-rm";
					args[1] = "/test/mkdirs/myFile*";
					int val = -1;
					try
					{
						val = shell.Run(args);
					}
					catch (Exception e)
					{
						System.Console.Error.WriteLine("Exception raised from DFSShell.run " + e.GetLocalizedMessage
							());
					}
					NUnit.Framework.Assert.IsTrue(val == 0);
					NUnit.Framework.Assert.IsFalse(fileSys.Exists(myFile));
					NUnit.Framework.Assert.IsFalse(fileSys.Exists(myFile2));
					//re-create the files for other tests
					WriteFile(fileSys, myFile);
					NUnit.Framework.Assert.IsTrue(fileSys.Exists(myFile));
					WriteFile(fileSys, myFile2);
					NUnit.Framework.Assert.IsTrue(fileSys.Exists(myFile2));
				}
				{
					// Verify that we can read the file
					string[] args = new string[3];
					args[0] = "-cat";
					args[1] = "/test/mkdirs/myFile";
					args[2] = "/test/mkdirs/myFile2";
					int val = -1;
					try
					{
						val = shell.Run(args);
					}
					catch (Exception e)
					{
						System.Console.Error.WriteLine("Exception raised from DFSShell.run: " + StringUtils
							.StringifyException(e));
					}
					NUnit.Framework.Assert.IsTrue(val == 0);
				}
				fileSys.Delete(myFile2, true);
				{
					// Verify that we get an error while trying to read an nonexistent file
					string[] args = new string[2];
					args[0] = "-cat";
					args[1] = "/test/mkdirs/myFile1";
					int val = -1;
					try
					{
						val = shell.Run(args);
					}
					catch (Exception e)
					{
						System.Console.Error.WriteLine("Exception raised from DFSShell.run " + e.GetLocalizedMessage
							());
					}
					NUnit.Framework.Assert.IsTrue(val != 0);
				}
				{
					// Verify that we get an error while trying to delete an nonexistent file
					string[] args = new string[2];
					args[0] = "-rm";
					args[1] = "/test/mkdirs/myFile1";
					int val = -1;
					try
					{
						val = shell.Run(args);
					}
					catch (Exception e)
					{
						System.Console.Error.WriteLine("Exception raised from DFSShell.run " + e.GetLocalizedMessage
							());
					}
					NUnit.Framework.Assert.IsTrue(val != 0);
				}
				{
					// Verify that we succeed in removing the file we created
					string[] args = new string[2];
					args[0] = "-rm";
					args[1] = "/test/mkdirs/myFile";
					int val = -1;
					try
					{
						val = shell.Run(args);
					}
					catch (Exception e)
					{
						System.Console.Error.WriteLine("Exception raised from DFSShell.run " + e.GetLocalizedMessage
							());
					}
					NUnit.Framework.Assert.IsTrue(val == 0);
				}
				{
					// Verify touch/test
					string[] args;
					int val;
					args = new string[3];
					args[0] = "-test";
					args[1] = "-e";
					args[2] = "/test/mkdirs/noFileHere";
					val = -1;
					try
					{
						val = shell.Run(args);
					}
					catch (Exception e)
					{
						System.Console.Error.WriteLine("Exception raised from DFSShell.run " + e.GetLocalizedMessage
							());
					}
					NUnit.Framework.Assert.AreEqual(1, val);
					args[1] = "-z";
					val = -1;
					try
					{
						val = shell.Run(args);
					}
					catch (Exception e)
					{
						System.Console.Error.WriteLine("Exception raised from DFSShell.run " + e.GetLocalizedMessage
							());
					}
					NUnit.Framework.Assert.AreEqual(1, val);
					args = new string[2];
					args[0] = "-touchz";
					args[1] = "/test/mkdirs/isFileHere";
					val = -1;
					try
					{
						val = shell.Run(args);
					}
					catch (Exception e)
					{
						System.Console.Error.WriteLine("Exception raised from DFSShell.run " + e.GetLocalizedMessage
							());
					}
					NUnit.Framework.Assert.AreEqual(0, val);
					args = new string[2];
					args[0] = "-touchz";
					args[1] = "/test/mkdirs/thisDirNotExists/isFileHere";
					val = -1;
					try
					{
						val = shell.Run(args);
					}
					catch (Exception e)
					{
						System.Console.Error.WriteLine("Exception raised from DFSShell.run " + e.GetLocalizedMessage
							());
					}
					NUnit.Framework.Assert.AreEqual(1, val);
					args = new string[3];
					args[0] = "-test";
					args[1] = "-e";
					args[2] = "/test/mkdirs/isFileHere";
					val = -1;
					try
					{
						val = shell.Run(args);
					}
					catch (Exception e)
					{
						System.Console.Error.WriteLine("Exception raised from DFSShell.run " + e.GetLocalizedMessage
							());
					}
					NUnit.Framework.Assert.AreEqual(0, val);
					args[1] = "-d";
					val = -1;
					try
					{
						val = shell.Run(args);
					}
					catch (Exception e)
					{
						System.Console.Error.WriteLine("Exception raised from DFSShell.run " + e.GetLocalizedMessage
							());
					}
					NUnit.Framework.Assert.AreEqual(1, val);
					args[1] = "-z";
					val = -1;
					try
					{
						val = shell.Run(args);
					}
					catch (Exception e)
					{
						System.Console.Error.WriteLine("Exception raised from DFSShell.run " + e.GetLocalizedMessage
							());
					}
					NUnit.Framework.Assert.AreEqual(0, val);
				}
				{
					// Verify that cp from a directory to a subdirectory fails
					string[] args = new string[2];
					args[0] = "-mkdir";
					args[1] = "/test/dir1";
					int val = -1;
					try
					{
						val = shell.Run(args);
					}
					catch (Exception e)
					{
						System.Console.Error.WriteLine("Exception raised from DFSShell.run " + e.GetLocalizedMessage
							());
					}
					NUnit.Framework.Assert.AreEqual(0, val);
					// this should fail
					string[] args1 = new string[3];
					args1[0] = "-cp";
					args1[1] = "/test/dir1";
					args1[2] = "/test/dir1/dir2";
					val = 0;
					try
					{
						val = shell.Run(args1);
					}
					catch (Exception e)
					{
						System.Console.Error.WriteLine("Exception raised from DFSShell.run " + e.GetLocalizedMessage
							());
					}
					NUnit.Framework.Assert.AreEqual(1, val);
					// this should succeed
					args1[0] = "-cp";
					args1[1] = "/test/dir1";
					args1[2] = "/test/dir1foo";
					val = -1;
					try
					{
						val = shell.Run(args1);
					}
					catch (Exception e)
					{
						System.Console.Error.WriteLine("Exception raised from DFSShell.run " + e.GetLocalizedMessage
							());
					}
					NUnit.Framework.Assert.AreEqual(0, val);
				}
				{
					// Verify -test -f negative case (missing file)
					string[] args = new string[3];
					args[0] = "-test";
					args[1] = "-f";
					args[2] = "/test/mkdirs/noFileHere";
					int val = -1;
					try
					{
						val = shell.Run(args);
					}
					catch (Exception e)
					{
						System.Console.Error.WriteLine("Exception raised from DFSShell.run " + e.GetLocalizedMessage
							());
					}
					NUnit.Framework.Assert.AreEqual(1, val);
				}
				{
					// Verify -test -f negative case (directory rather than file)
					string[] args = new string[3];
					args[0] = "-test";
					args[1] = "-f";
					args[2] = "/test/mkdirs";
					int val = -1;
					try
					{
						val = shell.Run(args);
					}
					catch (Exception e)
					{
						System.Console.Error.WriteLine("Exception raised from DFSShell.run " + e.GetLocalizedMessage
							());
					}
					NUnit.Framework.Assert.AreEqual(1, val);
				}
				{
					// Verify -test -f positive case
					WriteFile(fileSys, myFile);
					NUnit.Framework.Assert.IsTrue(fileSys.Exists(myFile));
					string[] args = new string[3];
					args[0] = "-test";
					args[1] = "-f";
					args[2] = myFile.ToString();
					int val = -1;
					try
					{
						val = shell.Run(args);
					}
					catch (Exception e)
					{
						System.Console.Error.WriteLine("Exception raised from DFSShell.run " + e.GetLocalizedMessage
							());
					}
					NUnit.Framework.Assert.AreEqual(0, val);
				}
				{
					// Verify -test -s negative case (missing file)
					string[] args = new string[3];
					args[0] = "-test";
					args[1] = "-s";
					args[2] = "/test/mkdirs/noFileHere";
					int val = -1;
					try
					{
						val = shell.Run(args);
					}
					catch (Exception e)
					{
						System.Console.Error.WriteLine("Exception raised from DFSShell.run " + e.GetLocalizedMessage
							());
					}
					NUnit.Framework.Assert.AreEqual(1, val);
				}
				{
					// Verify -test -s negative case (zero length file)
					string[] args = new string[3];
					args[0] = "-test";
					args[1] = "-s";
					args[2] = "/test/mkdirs/isFileHere";
					int val = -1;
					try
					{
						val = shell.Run(args);
					}
					catch (Exception e)
					{
						System.Console.Error.WriteLine("Exception raised from DFSShell.run " + e.GetLocalizedMessage
							());
					}
					NUnit.Framework.Assert.AreEqual(1, val);
				}
				{
					// Verify -test -s positive case (nonzero length file)
					string[] args = new string[3];
					args[0] = "-test";
					args[1] = "-s";
					args[2] = myFile.ToString();
					int val = -1;
					try
					{
						val = shell.Run(args);
					}
					catch (Exception e)
					{
						System.Console.Error.WriteLine("Exception raised from DFSShell.run " + e.GetLocalizedMessage
							());
					}
					NUnit.Framework.Assert.AreEqual(0, val);
				}
			}
			finally
			{
				try
				{
					fileSys.Close();
				}
				catch (Exception)
				{
				}
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static IList<FilePath> GetBlockFiles(MiniDFSCluster cluster)
		{
			IList<FilePath> files = new AList<FilePath>();
			IList<DataNode> datanodes = cluster.GetDataNodes();
			string poolId = cluster.GetNamesystem().GetBlockPoolId();
			IList<IDictionary<DatanodeStorage, BlockListAsLongs>> blocks = cluster.GetAllBlockReports
				(poolId);
			for (int i = 0; i < blocks.Count; i++)
			{
				DataNode dn = datanodes[i];
				IDictionary<DatanodeStorage, BlockListAsLongs> map = blocks[i];
				foreach (KeyValuePair<DatanodeStorage, BlockListAsLongs> e in map)
				{
					foreach (Block b in e.Value)
					{
						files.AddItem(DataNodeTestUtils.GetFile(dn, poolId, b.GetBlockId()));
					}
				}
			}
			return files;
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void Corrupt(IList<FilePath> files)
		{
			foreach (FilePath f in files)
			{
				StringBuilder content = new StringBuilder(DFSTestUtil.ReadFile(f));
				char c = content[0];
				Sharpen.Runtime.SetCharAt(content, 0, ++c);
				PrintWriter @out = new PrintWriter(f);
				@out.Write(content);
				@out.Flush();
				@out.Close();
			}
		}

		internal interface TestGetRunner
		{
			/// <exception cref="System.IO.IOException"/>
			string Run(int exitcode, params string[] options);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRemoteException()
		{
			UserGroupInformation tmpUGI = UserGroupInformation.CreateUserForTesting("tmpname"
				, new string[] { "mygroup" });
			MiniDFSCluster dfs = null;
			TextWriter bak = null;
			try
			{
				Configuration conf = new HdfsConfiguration();
				dfs = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
				FileSystem fs = dfs.GetFileSystem();
				Path p = new Path("/foo");
				fs.Mkdirs(p);
				fs.SetPermission(p, new FsPermission((short)0x1c0));
				bak = System.Console.Error;
				tmpUGI.DoAs(new _PrivilegedExceptionAction_1465(conf));
			}
			finally
			{
				if (bak != null)
				{
					Runtime.SetErr(bak);
				}
				if (dfs != null)
				{
					dfs.Shutdown();
				}
			}
		}

		private sealed class _PrivilegedExceptionAction_1465 : PrivilegedExceptionAction<
			object>
		{
			public _PrivilegedExceptionAction_1465(Configuration conf)
			{
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				FsShell fshell = new FsShell(conf);
				ByteArrayOutputStream @out = new ByteArrayOutputStream();
				TextWriter tmp = new TextWriter(@out);
				Runtime.SetErr(tmp);
				string[] args = new string[2];
				args[0] = "-ls";
				args[1] = "/foo";
				int ret = ToolRunner.Run(fshell, args);
				NUnit.Framework.Assert.AreEqual("returned should be 1", 1, ret);
				string str = @out.ToString();
				NUnit.Framework.Assert.IsTrue("permission denied printed", str.IndexOf("Permission denied"
					) != -1);
				@out.Reset();
				return null;
			}

			private readonly Configuration conf;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGet()
		{
			GenericTestUtils.SetLogLevel(FSInputChecker.Log, Level.All);
			string fname = "testGet.txt";
			Path root = new Path("/test/get");
			Path remotef = new Path(root, fname);
			Configuration conf = new HdfsConfiguration();
			// Set short retry timeouts so this test runs faster
			conf.SetInt(DFSConfigKeys.DfsClientRetryWindowBase, 10);
			TestDFSShell.TestGetRunner runner = new _TestGetRunner_1504(conf, fname, remotef);
			FilePath localf = CreateLocalFile(new FilePath(TestRootDir, fname));
			MiniDFSCluster cluster = null;
			DistributedFileSystem dfs = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Format(true).Build();
				dfs = cluster.GetFileSystem();
				Mkdir(dfs, root);
				dfs.CopyFromLocalFile(false, false, new Path(localf.GetPath()), remotef);
				string localfcontent = DFSTestUtil.ReadFile(localf);
				NUnit.Framework.Assert.AreEqual(localfcontent, runner.Run(0));
				NUnit.Framework.Assert.AreEqual(localfcontent, runner.Run(0, "-ignoreCrc"));
				// find block files to modify later
				IList<FilePath> files = GetBlockFiles(cluster);
				// Shut down cluster and then corrupt the block files by overwriting a
				// portion with junk data.  We must shut down the cluster so that threads
				// in the data node do not hold locks on the block files while we try to
				// write into them.  Particularly on Windows, the data node's use of the
				// FileChannel.transferTo method can cause block files to be memory mapped
				// in read-only mode during the transfer to a client, and this causes a
				// locking conflict.  The call to shutdown the cluster blocks until all
				// DataXceiver threads exit, preventing this problem.
				dfs.Close();
				cluster.Shutdown();
				Show("files=" + files);
				Corrupt(files);
				// Start the cluster again, but do not reformat, so prior files remain.
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Format(false).Build();
				dfs = cluster.GetFileSystem();
				NUnit.Framework.Assert.AreEqual(null, runner.Run(1));
				string corruptedcontent = runner.Run(0, "-ignoreCrc");
				NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.Substring(localfcontent, 1), Sharpen.Runtime.Substring
					(corruptedcontent, 1));
				NUnit.Framework.Assert.AreEqual(localfcontent[0] + 1, corruptedcontent[0]);
			}
			finally
			{
				if (null != dfs)
				{
					try
					{
						dfs.Close();
					}
					catch (Exception)
					{
					}
				}
				if (null != cluster)
				{
					cluster.Shutdown();
				}
				localf.Delete();
			}
		}

		private sealed class _TestGetRunner_1504 : TestDFSShell.TestGetRunner
		{
			public _TestGetRunner_1504(Configuration conf, string fname, Path remotef)
			{
				this.conf = conf;
				this.fname = fname;
				this.remotef = remotef;
				this.count = 0;
				this.shell = new FsShell(conf);
			}

			private int count;

			private readonly FsShell shell;

			/// <exception cref="System.IO.IOException"/>
			public string Run(int exitcode, params string[] options)
			{
				string dst = new FilePath(TestDFSShell.TestRootDir, fname + ++this.count).GetAbsolutePath
					();
				string[] args = new string[options.Length + 3];
				args[0] = "-get";
				args[args.Length - 2] = remotef.ToString();
				args[args.Length - 1] = dst;
				for (int i = 0; i < options.Length; i++)
				{
					args[i + 1] = options[i];
				}
				TestDFSShell.Show("args=" + Arrays.AsList(args));
				try
				{
					NUnit.Framework.Assert.AreEqual(exitcode, this.shell.Run(args));
				}
				catch (Exception e)
				{
					NUnit.Framework.Assert.IsTrue(StringUtils.StringifyException(e), false);
				}
				return exitcode == 0 ? DFSTestUtil.ReadFile(new FilePath(dst)) : null;
			}

			private readonly Configuration conf;

			private readonly string fname;

			private readonly Path remotef;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestLsr()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			DistributedFileSystem dfs = cluster.GetFileSystem();
			try
			{
				string root = CreateTree(dfs, "lsr");
				dfs.Mkdirs(new Path(root, "zzz"));
				RunLsr(new FsShell(conf), root, 0);
				Path sub = new Path(root, "sub");
				dfs.SetPermission(sub, new FsPermission((short)0));
				UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
				string tmpusername = ugi.GetShortUserName() + "1";
				UserGroupInformation tmpUGI = UserGroupInformation.CreateUserForTesting(tmpusername
					, new string[] { tmpusername });
				string results = tmpUGI.DoAs(new _PrivilegedExceptionAction_1604(conf, root));
				NUnit.Framework.Assert.IsTrue(results.Contains("zzz"));
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		private sealed class _PrivilegedExceptionAction_1604 : PrivilegedExceptionAction<
			string>
		{
			public _PrivilegedExceptionAction_1604(Configuration conf, string root)
			{
				this.conf = conf;
				this.root = root;
			}

			/// <exception cref="System.Exception"/>
			public string Run()
			{
				return TestDFSShell.RunLsr(new FsShell(conf), root, 1);
			}

			private readonly Configuration conf;

			private readonly string root;
		}

		/// <exception cref="System.Exception"/>
		private static string RunLsr(FsShell shell, string root, int returnvalue)
		{
			System.Console.Out.WriteLine("root=" + root + ", returnvalue=" + returnvalue);
			ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			TextWriter @out = new TextWriter(bytes);
			TextWriter oldOut = System.Console.Out;
			TextWriter oldErr = System.Console.Error;
			Runtime.SetOut(@out);
			Runtime.SetErr(@out);
			string results;
			try
			{
				NUnit.Framework.Assert.AreEqual(returnvalue, shell.Run(new string[] { "-lsr", root
					 }));
				results = bytes.ToString();
			}
			finally
			{
				IOUtils.CloseStream(@out);
				Runtime.SetOut(oldOut);
				Runtime.SetErr(oldErr);
			}
			System.Console.Out.WriteLine("results:\n" + results);
			return results;
		}

		/// <summary>
		/// default setting is file:// which is not a DFS
		/// so DFSAdmin should throw and catch InvalidArgumentException
		/// and return -1 exit code.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestInvalidShell()
		{
			Configuration conf = new Configuration();
			// default FS (non-DFS)
			DFSAdmin admin = new DFSAdmin();
			admin.SetConf(conf);
			int res = admin.Run(new string[] { "-refreshNodes" });
			NUnit.Framework.Assert.AreEqual("expected to fail -1", res, -1);
		}

		// Preserve Copy Option is -ptopxa (timestamps, ownership, permission, XATTR,
		// ACLs)
		/// <exception cref="System.Exception"/>
		public virtual void TestCopyCommandsWithPreserveOption()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeXattrsEnabledKey, true);
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeAclsEnabledKey, true);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Format(
				true).Build();
			FsShell shell = null;
			FileSystem fs = null;
			string testdir = "/tmp/TestDFSShell-testCopyCommandsWithPreserveOption-" + counter
				.GetAndIncrement();
			Path hdfsTestDir = new Path(testdir);
			try
			{
				fs = cluster.GetFileSystem();
				fs.Mkdirs(hdfsTestDir);
				Path src = new Path(hdfsTestDir, "srcfile");
				fs.Create(src).Close();
				fs.SetAcl(src, Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
					.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
					.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
					.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
					.Group, "bar", FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope.Access
					, AclEntryType.Other, FsAction.Execute)));
				FileStatus status = fs.GetFileStatus(src);
				long mtime = status.GetModificationTime();
				long atime = status.GetAccessTime();
				string owner = status.GetOwner();
				string group = status.GetGroup();
				FsPermission perm = status.GetPermission();
				fs.SetXAttr(src, UserA1, UserA1Value);
				fs.SetXAttr(src, TrustedA1, TrustedA1Value);
				shell = new FsShell(conf);
				// -p
				Path target1 = new Path(hdfsTestDir, "targetfile1");
				string[] argv = new string[] { "-cp", "-p", src.ToUri().ToString(), target1.ToUri
					().ToString() };
				int ret = ToolRunner.Run(shell, argv);
				NUnit.Framework.Assert.AreEqual("cp -p is not working", Success, ret);
				FileStatus targetStatus = fs.GetFileStatus(target1);
				NUnit.Framework.Assert.AreEqual(mtime, targetStatus.GetModificationTime());
				NUnit.Framework.Assert.AreEqual(atime, targetStatus.GetAccessTime());
				NUnit.Framework.Assert.AreEqual(owner, targetStatus.GetOwner());
				NUnit.Framework.Assert.AreEqual(group, targetStatus.GetGroup());
				FsPermission targetPerm = targetStatus.GetPermission();
				NUnit.Framework.Assert.IsTrue(perm.Equals(targetPerm));
				IDictionary<string, byte[]> xattrs = fs.GetXAttrs(target1);
				NUnit.Framework.Assert.IsTrue(xattrs.IsEmpty());
				IList<AclEntry> acls = fs.GetAclStatus(target1).GetEntries();
				NUnit.Framework.Assert.IsTrue(acls.IsEmpty());
				NUnit.Framework.Assert.IsFalse(targetPerm.GetAclBit());
				// -ptop
				Path target2 = new Path(hdfsTestDir, "targetfile2");
				argv = new string[] { "-cp", "-ptop", src.ToUri().ToString(), target2.ToUri().ToString
					() };
				ret = ToolRunner.Run(shell, argv);
				NUnit.Framework.Assert.AreEqual("cp -ptop is not working", Success, ret);
				targetStatus = fs.GetFileStatus(target2);
				NUnit.Framework.Assert.AreEqual(mtime, targetStatus.GetModificationTime());
				NUnit.Framework.Assert.AreEqual(atime, targetStatus.GetAccessTime());
				NUnit.Framework.Assert.AreEqual(owner, targetStatus.GetOwner());
				NUnit.Framework.Assert.AreEqual(group, targetStatus.GetGroup());
				targetPerm = targetStatus.GetPermission();
				NUnit.Framework.Assert.IsTrue(perm.Equals(targetPerm));
				xattrs = fs.GetXAttrs(target2);
				NUnit.Framework.Assert.IsTrue(xattrs.IsEmpty());
				acls = fs.GetAclStatus(target2).GetEntries();
				NUnit.Framework.Assert.IsTrue(acls.IsEmpty());
				NUnit.Framework.Assert.IsFalse(targetPerm.GetAclBit());
				// -ptopx
				Path target3 = new Path(hdfsTestDir, "targetfile3");
				argv = new string[] { "-cp", "-ptopx", src.ToUri().ToString(), target3.ToUri().ToString
					() };
				ret = ToolRunner.Run(shell, argv);
				NUnit.Framework.Assert.AreEqual("cp -ptopx is not working", Success, ret);
				targetStatus = fs.GetFileStatus(target3);
				NUnit.Framework.Assert.AreEqual(mtime, targetStatus.GetModificationTime());
				NUnit.Framework.Assert.AreEqual(atime, targetStatus.GetAccessTime());
				NUnit.Framework.Assert.AreEqual(owner, targetStatus.GetOwner());
				NUnit.Framework.Assert.AreEqual(group, targetStatus.GetGroup());
				targetPerm = targetStatus.GetPermission();
				NUnit.Framework.Assert.IsTrue(perm.Equals(targetPerm));
				xattrs = fs.GetXAttrs(target3);
				NUnit.Framework.Assert.AreEqual(xattrs.Count, 2);
				Assert.AssertArrayEquals(UserA1Value, xattrs[UserA1]);
				Assert.AssertArrayEquals(TrustedA1Value, xattrs[TrustedA1]);
				acls = fs.GetAclStatus(target3).GetEntries();
				NUnit.Framework.Assert.IsTrue(acls.IsEmpty());
				NUnit.Framework.Assert.IsFalse(targetPerm.GetAclBit());
				// -ptopa
				Path target4 = new Path(hdfsTestDir, "targetfile4");
				argv = new string[] { "-cp", "-ptopa", src.ToUri().ToString(), target4.ToUri().ToString
					() };
				ret = ToolRunner.Run(shell, argv);
				NUnit.Framework.Assert.AreEqual("cp -ptopa is not working", Success, ret);
				targetStatus = fs.GetFileStatus(target4);
				NUnit.Framework.Assert.AreEqual(mtime, targetStatus.GetModificationTime());
				NUnit.Framework.Assert.AreEqual(atime, targetStatus.GetAccessTime());
				NUnit.Framework.Assert.AreEqual(owner, targetStatus.GetOwner());
				NUnit.Framework.Assert.AreEqual(group, targetStatus.GetGroup());
				targetPerm = targetStatus.GetPermission();
				NUnit.Framework.Assert.IsTrue(perm.Equals(targetPerm));
				xattrs = fs.GetXAttrs(target4);
				NUnit.Framework.Assert.IsTrue(xattrs.IsEmpty());
				acls = fs.GetAclStatus(target4).GetEntries();
				NUnit.Framework.Assert.IsFalse(acls.IsEmpty());
				NUnit.Framework.Assert.IsTrue(targetPerm.GetAclBit());
				NUnit.Framework.Assert.AreEqual(fs.GetAclStatus(src), fs.GetAclStatus(target4));
				// -ptoa (verify -pa option will preserve permissions also)
				Path target5 = new Path(hdfsTestDir, "targetfile5");
				argv = new string[] { "-cp", "-ptoa", src.ToUri().ToString(), target5.ToUri().ToString
					() };
				ret = ToolRunner.Run(shell, argv);
				NUnit.Framework.Assert.AreEqual("cp -ptoa is not working", Success, ret);
				targetStatus = fs.GetFileStatus(target5);
				NUnit.Framework.Assert.AreEqual(mtime, targetStatus.GetModificationTime());
				NUnit.Framework.Assert.AreEqual(atime, targetStatus.GetAccessTime());
				NUnit.Framework.Assert.AreEqual(owner, targetStatus.GetOwner());
				NUnit.Framework.Assert.AreEqual(group, targetStatus.GetGroup());
				targetPerm = targetStatus.GetPermission();
				NUnit.Framework.Assert.IsTrue(perm.Equals(targetPerm));
				xattrs = fs.GetXAttrs(target5);
				NUnit.Framework.Assert.IsTrue(xattrs.IsEmpty());
				acls = fs.GetAclStatus(target5).GetEntries();
				NUnit.Framework.Assert.IsFalse(acls.IsEmpty());
				NUnit.Framework.Assert.IsTrue(targetPerm.GetAclBit());
				NUnit.Framework.Assert.AreEqual(fs.GetAclStatus(src), fs.GetAclStatus(target5));
			}
			finally
			{
				if (null != shell)
				{
					shell.Close();
				}
				if (null != fs)
				{
					fs.Delete(hdfsTestDir, true);
					fs.Close();
				}
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCopyCommandsWithRawXAttrs()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeXattrsEnabledKey, true);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Format(
				true).Build();
			FsShell shell = null;
			FileSystem fs = null;
			string testdir = "/tmp/TestDFSShell-testCopyCommandsWithRawXAttrs-" + counter.GetAndIncrement
				();
			Path hdfsTestDir = new Path(testdir);
			Path rawHdfsTestDir = new Path("/.reserved/raw" + testdir);
			try
			{
				fs = cluster.GetFileSystem();
				fs.Mkdirs(hdfsTestDir);
				Path src = new Path(hdfsTestDir, "srcfile");
				string rawSrcBase = "/.reserved/raw" + testdir;
				Path rawSrc = new Path(rawSrcBase, "srcfile");
				fs.Create(src).Close();
				Path srcDir = new Path(hdfsTestDir, "srcdir");
				Path rawSrcDir = new Path("/.reserved/raw" + testdir, "srcdir");
				fs.Mkdirs(srcDir);
				Path srcDirFile = new Path(srcDir, "srcfile");
				Path rawSrcDirFile = new Path("/.reserved/raw" + srcDirFile);
				fs.Create(srcDirFile).Close();
				Path[] paths = new Path[] { rawSrc, rawSrcDir, rawSrcDirFile };
				string[] xattrNames = new string[] { UserA1, RawA1 };
				byte[][] xattrVals = new byte[][] { UserA1Value, RawA1Value };
				for (int i = 0; i < paths.Length; i++)
				{
					for (int j = 0; j < xattrNames.Length; j++)
					{
						fs.SetXAttr(paths[i], xattrNames[j], xattrVals[j]);
					}
				}
				shell = new FsShell(conf);
				/* Check that a file as the source path works ok. */
				DoTestCopyCommandsWithRawXAttrs(shell, fs, src, hdfsTestDir, false);
				DoTestCopyCommandsWithRawXAttrs(shell, fs, rawSrc, hdfsTestDir, false);
				DoTestCopyCommandsWithRawXAttrs(shell, fs, src, rawHdfsTestDir, false);
				DoTestCopyCommandsWithRawXAttrs(shell, fs, rawSrc, rawHdfsTestDir, true);
				/* Use a relative /.reserved/raw path. */
				Path savedWd = fs.GetWorkingDirectory();
				try
				{
					fs.SetWorkingDirectory(new Path(rawSrcBase));
					Path relRawSrc = new Path("../srcfile");
					Path relRawHdfsTestDir = new Path("..");
					DoTestCopyCommandsWithRawXAttrs(shell, fs, relRawSrc, relRawHdfsTestDir, true);
				}
				finally
				{
					fs.SetWorkingDirectory(savedWd);
				}
				/* Check that a directory as the source path works ok. */
				DoTestCopyCommandsWithRawXAttrs(shell, fs, srcDir, hdfsTestDir, false);
				DoTestCopyCommandsWithRawXAttrs(shell, fs, rawSrcDir, hdfsTestDir, false);
				DoTestCopyCommandsWithRawXAttrs(shell, fs, srcDir, rawHdfsTestDir, false);
				DoTestCopyCommandsWithRawXAttrs(shell, fs, rawSrcDir, rawHdfsTestDir, true);
				/* Use relative in an absolute path. */
				string relRawSrcDir = "./.reserved/../.reserved/raw/../raw" + testdir + "/srcdir";
				string relRawDstDir = "./.reserved/../.reserved/raw/../raw" + testdir;
				DoTestCopyCommandsWithRawXAttrs(shell, fs, new Path(relRawSrcDir), new Path(relRawDstDir
					), true);
			}
			finally
			{
				if (null != shell)
				{
					shell.Close();
				}
				if (null != fs)
				{
					fs.Delete(hdfsTestDir, true);
					fs.Close();
				}
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		private void DoTestCopyCommandsWithRawXAttrs(FsShell shell, FileSystem fs, Path src
			, Path hdfsTestDir, bool expectRaw)
		{
			Path target;
			bool srcIsRaw;
			if (src.IsAbsolute())
			{
				srcIsRaw = src.ToString().Contains("/.reserved/raw");
			}
			else
			{
				srcIsRaw = new Path(fs.GetWorkingDirectory(), src).ToString().Contains("/.reserved/raw"
					);
			}
			bool destIsRaw = hdfsTestDir.ToString().Contains("/.reserved/raw");
			bool srcDestMismatch = srcIsRaw ^ destIsRaw;
			// -p (possibly preserve raw if src & dst are both /.r/r */
			if (srcDestMismatch)
			{
				DoCopyAndTest(shell, hdfsTestDir, src, "-p", Error);
			}
			else
			{
				target = DoCopyAndTest(shell, hdfsTestDir, src, "-p", Success);
				CheckXAttrs(fs, target, expectRaw, false);
			}
			// -px (possibly preserve raw, always preserve non-raw xattrs. */
			if (srcDestMismatch)
			{
				DoCopyAndTest(shell, hdfsTestDir, src, "-px", Error);
			}
			else
			{
				target = DoCopyAndTest(shell, hdfsTestDir, src, "-px", Success);
				CheckXAttrs(fs, target, expectRaw, true);
			}
			// no args (possibly preserve raw, never preserve non-raw xattrs. */
			if (srcDestMismatch)
			{
				DoCopyAndTest(shell, hdfsTestDir, src, null, Error);
			}
			else
			{
				target = DoCopyAndTest(shell, hdfsTestDir, src, null, Success);
				CheckXAttrs(fs, target, expectRaw, false);
			}
		}

		/// <exception cref="System.Exception"/>
		private Path DoCopyAndTest(FsShell shell, Path dest, Path src, string cpArgs, int
			 expectedExitCode)
		{
			Path target = new Path(dest, "targetfile" + counter.GetAndIncrement());
			string[] argv = cpArgs == null ? new string[] { "-cp", src.ToUri().ToString(), target
				.ToUri().ToString() } : new string[] { "-cp", cpArgs, src.ToUri().ToString(), target
				.ToUri().ToString() };
			int ret = ToolRunner.Run(shell, argv);
			NUnit.Framework.Assert.AreEqual("cp -p is not working", expectedExitCode, ret);
			return target;
		}

		/// <exception cref="System.Exception"/>
		private void CheckXAttrs(FileSystem fs, Path target, bool expectRaw, bool expectVanillaXAttrs
			)
		{
			IDictionary<string, byte[]> xattrs = fs.GetXAttrs(target);
			int expectedCount = 0;
			if (expectRaw)
			{
				Assert.AssertArrayEquals("raw.a1 has incorrect value", RawA1Value, xattrs[RawA1]);
				expectedCount++;
			}
			if (expectVanillaXAttrs)
			{
				Assert.AssertArrayEquals("user.a1 has incorrect value", UserA1Value, xattrs[UserA1
					]);
				expectedCount++;
			}
			NUnit.Framework.Assert.AreEqual("xattrs size mismatch", expectedCount, xattrs.Count
				);
		}

		// verify cp -ptopxa option will preserve directory attributes.
		/// <exception cref="System.Exception"/>
		public virtual void TestCopyCommandsToDirectoryWithPreserveOption()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeXattrsEnabledKey, true);
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeAclsEnabledKey, true);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Format(
				true).Build();
			FsShell shell = null;
			FileSystem fs = null;
			string testdir = "/tmp/TestDFSShell-testCopyCommandsToDirectoryWithPreserveOption-"
				 + counter.GetAndIncrement();
			Path hdfsTestDir = new Path(testdir);
			try
			{
				fs = cluster.GetFileSystem();
				fs.Mkdirs(hdfsTestDir);
				Path srcDir = new Path(hdfsTestDir, "srcDir");
				fs.Mkdirs(srcDir);
				fs.SetAcl(srcDir, Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope.Access
					, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Access
					, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
					.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
					.Default, AclEntryType.Group, "bar", FsAction.ReadExecute), AclTestHelpers.AclEntry
					(AclEntryScope.Access, AclEntryType.Other, FsAction.Execute)));
				// set sticky bit
				fs.SetPermission(srcDir, new FsPermission(FsAction.All, FsAction.ReadExecute, FsAction
					.Execute, true));
				// Create a file in srcDir to check if modification time of
				// srcDir to be preserved after copying the file.
				// If cp -p command is to preserve modification time and then copy child
				// (srcFile), modification time will not be preserved.
				Path srcFile = new Path(srcDir, "srcFile");
				fs.Create(srcFile).Close();
				FileStatus status = fs.GetFileStatus(srcDir);
				long mtime = status.GetModificationTime();
				long atime = status.GetAccessTime();
				string owner = status.GetOwner();
				string group = status.GetGroup();
				FsPermission perm = status.GetPermission();
				fs.SetXAttr(srcDir, UserA1, UserA1Value);
				fs.SetXAttr(srcDir, TrustedA1, TrustedA1Value);
				shell = new FsShell(conf);
				// -p
				Path targetDir1 = new Path(hdfsTestDir, "targetDir1");
				string[] argv = new string[] { "-cp", "-p", srcDir.ToUri().ToString(), targetDir1
					.ToUri().ToString() };
				int ret = ToolRunner.Run(shell, argv);
				NUnit.Framework.Assert.AreEqual("cp -p is not working", Success, ret);
				FileStatus targetStatus = fs.GetFileStatus(targetDir1);
				NUnit.Framework.Assert.AreEqual(mtime, targetStatus.GetModificationTime());
				NUnit.Framework.Assert.AreEqual(atime, targetStatus.GetAccessTime());
				NUnit.Framework.Assert.AreEqual(owner, targetStatus.GetOwner());
				NUnit.Framework.Assert.AreEqual(group, targetStatus.GetGroup());
				FsPermission targetPerm = targetStatus.GetPermission();
				NUnit.Framework.Assert.IsTrue(perm.Equals(targetPerm));
				IDictionary<string, byte[]> xattrs = fs.GetXAttrs(targetDir1);
				NUnit.Framework.Assert.IsTrue(xattrs.IsEmpty());
				IList<AclEntry> acls = fs.GetAclStatus(targetDir1).GetEntries();
				NUnit.Framework.Assert.IsTrue(acls.IsEmpty());
				NUnit.Framework.Assert.IsFalse(targetPerm.GetAclBit());
				// -ptop
				Path targetDir2 = new Path(hdfsTestDir, "targetDir2");
				argv = new string[] { "-cp", "-ptop", srcDir.ToUri().ToString(), targetDir2.ToUri
					().ToString() };
				ret = ToolRunner.Run(shell, argv);
				NUnit.Framework.Assert.AreEqual("cp -ptop is not working", Success, ret);
				targetStatus = fs.GetFileStatus(targetDir2);
				NUnit.Framework.Assert.AreEqual(mtime, targetStatus.GetModificationTime());
				NUnit.Framework.Assert.AreEqual(atime, targetStatus.GetAccessTime());
				NUnit.Framework.Assert.AreEqual(owner, targetStatus.GetOwner());
				NUnit.Framework.Assert.AreEqual(group, targetStatus.GetGroup());
				targetPerm = targetStatus.GetPermission();
				NUnit.Framework.Assert.IsTrue(perm.Equals(targetPerm));
				xattrs = fs.GetXAttrs(targetDir2);
				NUnit.Framework.Assert.IsTrue(xattrs.IsEmpty());
				acls = fs.GetAclStatus(targetDir2).GetEntries();
				NUnit.Framework.Assert.IsTrue(acls.IsEmpty());
				NUnit.Framework.Assert.IsFalse(targetPerm.GetAclBit());
				// -ptopx
				Path targetDir3 = new Path(hdfsTestDir, "targetDir3");
				argv = new string[] { "-cp", "-ptopx", srcDir.ToUri().ToString(), targetDir3.ToUri
					().ToString() };
				ret = ToolRunner.Run(shell, argv);
				NUnit.Framework.Assert.AreEqual("cp -ptopx is not working", Success, ret);
				targetStatus = fs.GetFileStatus(targetDir3);
				NUnit.Framework.Assert.AreEqual(mtime, targetStatus.GetModificationTime());
				NUnit.Framework.Assert.AreEqual(atime, targetStatus.GetAccessTime());
				NUnit.Framework.Assert.AreEqual(owner, targetStatus.GetOwner());
				NUnit.Framework.Assert.AreEqual(group, targetStatus.GetGroup());
				targetPerm = targetStatus.GetPermission();
				NUnit.Framework.Assert.IsTrue(perm.Equals(targetPerm));
				xattrs = fs.GetXAttrs(targetDir3);
				NUnit.Framework.Assert.AreEqual(xattrs.Count, 2);
				Assert.AssertArrayEquals(UserA1Value, xattrs[UserA1]);
				Assert.AssertArrayEquals(TrustedA1Value, xattrs[TrustedA1]);
				acls = fs.GetAclStatus(targetDir3).GetEntries();
				NUnit.Framework.Assert.IsTrue(acls.IsEmpty());
				NUnit.Framework.Assert.IsFalse(targetPerm.GetAclBit());
				// -ptopa
				Path targetDir4 = new Path(hdfsTestDir, "targetDir4");
				argv = new string[] { "-cp", "-ptopa", srcDir.ToUri().ToString(), targetDir4.ToUri
					().ToString() };
				ret = ToolRunner.Run(shell, argv);
				NUnit.Framework.Assert.AreEqual("cp -ptopa is not working", Success, ret);
				targetStatus = fs.GetFileStatus(targetDir4);
				NUnit.Framework.Assert.AreEqual(mtime, targetStatus.GetModificationTime());
				NUnit.Framework.Assert.AreEqual(atime, targetStatus.GetAccessTime());
				NUnit.Framework.Assert.AreEqual(owner, targetStatus.GetOwner());
				NUnit.Framework.Assert.AreEqual(group, targetStatus.GetGroup());
				targetPerm = targetStatus.GetPermission();
				NUnit.Framework.Assert.IsTrue(perm.Equals(targetPerm));
				xattrs = fs.GetXAttrs(targetDir4);
				NUnit.Framework.Assert.IsTrue(xattrs.IsEmpty());
				acls = fs.GetAclStatus(targetDir4).GetEntries();
				NUnit.Framework.Assert.IsFalse(acls.IsEmpty());
				NUnit.Framework.Assert.IsTrue(targetPerm.GetAclBit());
				NUnit.Framework.Assert.AreEqual(fs.GetAclStatus(srcDir), fs.GetAclStatus(targetDir4
					));
				// -ptoa (verify -pa option will preserve permissions also)
				Path targetDir5 = new Path(hdfsTestDir, "targetDir5");
				argv = new string[] { "-cp", "-ptoa", srcDir.ToUri().ToString(), targetDir5.ToUri
					().ToString() };
				ret = ToolRunner.Run(shell, argv);
				NUnit.Framework.Assert.AreEqual("cp -ptoa is not working", Success, ret);
				targetStatus = fs.GetFileStatus(targetDir5);
				NUnit.Framework.Assert.AreEqual(mtime, targetStatus.GetModificationTime());
				NUnit.Framework.Assert.AreEqual(atime, targetStatus.GetAccessTime());
				NUnit.Framework.Assert.AreEqual(owner, targetStatus.GetOwner());
				NUnit.Framework.Assert.AreEqual(group, targetStatus.GetGroup());
				targetPerm = targetStatus.GetPermission();
				NUnit.Framework.Assert.IsTrue(perm.Equals(targetPerm));
				xattrs = fs.GetXAttrs(targetDir5);
				NUnit.Framework.Assert.IsTrue(xattrs.IsEmpty());
				acls = fs.GetAclStatus(targetDir5).GetEntries();
				NUnit.Framework.Assert.IsFalse(acls.IsEmpty());
				NUnit.Framework.Assert.IsTrue(targetPerm.GetAclBit());
				NUnit.Framework.Assert.AreEqual(fs.GetAclStatus(srcDir), fs.GetAclStatus(targetDir5
					));
			}
			finally
			{
				if (shell != null)
				{
					shell.Close();
				}
				if (fs != null)
				{
					fs.Delete(hdfsTestDir, true);
					fs.Close();
				}
				cluster.Shutdown();
			}
		}

		// Verify cp -pa option will preserve both ACL and sticky bit.
		/// <exception cref="System.Exception"/>
		public virtual void TestCopyCommandsPreserveAclAndStickyBit()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeAclsEnabledKey, true);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Format(
				true).Build();
			FsShell shell = null;
			FileSystem fs = null;
			string testdir = "/tmp/TestDFSShell-testCopyCommandsPreserveAclAndStickyBit-" + counter
				.GetAndIncrement();
			Path hdfsTestDir = new Path(testdir);
			try
			{
				fs = cluster.GetFileSystem();
				fs.Mkdirs(hdfsTestDir);
				Path src = new Path(hdfsTestDir, "srcfile");
				fs.Create(src).Close();
				fs.SetAcl(src, Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
					.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
					.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
					.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
					.Group, "bar", FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope.Access
					, AclEntryType.Other, FsAction.Execute)));
				// set sticky bit
				fs.SetPermission(src, new FsPermission(FsAction.All, FsAction.ReadExecute, FsAction
					.Execute, true));
				FileStatus status = fs.GetFileStatus(src);
				long mtime = status.GetModificationTime();
				long atime = status.GetAccessTime();
				string owner = status.GetOwner();
				string group = status.GetGroup();
				FsPermission perm = status.GetPermission();
				shell = new FsShell(conf);
				// -p preserves sticky bit and doesn't preserve ACL
				Path target1 = new Path(hdfsTestDir, "targetfile1");
				string[] argv = new string[] { "-cp", "-p", src.ToUri().ToString(), target1.ToUri
					().ToString() };
				int ret = ToolRunner.Run(shell, argv);
				NUnit.Framework.Assert.AreEqual("cp is not working", Success, ret);
				FileStatus targetStatus = fs.GetFileStatus(target1);
				NUnit.Framework.Assert.AreEqual(mtime, targetStatus.GetModificationTime());
				NUnit.Framework.Assert.AreEqual(atime, targetStatus.GetAccessTime());
				NUnit.Framework.Assert.AreEqual(owner, targetStatus.GetOwner());
				NUnit.Framework.Assert.AreEqual(group, targetStatus.GetGroup());
				FsPermission targetPerm = targetStatus.GetPermission();
				NUnit.Framework.Assert.IsTrue(perm.Equals(targetPerm));
				IList<AclEntry> acls = fs.GetAclStatus(target1).GetEntries();
				NUnit.Framework.Assert.IsTrue(acls.IsEmpty());
				NUnit.Framework.Assert.IsFalse(targetPerm.GetAclBit());
				// -ptopa preserves both sticky bit and ACL
				Path target2 = new Path(hdfsTestDir, "targetfile2");
				argv = new string[] { "-cp", "-ptopa", src.ToUri().ToString(), target2.ToUri().ToString
					() };
				ret = ToolRunner.Run(shell, argv);
				NUnit.Framework.Assert.AreEqual("cp -ptopa is not working", Success, ret);
				targetStatus = fs.GetFileStatus(target2);
				NUnit.Framework.Assert.AreEqual(mtime, targetStatus.GetModificationTime());
				NUnit.Framework.Assert.AreEqual(atime, targetStatus.GetAccessTime());
				NUnit.Framework.Assert.AreEqual(owner, targetStatus.GetOwner());
				NUnit.Framework.Assert.AreEqual(group, targetStatus.GetGroup());
				targetPerm = targetStatus.GetPermission();
				NUnit.Framework.Assert.IsTrue(perm.Equals(targetPerm));
				acls = fs.GetAclStatus(target2).GetEntries();
				NUnit.Framework.Assert.IsFalse(acls.IsEmpty());
				NUnit.Framework.Assert.IsTrue(targetPerm.GetAclBit());
				NUnit.Framework.Assert.AreEqual(fs.GetAclStatus(src), fs.GetAclStatus(target2));
			}
			finally
			{
				if (null != shell)
				{
					shell.Close();
				}
				if (null != fs)
				{
					fs.Delete(hdfsTestDir, true);
					fs.Close();
				}
				cluster.Shutdown();
			}
		}

		// force Copy Option is -f
		/// <exception cref="System.Exception"/>
		public virtual void TestCopyCommandsWithForceOption()
		{
			Configuration conf = new Configuration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Format(
				true).Build();
			FsShell shell = null;
			FileSystem fs = null;
			FilePath localFile = new FilePath(TestRootDir, "testFileForPut");
			string localfilepath = new Path(localFile.GetAbsolutePath()).ToUri().ToString();
			string testdir = "/tmp/TestDFSShell-testCopyCommandsWithForceOption-" + counter.GetAndIncrement
				();
			Path hdfsTestDir = new Path(testdir);
			try
			{
				fs = cluster.GetFileSystem();
				fs.Mkdirs(hdfsTestDir);
				localFile.CreateNewFile();
				WriteFile(fs, new Path(testdir, "testFileForPut"));
				shell = new FsShell();
				// Tests for put
				string[] argv = new string[] { "-put", "-f", localfilepath, testdir };
				int res = ToolRunner.Run(shell, argv);
				NUnit.Framework.Assert.AreEqual("put -f is not working", Success, res);
				argv = new string[] { "-put", localfilepath, testdir };
				res = ToolRunner.Run(shell, argv);
				NUnit.Framework.Assert.AreEqual("put command itself is able to overwrite the file"
					, Error, res);
				// Tests for copyFromLocal
				argv = new string[] { "-copyFromLocal", "-f", localfilepath, testdir };
				res = ToolRunner.Run(shell, argv);
				NUnit.Framework.Assert.AreEqual("copyFromLocal -f is not working", Success, res);
				argv = new string[] { "-copyFromLocal", localfilepath, testdir };
				res = ToolRunner.Run(shell, argv);
				NUnit.Framework.Assert.AreEqual("copyFromLocal command itself is able to overwrite the file"
					, Error, res);
				// Tests for cp
				argv = new string[] { "-cp", "-f", localfilepath, testdir };
				res = ToolRunner.Run(shell, argv);
				NUnit.Framework.Assert.AreEqual("cp -f is not working", Success, res);
				argv = new string[] { "-cp", localfilepath, testdir };
				res = ToolRunner.Run(shell, argv);
				NUnit.Framework.Assert.AreEqual("cp command itself is able to overwrite the file"
					, Error, res);
			}
			finally
			{
				if (null != shell)
				{
					shell.Close();
				}
				if (localFile.Exists())
				{
					localFile.Delete();
				}
				if (null != fs)
				{
					fs.Delete(hdfsTestDir, true);
					fs.Close();
				}
				cluster.Shutdown();
			}
		}

		// setrep for file and directory.
		/// <exception cref="System.Exception"/>
		public virtual void TestSetrep()
		{
			Configuration conf = new Configuration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Format(
				true).Build();
			FsShell shell = null;
			FileSystem fs = null;
			string testdir1 = "/tmp/TestDFSShell-testSetrep-" + counter.GetAndIncrement();
			string testdir2 = testdir1 + "/nestedDir";
			Path hdfsFile1 = new Path(testdir1, "testFileForSetrep");
			Path hdfsFile2 = new Path(testdir2, "testFileForSetrep");
			short oldRepFactor = (short)1;
			short newRepFactor = (short)3;
			try
			{
				string[] argv;
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				Assert.AssertThat(fs.Mkdirs(new Path(testdir2)), CoreMatchers.Is(true));
				shell = new FsShell(conf);
				fs.Create(hdfsFile1, true).Close();
				fs.Create(hdfsFile2, true).Close();
				// Tests for setrep on a file.
				argv = new string[] { "-setrep", newRepFactor.ToString(), hdfsFile1.ToString() };
				Assert.AssertThat(shell.Run(argv), CoreMatchers.Is(Success));
				Assert.AssertThat(fs.GetFileStatus(hdfsFile1).GetReplication(), CoreMatchers.Is(newRepFactor
					));
				Assert.AssertThat(fs.GetFileStatus(hdfsFile2).GetReplication(), CoreMatchers.Is(oldRepFactor
					));
				// Tests for setrep
				// Tests for setrep on a directory and make sure it is applied recursively.
				argv = new string[] { "-setrep", newRepFactor.ToString(), testdir1 };
				Assert.AssertThat(shell.Run(argv), CoreMatchers.Is(Success));
				Assert.AssertThat(fs.GetFileStatus(hdfsFile1).GetReplication(), CoreMatchers.Is(newRepFactor
					));
				Assert.AssertThat(fs.GetFileStatus(hdfsFile2).GetReplication(), CoreMatchers.Is(newRepFactor
					));
			}
			finally
			{
				if (shell != null)
				{
					shell.Close();
				}
				cluster.Shutdown();
			}
		}

		/// <summary>Delete a file optionally configuring trash on the server and client.</summary>
		/// <exception cref="System.Exception"/>
		private void DeleteFileUsingTrash(bool serverTrash, bool clientTrash)
		{
			// Run a cluster, optionally with trash enabled on the server
			Configuration serverConf = new HdfsConfiguration();
			if (serverTrash)
			{
				serverConf.SetLong(CommonConfigurationKeysPublic.FsTrashIntervalKey, 1);
			}
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(serverConf).NumDataNodes(1).Format
				(true).Build();
			Configuration clientConf = new Configuration(serverConf);
			// Create a client, optionally with trash enabled
			if (clientTrash)
			{
				clientConf.SetLong(CommonConfigurationKeysPublic.FsTrashIntervalKey, 1);
			}
			else
			{
				clientConf.SetLong(CommonConfigurationKeysPublic.FsTrashIntervalKey, 0);
			}
			FsShell shell = new FsShell(clientConf);
			FileSystem fs = null;
			try
			{
				// Create and delete a file
				fs = cluster.GetFileSystem();
				// Use a separate tmp dir for each invocation.
				string testdir = "/tmp/TestDFSShell-deleteFileUsingTrash-" + counter.GetAndIncrement
					();
				WriteFile(fs, new Path(testdir, "foo"));
				string testFile = testdir + "/foo";
				string trashFile = shell.GetCurrentTrashDir() + "/" + testFile;
				string[] argv = new string[] { "-rm", testFile };
				int res = ToolRunner.Run(shell, argv);
				NUnit.Framework.Assert.AreEqual("rm failed", 0, res);
				if (serverTrash)
				{
					// If the server config was set we should use it unconditionally
					NUnit.Framework.Assert.IsTrue("File not in trash", fs.Exists(new Path(trashFile))
						);
				}
				else
				{
					if (clientTrash)
					{
						// If the server config was not set but the client config was
						// set then we should use it
						NUnit.Framework.Assert.IsTrue("File not in trashed", fs.Exists(new Path(trashFile
							)));
					}
					else
					{
						// If neither was set then we should not have trashed the file
						NUnit.Framework.Assert.IsFalse("File was not removed", fs.Exists(new Path(testFile
							)));
						NUnit.Framework.Assert.IsFalse("File was trashed", fs.Exists(new Path(trashFile))
							);
					}
				}
			}
			finally
			{
				if (fs != null)
				{
					fs.Close();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAppendToFile()
		{
			int inputFileLength = 1024 * 1024;
			FilePath testRoot = new FilePath(TestRootDir, "testAppendtoFileDir");
			testRoot.Mkdirs();
			FilePath file1 = new FilePath(testRoot, "file1");
			FilePath file2 = new FilePath(testRoot, "file2");
			CreateLocalFileWithRandomData(inputFileLength, file1);
			CreateLocalFileWithRandomData(inputFileLength, file2);
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.WaitActive();
			try
			{
				FileSystem dfs = cluster.GetFileSystem();
				NUnit.Framework.Assert.IsTrue("Not a HDFS: " + dfs.GetUri(), dfs is DistributedFileSystem
					);
				// Run appendToFile once, make sure that the target file is
				// created and is of the right size.
				Path remoteFile = new Path("/remoteFile");
				FsShell shell = new FsShell();
				shell.SetConf(conf);
				string[] argv = new string[] { "-appendToFile", file1.ToString(), file2.ToString(
					), remoteFile.ToString() };
				int res = ToolRunner.Run(shell, argv);
				Assert.AssertThat(res, CoreMatchers.Is(0));
				Assert.AssertThat(dfs.GetFileStatus(remoteFile).GetLen(), CoreMatchers.Is((long)inputFileLength
					 * 2));
				// Run the command once again and make sure that the target file
				// size has been doubled.
				res = ToolRunner.Run(shell, argv);
				Assert.AssertThat(res, CoreMatchers.Is(0));
				Assert.AssertThat(dfs.GetFileStatus(remoteFile).GetLen(), CoreMatchers.Is((long)inputFileLength
					 * 4));
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAppendToFileBadArgs()
		{
			int inputFileLength = 1024 * 1024;
			FilePath testRoot = new FilePath(TestRootDir, "testAppendToFileBadArgsDir");
			testRoot.Mkdirs();
			FilePath file1 = new FilePath(testRoot, "file1");
			CreateLocalFileWithRandomData(inputFileLength, file1);
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.WaitActive();
			try
			{
				FileSystem dfs = cluster.GetFileSystem();
				NUnit.Framework.Assert.IsTrue("Not a HDFS: " + dfs.GetUri(), dfs is DistributedFileSystem
					);
				// Run appendToFile with insufficient arguments.
				FsShell shell = new FsShell();
				shell.SetConf(conf);
				string[] argv = new string[] { "-appendToFile", file1.ToString() };
				int res = ToolRunner.Run(shell, argv);
				Assert.AssertThat(res, CoreMatchers.Not(0));
				// Mix stdin with other input files. Must fail.
				Path remoteFile = new Path("/remoteFile");
				argv = new string[] { "-appendToFile", file1.ToString(), "-", remoteFile.ToString
					() };
				res = ToolRunner.Run(shell, argv);
				Assert.AssertThat(res, CoreMatchers.Not(0));
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSetXAttrPermission()
		{
			UserGroupInformation user = UserGroupInformation.CreateUserForTesting("user", new 
				string[] { "mygroup" });
			MiniDFSCluster cluster = null;
			TextWriter bak = null;
			try
			{
				Configuration conf = new HdfsConfiguration();
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				cluster.WaitActive();
				FileSystem fs = cluster.GetFileSystem();
				Path p = new Path("/foo");
				fs.Mkdirs(p);
				bak = System.Console.Error;
				FsShell fshell = new FsShell(conf);
				ByteArrayOutputStream @out = new ByteArrayOutputStream();
				Runtime.SetErr(new TextWriter(@out));
				// No permission to write xattr
				fs.SetPermission(p, new FsPermission((short)0x1c0));
				user.DoAs(new _PrivilegedExceptionAction_2478(fshell, @out));
				int ret = ToolRunner.Run(fshell, new string[] { "-setfattr", "-n", "user.a1", "-v"
					, "1234", "/foo" });
				NUnit.Framework.Assert.AreEqual("Returned should be 0", 0, ret);
				@out.Reset();
				// No permission to read and remove
				fs.SetPermission(p, new FsPermission((short)0x1e8));
				user.DoAs(new _PrivilegedExceptionAction_2499(fshell, @out));
			}
			finally
			{
				// Read
				// Remove
				if (bak != null)
				{
					Runtime.SetErr(bak);
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		private sealed class _PrivilegedExceptionAction_2478 : PrivilegedExceptionAction<
			object>
		{
			public _PrivilegedExceptionAction_2478(FsShell fshell, ByteArrayOutputStream @out
				)
			{
				this.fshell = fshell;
				this.@out = @out;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				int ret = ToolRunner.Run(fshell, new string[] { "-setfattr", "-n", "user.a1", "-v"
					, "1234", "/foo" });
				NUnit.Framework.Assert.AreEqual("Returned should be 1", 1, ret);
				string str = @out.ToString();
				NUnit.Framework.Assert.IsTrue("Permission denied printed", str.IndexOf("Permission denied"
					) != -1);
				@out.Reset();
				return null;
			}

			private readonly FsShell fshell;

			private readonly ByteArrayOutputStream @out;
		}

		private sealed class _PrivilegedExceptionAction_2499 : PrivilegedExceptionAction<
			object>
		{
			public _PrivilegedExceptionAction_2499(FsShell fshell, ByteArrayOutputStream @out
				)
			{
				this.fshell = fshell;
				this.@out = @out;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				int ret = ToolRunner.Run(fshell, new string[] { "-getfattr", "-n", "user.a1", "/foo"
					 });
				NUnit.Framework.Assert.AreEqual("Returned should be 1", 1, ret);
				string str = @out.ToString();
				NUnit.Framework.Assert.IsTrue("Permission denied printed", str.IndexOf("Permission denied"
					) != -1);
				@out.Reset();
				ret = ToolRunner.Run(fshell, new string[] { "-setfattr", "-x", "user.a1", "/foo" }
					);
				NUnit.Framework.Assert.AreEqual("Returned should be 1", 1, ret);
				str = @out.ToString();
				NUnit.Framework.Assert.IsTrue("Permission denied printed", str.IndexOf("Permission denied"
					) != -1);
				@out.Reset();
				return null;
			}

			private readonly FsShell fshell;

			private readonly ByteArrayOutputStream @out;
		}

		/* HDFS-6413 xattr names erroneously handled as case-insensitive */
		/// <exception cref="System.Exception"/>
		public virtual void TestSetXAttrCaseSensitivity()
		{
			UserGroupInformation user = UserGroupInformation.CreateUserForTesting("user", new 
				string[] { "mygroup" });
			MiniDFSCluster cluster = null;
			TextWriter bak = null;
			try
			{
				Configuration conf = new HdfsConfiguration();
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				cluster.WaitActive();
				FileSystem fs = cluster.GetFileSystem();
				Path p = new Path("/mydir");
				fs.Mkdirs(p);
				bak = System.Console.Error;
				FsShell fshell = new FsShell(conf);
				ByteArrayOutputStream @out = new ByteArrayOutputStream();
				Runtime.SetOut(new TextWriter(@out));
				DoSetXattr(@out, fshell, new string[] { "-setfattr", "-n", "User.Foo", "/mydir" }
					, new string[] { "-getfattr", "-d", "/mydir" }, new string[] { "user.Foo" }, new 
					string[] {  });
				DoSetXattr(@out, fshell, new string[] { "-setfattr", "-n", "user.FOO", "/mydir" }
					, new string[] { "-getfattr", "-d", "/mydir" }, new string[] { "user.Foo", "user.FOO"
					 }, new string[] {  });
				DoSetXattr(@out, fshell, new string[] { "-setfattr", "-n", "USER.foo", "/mydir" }
					, new string[] { "-getfattr", "-d", "/mydir" }, new string[] { "user.Foo", "user.FOO"
					, "user.foo" }, new string[] {  });
				DoSetXattr(@out, fshell, new string[] { "-setfattr", "-n", "USER.fOo", "-v", "myval"
					, "/mydir" }, new string[] { "-getfattr", "-d", "/mydir" }, new string[] { "user.Foo"
					, "user.FOO", "user.foo", "user.fOo=\"myval\"" }, new string[] { "user.Foo=", "user.FOO="
					, "user.foo=" });
				DoSetXattr(@out, fshell, new string[] { "-setfattr", "-x", "useR.foo", "/mydir" }
					, new string[] { "-getfattr", "-d", "/mydir" }, new string[] { "user.Foo", "user.FOO"
					 }, new string[] { "foo" });
				DoSetXattr(@out, fshell, new string[] { "-setfattr", "-x", "USER.FOO", "/mydir" }
					, new string[] { "-getfattr", "-d", "/mydir" }, new string[] { "user.Foo" }, new 
					string[] { "FOO" });
				DoSetXattr(@out, fshell, new string[] { "-setfattr", "-x", "useR.Foo", "/mydir" }
					, new string[] { "-getfattr", "-n", "User.Foo", "/mydir" }, new string[] {  }, new 
					string[] { "Foo" });
			}
			finally
			{
				if (bak != null)
				{
					Runtime.SetOut(bak);
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private void DoSetXattr(ByteArrayOutputStream @out, FsShell fshell, string[] setOp
			, string[] getOp, string[] expectArr, string[] dontExpectArr)
		{
			int ret = ToolRunner.Run(fshell, setOp);
			@out.Reset();
			ret = ToolRunner.Run(fshell, getOp);
			string str = @out.ToString();
			for (int i = 0; i < expectArr.Length; i++)
			{
				string expect = expectArr[i];
				StringBuilder sb = new StringBuilder("Incorrect results from getfattr. Expected: "
					);
				sb.Append(expect).Append(" Full Result: ");
				sb.Append(str);
				NUnit.Framework.Assert.IsTrue(sb.ToString(), str.IndexOf(expect) != -1);
			}
			for (int i_1 = 0; i_1 < dontExpectArr.Length; i_1++)
			{
				string dontExpect = dontExpectArr[i_1];
				StringBuilder sb = new StringBuilder("Incorrect results from getfattr. Didn't Expect: "
					);
				sb.Append(dontExpect).Append(" Full Result: ");
				sb.Append(str);
				NUnit.Framework.Assert.IsTrue(sb.ToString(), str.IndexOf(dontExpect) == -1);
			}
			@out.Reset();
		}

		/// <summary>
		/// Test to make sure that user namespace xattrs can be set only if path has
		/// access and for sticky directorries, only owner/privileged user can write.
		/// </summary>
		/// <remarks>
		/// Test to make sure that user namespace xattrs can be set only if path has
		/// access and for sticky directorries, only owner/privileged user can write.
		/// Trusted namespace xattrs can be set only with privileged users.
		/// As user1: Create a directory (/foo) as user1, chown it to user1 (and
		/// user1's group), grant rwx to "other".
		/// As user2: Set an xattr (should pass with path access).
		/// As user1: Set an xattr (should pass).
		/// As user2: Read the xattr (should pass). Remove the xattr (should pass with
		/// path access).
		/// As user1: Read the xattr (should pass). Remove the xattr (should pass).
		/// As user1: Change permissions only to owner
		/// As User2: Set an Xattr (Should fail set with no path access) Remove an
		/// Xattr (Should fail with no path access)
		/// As SuperUser: Set an Xattr with Trusted (Should pass)
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestSetXAttrPermissionAsDifferentOwner()
		{
			string User1 = "user1";
			string Group1 = "supergroup";
			UserGroupInformation user1 = UserGroupInformation.CreateUserForTesting(User1, new 
				string[] { Group1 });
			UserGroupInformation user2 = UserGroupInformation.CreateUserForTesting("user2", new 
				string[] { "mygroup2" });
			UserGroupInformation Superuser = UserGroupInformation.GetCurrentUser();
			MiniDFSCluster cluster = null;
			TextWriter bak = null;
			try
			{
				Configuration conf = new HdfsConfiguration();
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				cluster.WaitActive();
				FileSystem fs = cluster.GetFileSystem();
				fs.SetOwner(new Path("/"), User1, Group1);
				bak = System.Console.Error;
				FsShell fshell = new FsShell(conf);
				ByteArrayOutputStream @out = new ByteArrayOutputStream();
				Runtime.SetErr(new TextWriter(@out));
				//Test 1.  Let user1 be owner for /foo
				user1.DoAs(new _PrivilegedExceptionAction_2683(fshell, @out));
				//Test 2. Give access to others
				user1.DoAs(new _PrivilegedExceptionAction_2695(fshell, @out));
				// Give access to "other"
				// Test 3. Should be allowed to write xattr if there is a path access to
				// user (user2).
				user2.DoAs(new _PrivilegedExceptionAction_2709(fshell, @out));
				//Test 4. There should be permission to write xattr for
				// the owning user with write permissions.
				user1.DoAs(new _PrivilegedExceptionAction_2722(fshell, @out));
				// Test 5. There should be permission to read non-owning user (user2) if
				// there is path access to that user and also can remove.
				user2.DoAs(new _PrivilegedExceptionAction_2735(fshell, @out));
				// Read
				// Remove
				// Test 6. There should be permission to read/remove for
				// the owning user with path access.
				user1.DoAs(new _PrivilegedExceptionAction_2754());
				// Test 7. Change permission to have path access only to owner(user1)
				user1.DoAs(new _PrivilegedExceptionAction_2762(fshell, @out));
				// Give access to "other"
				// Test 8. There should be no permissions to set for
				// the non-owning user with no path access.
				user2.DoAs(new _PrivilegedExceptionAction_2776(fshell, @out));
				// set
				// Test 9. There should be no permissions to remove for
				// the non-owning user with no path access.
				user2.DoAs(new _PrivilegedExceptionAction_2793(fshell, @out));
				// set
				// Test 10. Superuser should be allowed to set with trusted namespace
				Superuser.DoAs(new _PrivilegedExceptionAction_2809(fshell, @out));
			}
			finally
			{
				// set
				if (bak != null)
				{
					Runtime.SetErr(bak);
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		private sealed class _PrivilegedExceptionAction_2683 : PrivilegedExceptionAction<
			object>
		{
			public _PrivilegedExceptionAction_2683(FsShell fshell, ByteArrayOutputStream @out
				)
			{
				this.fshell = fshell;
				this.@out = @out;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				int ret = ToolRunner.Run(fshell, new string[] { "-mkdir", "/foo" });
				NUnit.Framework.Assert.AreEqual("Return should be 0", 0, ret);
				@out.Reset();
				return null;
			}

			private readonly FsShell fshell;

			private readonly ByteArrayOutputStream @out;
		}

		private sealed class _PrivilegedExceptionAction_2695 : PrivilegedExceptionAction<
			object>
		{
			public _PrivilegedExceptionAction_2695(FsShell fshell, ByteArrayOutputStream @out
				)
			{
				this.fshell = fshell;
				this.@out = @out;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				int ret = ToolRunner.Run(fshell, new string[] { "-chmod", "707", "/foo" });
				NUnit.Framework.Assert.AreEqual("Return should be 0", 0, ret);
				@out.Reset();
				return null;
			}

			private readonly FsShell fshell;

			private readonly ByteArrayOutputStream @out;
		}

		private sealed class _PrivilegedExceptionAction_2709 : PrivilegedExceptionAction<
			object>
		{
			public _PrivilegedExceptionAction_2709(FsShell fshell, ByteArrayOutputStream @out
				)
			{
				this.fshell = fshell;
				this.@out = @out;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				int ret = ToolRunner.Run(fshell, new string[] { "-setfattr", "-n", "user.a1", "-v"
					, "1234", "/foo" });
				NUnit.Framework.Assert.AreEqual("Returned should be 0", 0, ret);
				@out.Reset();
				return null;
			}

			private readonly FsShell fshell;

			private readonly ByteArrayOutputStream @out;
		}

		private sealed class _PrivilegedExceptionAction_2722 : PrivilegedExceptionAction<
			object>
		{
			public _PrivilegedExceptionAction_2722(FsShell fshell, ByteArrayOutputStream @out
				)
			{
				this.fshell = fshell;
				this.@out = @out;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				int ret = ToolRunner.Run(fshell, new string[] { "-setfattr", "-n", "user.a1", "-v"
					, "1234", "/foo" });
				NUnit.Framework.Assert.AreEqual("Returned should be 0", 0, ret);
				@out.Reset();
				return null;
			}

			private readonly FsShell fshell;

			private readonly ByteArrayOutputStream @out;
		}

		private sealed class _PrivilegedExceptionAction_2735 : PrivilegedExceptionAction<
			object>
		{
			public _PrivilegedExceptionAction_2735(FsShell fshell, ByteArrayOutputStream @out
				)
			{
				this.fshell = fshell;
				this.@out = @out;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				int ret = ToolRunner.Run(fshell, new string[] { "-getfattr", "-n", "user.a1", "/foo"
					 });
				NUnit.Framework.Assert.AreEqual("Returned should be 0", 0, ret);
				@out.Reset();
				ret = ToolRunner.Run(fshell, new string[] { "-setfattr", "-x", "user.a1", "/foo" }
					);
				NUnit.Framework.Assert.AreEqual("Returned should be 0", 0, ret);
				@out.Reset();
				return null;
			}

			private readonly FsShell fshell;

			private readonly ByteArrayOutputStream @out;
		}

		private sealed class _PrivilegedExceptionAction_2754 : PrivilegedExceptionAction<
			object>
		{
			public _PrivilegedExceptionAction_2754()
			{
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				return null;
			}
		}

		private sealed class _PrivilegedExceptionAction_2762 : PrivilegedExceptionAction<
			object>
		{
			public _PrivilegedExceptionAction_2762(FsShell fshell, ByteArrayOutputStream @out
				)
			{
				this.fshell = fshell;
				this.@out = @out;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				int ret = ToolRunner.Run(fshell, new string[] { "-chmod", "700", "/foo" });
				NUnit.Framework.Assert.AreEqual("Return should be 0", 0, ret);
				@out.Reset();
				return null;
			}

			private readonly FsShell fshell;

			private readonly ByteArrayOutputStream @out;
		}

		private sealed class _PrivilegedExceptionAction_2776 : PrivilegedExceptionAction<
			object>
		{
			public _PrivilegedExceptionAction_2776(FsShell fshell, ByteArrayOutputStream @out
				)
			{
				this.fshell = fshell;
				this.@out = @out;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				int ret = ToolRunner.Run(fshell, new string[] { "-setfattr", "-n", "user.a2", "/foo"
					 });
				NUnit.Framework.Assert.AreEqual("Returned should be 1", 1, ret);
				string str = @out.ToString();
				NUnit.Framework.Assert.IsTrue("Permission denied printed", str.IndexOf("Permission denied"
					) != -1);
				@out.Reset();
				return null;
			}

			private readonly FsShell fshell;

			private readonly ByteArrayOutputStream @out;
		}

		private sealed class _PrivilegedExceptionAction_2793 : PrivilegedExceptionAction<
			object>
		{
			public _PrivilegedExceptionAction_2793(FsShell fshell, ByteArrayOutputStream @out
				)
			{
				this.fshell = fshell;
				this.@out = @out;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				int ret = ToolRunner.Run(fshell, new string[] { "-setfattr", "-x", "user.a2", "/foo"
					 });
				NUnit.Framework.Assert.AreEqual("Returned should be 1", 1, ret);
				string str = @out.ToString();
				NUnit.Framework.Assert.IsTrue("Permission denied printed", str.IndexOf("Permission denied"
					) != -1);
				@out.Reset();
				return null;
			}

			private readonly FsShell fshell;

			private readonly ByteArrayOutputStream @out;
		}

		private sealed class _PrivilegedExceptionAction_2809 : PrivilegedExceptionAction<
			object>
		{
			public _PrivilegedExceptionAction_2809(FsShell fshell, ByteArrayOutputStream @out
				)
			{
				this.fshell = fshell;
				this.@out = @out;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				int ret = ToolRunner.Run(fshell, new string[] { "-setfattr", "-n", "trusted.a3", 
					"/foo" });
				NUnit.Framework.Assert.AreEqual("Returned should be 0", 0, ret);
				@out.Reset();
				return null;
			}

			private readonly FsShell fshell;

			private readonly ByteArrayOutputStream @out;
		}

		/*
		* 1. Test that CLI throws an exception and returns non-0 when user does
		* not have permission to read an xattr.
		* 2. Test that CLI throws an exception and returns non-0 when a non-existent
		* xattr is requested.
		*/
		/// <exception cref="System.Exception"/>
		public virtual void TestGetFAttrErrors()
		{
			UserGroupInformation user = UserGroupInformation.CreateUserForTesting("user", new 
				string[] { "mygroup" });
			MiniDFSCluster cluster = null;
			TextWriter bakErr = null;
			try
			{
				Configuration conf = new HdfsConfiguration();
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				cluster.WaitActive();
				FileSystem fs = cluster.GetFileSystem();
				Path p = new Path("/foo");
				fs.Mkdirs(p);
				bakErr = System.Console.Error;
				FsShell fshell = new FsShell(conf);
				ByteArrayOutputStream @out = new ByteArrayOutputStream();
				Runtime.SetErr(new TextWriter(@out));
				// No permission for "other".
				fs.SetPermission(p, new FsPermission((short)0x1c0));
				{
					int ret = ToolRunner.Run(fshell, new string[] { "-setfattr", "-n", "user.a1", "-v"
						, "1234", "/foo" });
					NUnit.Framework.Assert.AreEqual("Returned should be 0", 0, ret);
					@out.Reset();
				}
				user.DoAs(new _PrivilegedExceptionAction_2866(fshell, @out));
				{
					int ret = ToolRunner.Run(fshell, new string[] { "-getfattr", "-n", "user.nonexistent"
						, "/foo" });
					string str = @out.ToString();
					NUnit.Framework.Assert.IsTrue("xattr value was incorrectly returned", str.IndexOf
						("getfattr: At least one of the attributes provided was not found") >= 0);
					@out.Reset();
				}
			}
			finally
			{
				if (bakErr != null)
				{
					Runtime.SetErr(bakErr);
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		private sealed class _PrivilegedExceptionAction_2866 : PrivilegedExceptionAction<
			object>
		{
			public _PrivilegedExceptionAction_2866(FsShell fshell, ByteArrayOutputStream @out
				)
			{
				this.fshell = fshell;
				this.@out = @out;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				int ret = ToolRunner.Run(fshell, new string[] { "-getfattr", "-n", "user.a1", "/foo"
					 });
				string str = @out.ToString();
				NUnit.Framework.Assert.IsTrue("xattr value was incorrectly returned", str.IndexOf
					("1234") == -1);
				@out.Reset();
				return null;
			}

			private readonly FsShell fshell;

			private readonly ByteArrayOutputStream @out;
		}

		/// <summary>
		/// Test that the server trash configuration is respected when
		/// the client configuration is not set.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestServerConfigRespected()
		{
			DeleteFileUsingTrash(true, false);
		}

		/// <summary>
		/// Test that server trash configuration is respected even when the
		/// client configuration is set.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestServerConfigRespectedWithClient()
		{
			DeleteFileUsingTrash(true, true);
		}

		/// <summary>
		/// Test that the client trash configuration is respected when
		/// the server configuration is not set.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestClientConfigRespected()
		{
			DeleteFileUsingTrash(false, true);
		}

		/// <summary>Test that trash is disabled by default.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestNoTrashConfig()
		{
			DeleteFileUsingTrash(false, false);
		}
	}
}
