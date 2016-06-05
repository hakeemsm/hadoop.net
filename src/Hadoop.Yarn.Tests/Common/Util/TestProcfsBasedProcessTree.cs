using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util
{
	/// <summary>A JUnit test to test ProcfsBasedProcessTree.</summary>
	public class TestProcfsBasedProcessTree
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestProcfsBasedProcessTree
			));

		protected internal static FilePath TestRootDir = new FilePath("target", typeof(TestProcfsBasedProcessTree
			).FullName + "-localDir");

		private Shell.ShellCommandExecutor shexec = null;

		private string pidFile;

		private string lowestDescendant;

		private string shellScript;

		private const int N = 6;

		private class RogueTaskThread : Sharpen.Thread
		{
			// Controls the RogueTask
			public override void Run()
			{
				try
				{
					Vector<string> args = new Vector<string>();
					if (TestProcfsBasedProcessTree.IsSetsidAvailable())
					{
						args.AddItem("setsid");
					}
					args.AddItem("bash");
					args.AddItem("-c");
					args.AddItem(" echo $$ > " + this._enclosing.pidFile + "; sh " + this._enclosing.
						shellScript + " " + TestProcfsBasedProcessTree.N + ";");
					this._enclosing.shexec = new Shell.ShellCommandExecutor(Sharpen.Collections.ToArray
						(args, new string[0]));
					this._enclosing.shexec.Execute();
				}
				catch (Shell.ExitCodeException ee)
				{
					TestProcfsBasedProcessTree.Log.Info("Shell Command exit with a non-zero exit code. This is"
						 + " expected as we are killing the subprocesses of the" + " task intentionally. "
						 + ee);
				}
				catch (IOException ioe)
				{
					TestProcfsBasedProcessTree.Log.Info("Error executing shell command " + ioe);
				}
				finally
				{
					TestProcfsBasedProcessTree.Log.Info("Exit code: " + this._enclosing.shexec.GetExitCode
						());
				}
			}

			internal RogueTaskThread(TestProcfsBasedProcessTree _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestProcfsBasedProcessTree _enclosing;
		}

		private string GetRogueTaskPID()
		{
			FilePath f = new FilePath(pidFile);
			while (!f.Exists())
			{
				try
				{
					Sharpen.Thread.Sleep(500);
				}
				catch (Exception)
				{
					break;
				}
			}
			// read from pidFile
			return GetPidFromPidFile(pidFile);
		}

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			Assume.AssumeTrue(Shell.Linux);
			FileContext.GetLocalFSFileContext().Delete(new Path(TestRootDir.GetAbsolutePath()
				), true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestProcessTree()
		{
			try
			{
				NUnit.Framework.Assert.IsTrue(ProcfsBasedProcessTree.IsAvailable());
			}
			catch (Exception e)
			{
				Log.Info(StringUtils.StringifyException(e));
				NUnit.Framework.Assert.IsTrue("ProcfsBaseProcessTree should be available on Linux"
					, false);
				return;
			}
			// create shell script
			Random rm = new Random();
			FilePath tempFile = new FilePath(TestRootDir, GetType().FullName + "_shellScript_"
				 + rm.Next() + ".sh");
			tempFile.DeleteOnExit();
			shellScript = TestRootDir + FilePath.separator + tempFile.GetName();
			// create pid file
			tempFile = new FilePath(TestRootDir, GetType().FullName + "_pidFile_" + rm.Next()
				 + ".pid");
			tempFile.DeleteOnExit();
			pidFile = TestRootDir + FilePath.separator + tempFile.GetName();
			lowestDescendant = TestRootDir + FilePath.separator + "lowestDescendantPidFile";
			// write to shell-script
			try
			{
				FileWriter fWriter = new FileWriter(shellScript);
				fWriter.Write("# rogue task\n" + "sleep 1\n" + "echo hello\n" + "if [ $1 -ne 0 ]\n"
					 + "then\n" + " sh " + shellScript + " $(($1-1))\n" + "else\n" + " echo $$ > " +
					 lowestDescendant + "\n" + " while true\n do\n" + "  sleep 5\n" + " done\n" + "fi"
					);
				fWriter.Close();
			}
			catch (IOException ioe)
			{
				Log.Info("Error: " + ioe);
				return;
			}
			Sharpen.Thread t = new TestProcfsBasedProcessTree.RogueTaskThread(this);
			t.Start();
			string pid = GetRogueTaskPID();
			Log.Info("Root process pid: " + pid);
			ProcfsBasedProcessTree p = CreateProcessTree(pid);
			p.UpdateProcessTree();
			// initialize
			Log.Info("ProcessTree: " + p.ToString());
			FilePath leaf = new FilePath(lowestDescendant);
			// wait till lowest descendant process of Rougue Task starts execution
			while (!leaf.Exists())
			{
				try
				{
					Sharpen.Thread.Sleep(500);
				}
				catch (Exception)
				{
					break;
				}
			}
			p.UpdateProcessTree();
			// reconstruct
			Log.Info("ProcessTree: " + p.ToString());
			// Get the process-tree dump
			string processTreeDump = p.GetProcessTreeDump();
			// destroy the process and all its subprocesses
			DestroyProcessTree(pid);
			bool isAlive = true;
			for (int tries = 100; tries > 0; tries--)
			{
				if (IsSetsidAvailable())
				{
					// whole processtree
					isAlive = IsAnyProcessInTreeAlive(p);
				}
				else
				{
					// process
					isAlive = IsAlive(pid);
				}
				if (!isAlive)
				{
					break;
				}
				Sharpen.Thread.Sleep(100);
			}
			if (isAlive)
			{
				NUnit.Framework.Assert.Fail("ProcessTree shouldn't be alive");
			}
			Log.Info("Process-tree dump follows: \n" + processTreeDump);
			NUnit.Framework.Assert.IsTrue("Process-tree dump doesn't start with a proper header"
				, processTreeDump.StartsWith("\t|- PID PPID PGRPID SESSID CMD_NAME " + "USER_MODE_TIME(MILLIS) SYSTEM_TIME(MILLIS) VMEM_USAGE(BYTES) "
				 + "RSSMEM_USAGE(PAGES) FULL_CMD_LINE\n"));
			for (int i = N; i >= 0; i--)
			{
				string cmdLineDump = "\\|- [0-9]+ [0-9]+ [0-9]+ [0-9]+ \\(sh\\)" + " [0-9]+ [0-9]+ [0-9]+ [0-9]+ sh "
					 + shellScript + " " + i;
				Sharpen.Pattern pat = Sharpen.Pattern.Compile(cmdLineDump);
				Matcher mat = pat.Matcher(processTreeDump);
				NUnit.Framework.Assert.IsTrue("Process-tree dump doesn't contain the cmdLineDump of "
					 + i + "th process!", mat.Find());
			}
			// Not able to join thread sometimes when forking with large N.
			try
			{
				t.Join(2000);
				Log.Info("RogueTaskThread successfully joined.");
			}
			catch (Exception)
			{
				Log.Info("Interrupted while joining RogueTaskThread.");
			}
			// ProcessTree is gone now. Any further calls should be sane.
			p.UpdateProcessTree();
			NUnit.Framework.Assert.IsFalse("ProcessTree must have been gone", IsAlive(pid));
			NUnit.Framework.Assert.IsTrue("vmem for the gone-process is " + p.GetVirtualMemorySize
				() + " . It should be zero.", p.GetVirtualMemorySize() == 0);
			NUnit.Framework.Assert.IsTrue("vmem (old API) for the gone-process is " + p.GetCumulativeVmem
				() + " . It should be zero.", p.GetCumulativeVmem() == 0);
			NUnit.Framework.Assert.IsTrue(p.ToString().Equals("[ ]"));
		}

		protected internal virtual ProcfsBasedProcessTree CreateProcessTree(string pid)
		{
			return new ProcfsBasedProcessTree(pid);
		}

		protected internal virtual ProcfsBasedProcessTree CreateProcessTree(string pid, string
			 procfsRootDir, Clock clock)
		{
			return new ProcfsBasedProcessTree(pid, procfsRootDir, clock);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void DestroyProcessTree(string pid)
		{
			SendSignal(pid, 9);
		}

		/// <summary>Get PID from a pid-file.</summary>
		/// <param name="pidFileName">Name of the pid-file.</param>
		/// <returns>
		/// the PID string read from the pid-file. Returns null if the
		/// pidFileName points to a non-existing file or if read fails from the
		/// file.
		/// </returns>
		public static string GetPidFromPidFile(string pidFileName)
		{
			BufferedReader pidFile = null;
			FileReader fReader = null;
			string pid = null;
			try
			{
				fReader = new FileReader(pidFileName);
				pidFile = new BufferedReader(fReader);
			}
			catch (FileNotFoundException)
			{
				Log.Debug("PidFile doesn't exist : " + pidFileName);
				return pid;
			}
			try
			{
				pid = pidFile.ReadLine();
			}
			catch (IOException)
			{
				Log.Error("Failed to read from " + pidFileName);
			}
			finally
			{
				try
				{
					if (fReader != null)
					{
						fReader.Close();
					}
					try
					{
						if (pidFile != null)
						{
							pidFile.Close();
						}
					}
					catch (IOException)
					{
						Log.Warn("Error closing the stream " + pidFile);
					}
				}
				catch (IOException)
				{
					Log.Warn("Error closing the stream " + fReader);
				}
			}
			return pid;
		}

		public class ProcessStatInfo
		{
			internal string pid;

			internal string name;

			internal string ppid;

			internal string pgrpId;

			internal string session;

			internal string vmem = "0";

			internal string rssmemPage = "0";

			internal string utime = "0";

			internal string stime = "0";

			public ProcessStatInfo(string[] statEntries)
			{
				// sample stat in a single line : 3910 (gpm) S 1 3910 3910 0 -1 4194624
				// 83 0 0 0 0 0 0 0 16 0 1 0 7852 2408448 88 4294967295 134512640
				// 134590050 3220521392 3220520036 10975138 0 0 4096 134234626
				// 4294967295 0 0 17 1 0 0
				pid = statEntries[0];
				name = statEntries[1];
				ppid = statEntries[2];
				pgrpId = statEntries[3];
				session = statEntries[4];
				vmem = statEntries[5];
				if (statEntries.Length > 6)
				{
					rssmemPage = statEntries[6];
				}
				if (statEntries.Length > 7)
				{
					utime = statEntries[7];
					stime = statEntries[8];
				}
			}

			// construct a line that mimics the procfs stat file.
			// all unused numerical entries are set to 0.
			public virtual string GetStatLine()
			{
				return string.Format("%s (%s) S %s %s %s 0 0 0" + " 0 0 0 0 %s %s 0 0 0 0 0 0 0 %s %s 0 0"
					 + " 0 0 0 0 0 0 0 0" + " 0 0 0 0 0", pid, name, ppid, pgrpId, session, utime, stime
					, vmem, rssmemPage);
			}
		}

		public virtual ProcfsBasedProcessTree.ProcessSmapMemoryInfo ConstructMemoryMappingInfo
			(string address, string[] entries)
		{
			ProcfsBasedProcessTree.ProcessSmapMemoryInfo info = new ProcfsBasedProcessTree.ProcessSmapMemoryInfo
				(address);
			info.SetMemInfo(ProcfsBasedProcessTree.MemInfo.Size.ToString(), entries[0]);
			info.SetMemInfo(ProcfsBasedProcessTree.MemInfo.Rss.ToString(), entries[1]);
			info.SetMemInfo(ProcfsBasedProcessTree.MemInfo.Pss.ToString(), entries[2]);
			info.SetMemInfo(ProcfsBasedProcessTree.MemInfo.SharedClean.ToString(), entries[3]
				);
			info.SetMemInfo(ProcfsBasedProcessTree.MemInfo.SharedDirty.ToString(), entries[4]
				);
			info.SetMemInfo(ProcfsBasedProcessTree.MemInfo.PrivateClean.ToString(), entries[5
				]);
			info.SetMemInfo(ProcfsBasedProcessTree.MemInfo.PrivateDirty.ToString(), entries[6
				]);
			info.SetMemInfo(ProcfsBasedProcessTree.MemInfo.Referenced.ToString(), entries[7]);
			info.SetMemInfo(ProcfsBasedProcessTree.MemInfo.Anonymous.ToString(), entries[8]);
			info.SetMemInfo(ProcfsBasedProcessTree.MemInfo.AnonHugePages.ToString(), entries[
				9]);
			info.SetMemInfo(ProcfsBasedProcessTree.MemInfo.Swap.ToString(), entries[10]);
			info.SetMemInfo(ProcfsBasedProcessTree.MemInfo.KernelPageSize.ToString(), entries
				[11]);
			info.SetMemInfo(ProcfsBasedProcessTree.MemInfo.MmuPageSize.ToString(), entries[12
				]);
			return info;
		}

		public virtual void CreateMemoryMappingInfo(ProcfsBasedProcessTree.ProcessTreeSmapMemInfo
			[] procMemInfo)
		{
			for (int i = 0; i < procMemInfo.Length; i++)
			{
				// Construct 4 memory mappings per process.
				// As per min(Shared_Dirty, Pss) + Private_Clean + Private_Dirty
				// and not including r--s, r-xs, we should get 100 KB per process
				IList<ProcfsBasedProcessTree.ProcessSmapMemoryInfo> memoryMappingList = procMemInfo
					[i].GetMemoryInfoList();
				memoryMappingList.AddItem(ConstructMemoryMappingInfo("7f56c177c000-7f56c177d000 "
					 + "rw-p 00010000 08:02 40371558                   " + "/grid/0/jdk1.7.0_25/jre/lib/amd64/libnio.so"
					, new string[] { "4", "4", "25", "4", "25", "15", "10", "4", "0", "0", "0", "4", 
					"4" }));
				memoryMappingList.AddItem(ConstructMemoryMappingInfo("7fb09382e000-7fb09382f000 r--s 00003000 "
					 + "08:02 25953545", new string[] { "4", "4", "25", "4", "0", "15", "10", "4", "0"
					, "0", "0", "4", "4" }));
				memoryMappingList.AddItem(ConstructMemoryMappingInfo("7e8790000-7e8b80000 r-xs 00000000 00:00 0"
					, new string[] { "4", "4", "25", "4", "0", "15", "10", "4", "0", "0", "0", "4", 
					"4" }));
				memoryMappingList.AddItem(ConstructMemoryMappingInfo("7da677000-7e0dcf000 rw-p 00000000 00:00 0"
					, new string[] { "4", "4", "25", "4", "50", "15", "10", "4", "0", "0", "0", "4", 
					"4" }));
			}
		}

		/// <summary>A basic test that creates a few process directories and writes stat files.
		/// 	</summary>
		/// <remarks>
		/// A basic test that creates a few process directories and writes stat files.
		/// Verifies that the cpu time and memory is correctly computed.
		/// </remarks>
		/// <exception cref="System.IO.IOException">
		/// if there was a problem setting up the fake procfs directories or
		/// files.
		/// </exception>
		public virtual void TestCpuAndMemoryForProcessTree()
		{
			// test processes
			string[] pids = new string[] { "100", "200", "300", "400" };
			ControlledClock testClock = new ControlledClock(new SystemClock());
			testClock.SetTime(0);
			// create the fake procfs root directory.
			FilePath procfsRootDir = new FilePath(TestRootDir, "proc");
			try
			{
				SetupProcfsRootDir(procfsRootDir);
				SetupPidDirs(procfsRootDir, pids);
				// create stat objects.
				// assuming processes 100, 200, 300 are in tree and 400 is not.
				TestProcfsBasedProcessTree.ProcessStatInfo[] procInfos = new TestProcfsBasedProcessTree.ProcessStatInfo
					[4];
				procInfos[0] = new TestProcfsBasedProcessTree.ProcessStatInfo(new string[] { "100"
					, "proc1", "1", "100", "100", "100000", "100", "1000", "200" });
				procInfos[1] = new TestProcfsBasedProcessTree.ProcessStatInfo(new string[] { "200"
					, "proc2", "100", "100", "100", "200000", "200", "2000", "400" });
				procInfos[2] = new TestProcfsBasedProcessTree.ProcessStatInfo(new string[] { "300"
					, "proc3", "200", "100", "100", "300000", "300", "3000", "600" });
				procInfos[3] = new TestProcfsBasedProcessTree.ProcessStatInfo(new string[] { "400"
					, "proc4", "1", "400", "400", "400000", "400", "4000", "800" });
				ProcfsBasedProcessTree.ProcessTreeSmapMemInfo[] memInfo = new ProcfsBasedProcessTree.ProcessTreeSmapMemInfo
					[4];
				memInfo[0] = new ProcfsBasedProcessTree.ProcessTreeSmapMemInfo("100");
				memInfo[1] = new ProcfsBasedProcessTree.ProcessTreeSmapMemInfo("200");
				memInfo[2] = new ProcfsBasedProcessTree.ProcessTreeSmapMemInfo("300");
				memInfo[3] = new ProcfsBasedProcessTree.ProcessTreeSmapMemInfo("400");
				CreateMemoryMappingInfo(memInfo);
				WriteStatFiles(procfsRootDir, pids, procInfos, memInfo);
				// crank up the process tree class.
				Configuration conf = new Configuration();
				ProcfsBasedProcessTree processTree = CreateProcessTree("100", procfsRootDir.GetAbsolutePath
					(), testClock);
				processTree.SetConf(conf);
				// build the process tree.
				processTree.UpdateProcessTree();
				// verify virtual memory
				NUnit.Framework.Assert.AreEqual("Virtual memory does not match", 600000L, processTree
					.GetVirtualMemorySize());
				// verify rss memory
				long cumuRssMem = ProcfsBasedProcessTree.PageSize > 0 ? 600L * ProcfsBasedProcessTree
					.PageSize : ResourceCalculatorProcessTree.Unavailable;
				NUnit.Framework.Assert.AreEqual("rss memory does not match", cumuRssMem, processTree
					.GetRssMemorySize());
				// verify old API
				NUnit.Framework.Assert.AreEqual("rss memory (old API) does not match", cumuRssMem
					, processTree.GetCumulativeRssmem());
				// verify cumulative cpu time
				long cumuCpuTime = ProcfsBasedProcessTree.JiffyLengthInMillis > 0 ? 7200L * ProcfsBasedProcessTree
					.JiffyLengthInMillis : 0L;
				NUnit.Framework.Assert.AreEqual("Cumulative cpu time does not match", cumuCpuTime
					, processTree.GetCumulativeCpuTime());
				// verify CPU usage
				NUnit.Framework.Assert.AreEqual("Percent CPU time should be set to -1 initially", 
					-1.0, processTree.GetCpuUsagePercent(), 0.01);
				// Check by enabling smaps
				SetSmapsInProceTree(processTree, true);
				// RSS=Min(shared_dirty,PSS)+PrivateClean+PrivateDirty (exclude r-xs,
				// r--s)
				NUnit.Framework.Assert.AreEqual("rss memory does not match", (100 * ProcfsBasedProcessTree
					.KbToBytes * 3), processTree.GetRssMemorySize());
				// verify old API
				NUnit.Framework.Assert.AreEqual("rss memory (old API) does not match", (100 * ProcfsBasedProcessTree
					.KbToBytes * 3), processTree.GetCumulativeRssmem());
				// test the cpu time again to see if it cumulates
				procInfos[0] = new TestProcfsBasedProcessTree.ProcessStatInfo(new string[] { "100"
					, "proc1", "1", "100", "100", "100000", "100", "2000", "300" });
				procInfos[1] = new TestProcfsBasedProcessTree.ProcessStatInfo(new string[] { "200"
					, "proc2", "100", "100", "100", "200000", "200", "3000", "500" });
				WriteStatFiles(procfsRootDir, pids, procInfos, memInfo);
				long elapsedTimeBetweenUpdatesMsec = 200000;
				testClock.SetTime(elapsedTimeBetweenUpdatesMsec);
				// build the process tree.
				processTree.UpdateProcessTree();
				// verify cumulative cpu time again
				long prevCumuCpuTime = cumuCpuTime;
				cumuCpuTime = ProcfsBasedProcessTree.JiffyLengthInMillis > 0 ? 9400L * ProcfsBasedProcessTree
					.JiffyLengthInMillis : 0L;
				NUnit.Framework.Assert.AreEqual("Cumulative cpu time does not match", cumuCpuTime
					, processTree.GetCumulativeCpuTime());
				double expectedCpuUsagePercent = (ProcfsBasedProcessTree.JiffyLengthInMillis > 0)
					 ? (cumuCpuTime - prevCumuCpuTime) * 100.0 / elapsedTimeBetweenUpdatesMsec : 0;
				// expectedCpuUsagePercent is given by (94000L - 72000) * 100/
				//    200000;
				// which in this case is 11. Lets verify that first
				NUnit.Framework.Assert.AreEqual(11, expectedCpuUsagePercent, 0.001);
				NUnit.Framework.Assert.AreEqual("Percent CPU time is not correct expected " + expectedCpuUsagePercent
					, expectedCpuUsagePercent, processTree.GetCpuUsagePercent(), 0.01);
			}
			finally
			{
				FileUtil.FullyDelete(procfsRootDir);
			}
		}

		private void SetSmapsInProceTree(ProcfsBasedProcessTree processTree, bool enableFlag
			)
		{
			Configuration conf = processTree.GetConf();
			if (conf == null)
			{
				conf = new Configuration();
			}
			conf.SetBoolean(YarnConfiguration.ProcfsUseSmapsBasedRssEnabled, enableFlag);
			processTree.SetConf(conf);
			processTree.UpdateProcessTree();
		}

		/// <summary>
		/// Tests that cumulative memory is computed only for processes older than a
		/// given age.
		/// </summary>
		/// <exception cref="System.IO.IOException">
		/// if there was a problem setting up the fake procfs directories or
		/// files.
		/// </exception>
		public virtual void TestMemForOlderProcesses()
		{
			TestMemForOlderProcesses(false);
			TestMemForOlderProcesses(true);
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestMemForOlderProcesses(bool smapEnabled)
		{
			// initial list of processes
			string[] pids = new string[] { "100", "200", "300", "400" };
			// create the fake procfs root directory.
			FilePath procfsRootDir = new FilePath(TestRootDir, "proc");
			try
			{
				SetupProcfsRootDir(procfsRootDir);
				SetupPidDirs(procfsRootDir, pids);
				// create stat objects.
				// assuming 100, 200 and 400 are in tree, 300 is not.
				TestProcfsBasedProcessTree.ProcessStatInfo[] procInfos = new TestProcfsBasedProcessTree.ProcessStatInfo
					[4];
				procInfos[0] = new TestProcfsBasedProcessTree.ProcessStatInfo(new string[] { "100"
					, "proc1", "1", "100", "100", "100000", "100" });
				procInfos[1] = new TestProcfsBasedProcessTree.ProcessStatInfo(new string[] { "200"
					, "proc2", "100", "100", "100", "200000", "200" });
				procInfos[2] = new TestProcfsBasedProcessTree.ProcessStatInfo(new string[] { "300"
					, "proc3", "1", "300", "300", "300000", "300" });
				procInfos[3] = new TestProcfsBasedProcessTree.ProcessStatInfo(new string[] { "400"
					, "proc4", "100", "100", "100", "400000", "400" });
				// write smap information invariably for testing
				ProcfsBasedProcessTree.ProcessTreeSmapMemInfo[] memInfo = new ProcfsBasedProcessTree.ProcessTreeSmapMemInfo
					[4];
				memInfo[0] = new ProcfsBasedProcessTree.ProcessTreeSmapMemInfo("100");
				memInfo[1] = new ProcfsBasedProcessTree.ProcessTreeSmapMemInfo("200");
				memInfo[2] = new ProcfsBasedProcessTree.ProcessTreeSmapMemInfo("300");
				memInfo[3] = new ProcfsBasedProcessTree.ProcessTreeSmapMemInfo("400");
				CreateMemoryMappingInfo(memInfo);
				WriteStatFiles(procfsRootDir, pids, procInfos, memInfo);
				// crank up the process tree class.
				ProcfsBasedProcessTree processTree = CreateProcessTree("100", procfsRootDir.GetAbsolutePath
					(), new SystemClock());
				SetSmapsInProceTree(processTree, smapEnabled);
				// verify virtual memory
				NUnit.Framework.Assert.AreEqual("Virtual memory does not match", 700000L, processTree
					.GetVirtualMemorySize());
				NUnit.Framework.Assert.AreEqual("Virtual memory (old API) does not match", 700000L
					, processTree.GetCumulativeVmem());
				// write one more process as child of 100.
				string[] newPids = new string[] { "500" };
				SetupPidDirs(procfsRootDir, newPids);
				TestProcfsBasedProcessTree.ProcessStatInfo[] newProcInfos = new TestProcfsBasedProcessTree.ProcessStatInfo
					[1];
				newProcInfos[0] = new TestProcfsBasedProcessTree.ProcessStatInfo(new string[] { "500"
					, "proc5", "100", "100", "100", "500000", "500" });
				ProcfsBasedProcessTree.ProcessTreeSmapMemInfo[] newMemInfos = new ProcfsBasedProcessTree.ProcessTreeSmapMemInfo
					[1];
				newMemInfos[0] = new ProcfsBasedProcessTree.ProcessTreeSmapMemInfo("500");
				CreateMemoryMappingInfo(newMemInfos);
				WriteStatFiles(procfsRootDir, newPids, newProcInfos, newMemInfos);
				// check memory includes the new process.
				processTree.UpdateProcessTree();
				NUnit.Framework.Assert.AreEqual("vmem does not include new process", 1200000L, processTree
					.GetVirtualMemorySize());
				NUnit.Framework.Assert.AreEqual("vmem (old API) does not include new process", 1200000L
					, processTree.GetCumulativeVmem());
				if (!smapEnabled)
				{
					long cumuRssMem = ProcfsBasedProcessTree.PageSize > 0 ? 1200L * ProcfsBasedProcessTree
						.PageSize : ResourceCalculatorProcessTree.Unavailable;
					NUnit.Framework.Assert.AreEqual("rssmem does not include new process", cumuRssMem
						, processTree.GetRssMemorySize());
					// verify old API
					NUnit.Framework.Assert.AreEqual("rssmem (old API) does not include new process", 
						cumuRssMem, processTree.GetCumulativeRssmem());
				}
				else
				{
					NUnit.Framework.Assert.AreEqual("rssmem does not include new process", 100 * ProcfsBasedProcessTree
						.KbToBytes * 4, processTree.GetRssMemorySize());
					// verify old API
					NUnit.Framework.Assert.AreEqual("rssmem (old API) does not include new process", 
						100 * ProcfsBasedProcessTree.KbToBytes * 4, processTree.GetCumulativeRssmem());
				}
				// however processes older than 1 iteration will retain the older value
				NUnit.Framework.Assert.AreEqual("vmem shouldn't have included new process", 700000L
					, processTree.GetVirtualMemorySize(1));
				// verify old API
				NUnit.Framework.Assert.AreEqual("vmem (old API) shouldn't have included new process"
					, 700000L, processTree.GetCumulativeVmem(1));
				if (!smapEnabled)
				{
					long cumuRssMem = ProcfsBasedProcessTree.PageSize > 0 ? 700L * ProcfsBasedProcessTree
						.PageSize : ResourceCalculatorProcessTree.Unavailable;
					NUnit.Framework.Assert.AreEqual("rssmem shouldn't have included new process", cumuRssMem
						, processTree.GetRssMemorySize(1));
					// Verify old API
					NUnit.Framework.Assert.AreEqual("rssmem (old API) shouldn't have included new process"
						, cumuRssMem, processTree.GetCumulativeRssmem(1));
				}
				else
				{
					NUnit.Framework.Assert.AreEqual("rssmem shouldn't have included new process", 100
						 * ProcfsBasedProcessTree.KbToBytes * 3, processTree.GetRssMemorySize(1));
					// Verify old API
					NUnit.Framework.Assert.AreEqual("rssmem (old API) shouldn't have included new process"
						, 100 * ProcfsBasedProcessTree.KbToBytes * 3, processTree.GetCumulativeRssmem(1)
						);
				}
				// one more process
				newPids = new string[] { "600" };
				SetupPidDirs(procfsRootDir, newPids);
				newProcInfos = new TestProcfsBasedProcessTree.ProcessStatInfo[1];
				newProcInfos[0] = new TestProcfsBasedProcessTree.ProcessStatInfo(new string[] { "600"
					, "proc6", "100", "100", "100", "600000", "600" });
				newMemInfos = new ProcfsBasedProcessTree.ProcessTreeSmapMemInfo[1];
				newMemInfos[0] = new ProcfsBasedProcessTree.ProcessTreeSmapMemInfo("600");
				CreateMemoryMappingInfo(newMemInfos);
				WriteStatFiles(procfsRootDir, newPids, newProcInfos, newMemInfos);
				// refresh process tree
				processTree.UpdateProcessTree();
				// processes older than 2 iterations should be same as before.
				NUnit.Framework.Assert.AreEqual("vmem shouldn't have included new processes", 700000L
					, processTree.GetVirtualMemorySize(2));
				// verify old API
				NUnit.Framework.Assert.AreEqual("vmem (old API) shouldn't have included new processes"
					, 700000L, processTree.GetCumulativeVmem(2));
				if (!smapEnabled)
				{
					long cumuRssMem = ProcfsBasedProcessTree.PageSize > 0 ? 700L * ProcfsBasedProcessTree
						.PageSize : ResourceCalculatorProcessTree.Unavailable;
					NUnit.Framework.Assert.AreEqual("rssmem shouldn't have included new processes", cumuRssMem
						, processTree.GetRssMemorySize(2));
					// Verify old API
					NUnit.Framework.Assert.AreEqual("rssmem (old API) shouldn't have included new processes"
						, cumuRssMem, processTree.GetCumulativeRssmem(2));
				}
				else
				{
					NUnit.Framework.Assert.AreEqual("rssmem shouldn't have included new processes", 100
						 * ProcfsBasedProcessTree.KbToBytes * 3, processTree.GetRssMemorySize(2));
					// Verify old API
					NUnit.Framework.Assert.AreEqual("rssmem (old API) shouldn't have included new processes"
						, 100 * ProcfsBasedProcessTree.KbToBytes * 3, processTree.GetCumulativeRssmem(2)
						);
				}
				// processes older than 1 iteration should not include new process,
				// but include process 500
				NUnit.Framework.Assert.AreEqual("vmem shouldn't have included new processes", 1200000L
					, processTree.GetVirtualMemorySize(1));
				// verify old API
				NUnit.Framework.Assert.AreEqual("vmem (old API) shouldn't have included new processes"
					, 1200000L, processTree.GetCumulativeVmem(1));
				if (!smapEnabled)
				{
					long cumuRssMem = ProcfsBasedProcessTree.PageSize > 0 ? 1200L * ProcfsBasedProcessTree
						.PageSize : ResourceCalculatorProcessTree.Unavailable;
					NUnit.Framework.Assert.AreEqual("rssmem shouldn't have included new processes", cumuRssMem
						, processTree.GetRssMemorySize(1));
					// verify old API
					NUnit.Framework.Assert.AreEqual("rssmem (old API) shouldn't have included new processes"
						, cumuRssMem, processTree.GetCumulativeRssmem(1));
				}
				else
				{
					NUnit.Framework.Assert.AreEqual("rssmem shouldn't have included new processes", 100
						 * ProcfsBasedProcessTree.KbToBytes * 4, processTree.GetRssMemorySize(1));
					NUnit.Framework.Assert.AreEqual("rssmem (old API) shouldn't have included new processes"
						, 100 * ProcfsBasedProcessTree.KbToBytes * 4, processTree.GetCumulativeRssmem(1)
						);
				}
				// no processes older than 3 iterations
				NUnit.Framework.Assert.AreEqual("Getting non-zero vmem for processes older than 3 iterations"
					, 0, processTree.GetVirtualMemorySize(3));
				// verify old API
				NUnit.Framework.Assert.AreEqual("Getting non-zero vmem (old API) for processes older than 3 iterations"
					, 0, processTree.GetCumulativeVmem(3));
				NUnit.Framework.Assert.AreEqual("Getting non-zero rssmem for processes older than 3 iterations"
					, 0, processTree.GetRssMemorySize(3));
				// verify old API
				NUnit.Framework.Assert.AreEqual("Getting non-zero rssmem (old API) for processes older than 3 iterations"
					, 0, processTree.GetCumulativeRssmem(3));
			}
			finally
			{
				FileUtil.FullyDelete(procfsRootDir);
			}
		}

		/// <summary>
		/// Verifies ProcfsBasedProcessTree.checkPidPgrpidForMatch() in case of
		/// 'constructProcessInfo() returning null' by not writing stat file for the
		/// mock process
		/// </summary>
		/// <exception cref="System.IO.IOException">
		/// if there was a problem setting up the fake procfs directories or
		/// files.
		/// </exception>
		public virtual void TestDestroyProcessTree()
		{
			// test process
			string pid = "100";
			// create the fake procfs root directory.
			FilePath procfsRootDir = new FilePath(TestRootDir, "proc");
			try
			{
				SetupProcfsRootDir(procfsRootDir);
				// crank up the process tree class.
				CreateProcessTree(pid, procfsRootDir.GetAbsolutePath(), new SystemClock());
				// Let us not create stat file for pid 100.
				NUnit.Framework.Assert.IsTrue(ProcfsBasedProcessTree.CheckPidPgrpidForMatch(pid, 
					procfsRootDir.GetAbsolutePath()));
			}
			finally
			{
				FileUtil.FullyDelete(procfsRootDir);
			}
		}

		/// <summary>Test the correctness of process-tree dump.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestProcessTreeDump()
		{
			string[] pids = new string[] { "100", "200", "300", "400", "500", "600" };
			FilePath procfsRootDir = new FilePath(TestRootDir, "proc");
			try
			{
				SetupProcfsRootDir(procfsRootDir);
				SetupPidDirs(procfsRootDir, pids);
				int numProcesses = pids.Length;
				// Processes 200, 300, 400 and 500 are descendants of 100. 600 is not.
				TestProcfsBasedProcessTree.ProcessStatInfo[] procInfos = new TestProcfsBasedProcessTree.ProcessStatInfo
					[numProcesses];
				procInfos[0] = new TestProcfsBasedProcessTree.ProcessStatInfo(new string[] { "100"
					, "proc1", "1", "100", "100", "100000", "100", "1000", "200" });
				procInfos[1] = new TestProcfsBasedProcessTree.ProcessStatInfo(new string[] { "200"
					, "proc2", "100", "100", "100", "200000", "200", "2000", "400" });
				procInfos[2] = new TestProcfsBasedProcessTree.ProcessStatInfo(new string[] { "300"
					, "proc3", "200", "100", "100", "300000", "300", "3000", "600" });
				procInfos[3] = new TestProcfsBasedProcessTree.ProcessStatInfo(new string[] { "400"
					, "proc4", "200", "100", "100", "400000", "400", "4000", "800" });
				procInfos[4] = new TestProcfsBasedProcessTree.ProcessStatInfo(new string[] { "500"
					, "proc5", "400", "100", "100", "400000", "400", "4000", "800" });
				procInfos[5] = new TestProcfsBasedProcessTree.ProcessStatInfo(new string[] { "600"
					, "proc6", "1", "1", "1", "400000", "400", "4000", "800" });
				ProcfsBasedProcessTree.ProcessTreeSmapMemInfo[] memInfos = new ProcfsBasedProcessTree.ProcessTreeSmapMemInfo
					[6];
				memInfos[0] = new ProcfsBasedProcessTree.ProcessTreeSmapMemInfo("100");
				memInfos[1] = new ProcfsBasedProcessTree.ProcessTreeSmapMemInfo("200");
				memInfos[2] = new ProcfsBasedProcessTree.ProcessTreeSmapMemInfo("300");
				memInfos[3] = new ProcfsBasedProcessTree.ProcessTreeSmapMemInfo("400");
				memInfos[4] = new ProcfsBasedProcessTree.ProcessTreeSmapMemInfo("500");
				memInfos[5] = new ProcfsBasedProcessTree.ProcessTreeSmapMemInfo("600");
				string[] cmdLines = new string[numProcesses];
				cmdLines[0] = "proc1 arg1 arg2";
				cmdLines[1] = "proc2 arg3 arg4";
				cmdLines[2] = "proc3 arg5 arg6";
				cmdLines[3] = "proc4 arg7 arg8";
				cmdLines[4] = "proc5 arg9 arg10";
				cmdLines[5] = "proc6 arg11 arg12";
				CreateMemoryMappingInfo(memInfos);
				WriteStatFiles(procfsRootDir, pids, procInfos, memInfos);
				WriteCmdLineFiles(procfsRootDir, pids, cmdLines);
				ProcfsBasedProcessTree processTree = CreateProcessTree("100", procfsRootDir.GetAbsolutePath
					(), new SystemClock());
				// build the process tree.
				processTree.UpdateProcessTree();
				// Get the process-tree dump
				string processTreeDump = processTree.GetProcessTreeDump();
				Log.Info("Process-tree dump follows: \n" + processTreeDump);
				NUnit.Framework.Assert.IsTrue("Process-tree dump doesn't start with a proper header"
					, processTreeDump.StartsWith("\t|- PID PPID PGRPID SESSID CMD_NAME " + "USER_MODE_TIME(MILLIS) SYSTEM_TIME(MILLIS) VMEM_USAGE(BYTES) "
					 + "RSSMEM_USAGE(PAGES) FULL_CMD_LINE\n"));
				for (int i = 0; i < 5; i++)
				{
					TestProcfsBasedProcessTree.ProcessStatInfo p = procInfos[i];
					NUnit.Framework.Assert.IsTrue("Process-tree dump doesn't contain the cmdLineDump of process "
						 + p.pid, processTreeDump.Contains("\t|- " + p.pid + " " + p.ppid + " " + p.pgrpId
						 + " " + p.session + " (" + p.name + ") " + p.utime + " " + p.stime + " " + p.vmem
						 + " " + p.rssmemPage + " " + cmdLines[i]));
				}
				// 600 should not be in the dump
				TestProcfsBasedProcessTree.ProcessStatInfo p_1 = procInfos[5];
				NUnit.Framework.Assert.IsFalse("Process-tree dump shouldn't contain the cmdLineDump of process "
					 + p_1.pid, processTreeDump.Contains("\t|- " + p_1.pid + " " + p_1.ppid + " " + 
					p_1.pgrpId + " " + p_1.session + " (" + p_1.name + ") " + p_1.utime + " " + p_1.
					stime + " " + p_1.vmem + " " + cmdLines[5]));
			}
			finally
			{
				FileUtil.FullyDelete(procfsRootDir);
			}
		}

		protected internal static bool IsSetsidAvailable()
		{
			Shell.ShellCommandExecutor shexec = null;
			bool setsidSupported = true;
			try
			{
				string[] args = new string[] { "setsid", "bash", "-c", "echo $$" };
				shexec = new Shell.ShellCommandExecutor(args);
				shexec.Execute();
			}
			catch (IOException)
			{
				Log.Warn("setsid is not available on this machine. So not using it.");
				setsidSupported = false;
			}
			finally
			{
				// handle the exit code
				Log.Info("setsid exited with exit code " + shexec.GetExitCode());
			}
			return setsidSupported;
		}

		/// <summary>Is the root-process alive? Used only in tests.</summary>
		/// <returns>true if the root-process is alive, false otherwise.</returns>
		private static bool IsAlive(string pid)
		{
			try
			{
				string sigpid = IsSetsidAvailable() ? "-" + pid : pid;
				try
				{
					SendSignal(sigpid, 0);
				}
				catch (Shell.ExitCodeException)
				{
					return false;
				}
				return true;
			}
			catch (IOException)
			{
			}
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		private static void SendSignal(string pid, int signal)
		{
			Shell.ShellCommandExecutor shexec = null;
			string[] arg = new string[] { "kill", "-" + signal, pid };
			shexec = new Shell.ShellCommandExecutor(arg);
			shexec.Execute();
		}

		/// <summary>Is any of the subprocesses in the process-tree alive? Used only in tests.
		/// 	</summary>
		/// <returns>
		/// true if any of the processes in the process-tree is alive, false
		/// otherwise.
		/// </returns>
		private static bool IsAnyProcessInTreeAlive(ProcfsBasedProcessTree processTree)
		{
			foreach (string pId in processTree.GetCurrentProcessIDs())
			{
				if (IsAlive(pId))
				{
					return true;
				}
			}
			return false;
		}

		/// <summary>Create a directory to mimic the procfs file system's root.</summary>
		/// <param name="procfsRootDir">root directory to create.</param>
		/// <exception cref="System.IO.IOException">if could not delete the procfs root directory
		/// 	</exception>
		public static void SetupProcfsRootDir(FilePath procfsRootDir)
		{
			// cleanup any existing process root dir.
			if (procfsRootDir.Exists())
			{
				NUnit.Framework.Assert.IsTrue(FileUtil.FullyDelete(procfsRootDir));
			}
			// create afresh
			NUnit.Framework.Assert.IsTrue(procfsRootDir.Mkdirs());
		}

		/// <summary>Create PID directories under the specified procfs root directory</summary>
		/// <param name="procfsRootDir">root directory of procfs file system</param>
		/// <param name="pids">the PID directories to create.</param>
		/// <exception cref="System.IO.IOException">If PID dirs could not be created</exception>
		public static void SetupPidDirs(FilePath procfsRootDir, string[] pids)
		{
			foreach (string pid in pids)
			{
				FilePath pidDir = new FilePath(procfsRootDir, pid);
				pidDir.Mkdir();
				if (!pidDir.Exists())
				{
					throw new IOException("couldn't make process directory under " + "fake procfs");
				}
				else
				{
					Log.Info("created pid dir");
				}
			}
		}

		/// <summary>
		/// Write stat files under the specified pid directories with data setup in the
		/// corresponding ProcessStatInfo objects
		/// </summary>
		/// <param name="procfsRootDir">root directory of procfs file system</param>
		/// <param name="pids">the PID directories under which to create the stat file</param>
		/// <param name="procs">
		/// corresponding ProcessStatInfo objects whose data should be written
		/// to the stat files.
		/// </param>
		/// <exception cref="System.IO.IOException">if stat files could not be written</exception>
		public static void WriteStatFiles(FilePath procfsRootDir, string[] pids, TestProcfsBasedProcessTree.ProcessStatInfo
			[] procs, ProcfsBasedProcessTree.ProcessTreeSmapMemInfo[] smaps)
		{
			for (int i = 0; i < pids.Length; i++)
			{
				FilePath statFile = new FilePath(new FilePath(procfsRootDir, pids[i]), ProcfsBasedProcessTree
					.ProcfsStatFile);
				BufferedWriter bw = null;
				try
				{
					FileWriter fw = new FileWriter(statFile);
					bw = new BufferedWriter(fw);
					bw.Write(procs[i].GetStatLine());
					Log.Info("wrote stat file for " + pids[i] + " with contents: " + procs[i].GetStatLine
						());
				}
				finally
				{
					// not handling exception - will throw an error and fail the test.
					if (bw != null)
					{
						bw.Close();
					}
				}
				if (smaps != null)
				{
					FilePath smapFile = new FilePath(new FilePath(procfsRootDir, pids[i]), ProcfsBasedProcessTree
						.Smaps);
					bw = null;
					try
					{
						FileWriter fw = new FileWriter(smapFile);
						bw = new BufferedWriter(fw);
						bw.Write(smaps[i].ToString());
						bw.Flush();
						Log.Info("wrote smap file for " + pids[i] + " with contents: " + smaps[i].ToString
							());
					}
					finally
					{
						// not handling exception - will throw an error and fail the test.
						if (bw != null)
						{
							bw.Close();
						}
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void WriteCmdLineFiles(FilePath procfsRootDir, string[] pids, string
			[] cmdLines)
		{
			for (int i = 0; i < pids.Length; i++)
			{
				FilePath statFile = new FilePath(new FilePath(procfsRootDir, pids[i]), ProcfsBasedProcessTree
					.ProcfsCmdlineFile);
				BufferedWriter bw = null;
				try
				{
					bw = new BufferedWriter(new FileWriter(statFile));
					bw.Write(cmdLines[i]);
					Log.Info("wrote command-line file for " + pids[i] + " with contents: " + cmdLines
						[i]);
				}
				finally
				{
					// not handling exception - will throw an error and fail the test.
					if (bw != null)
					{
						bw.Close();
					}
				}
			}
		}
	}
}
