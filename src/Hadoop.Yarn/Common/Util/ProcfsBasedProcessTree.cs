using System;
using System.Collections.Generic;
using System.IO;
using System.Security;
using System.Text;
using Mono.Math;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util
{
	/// <summary>A Proc file-system based ProcessTree.</summary>
	/// <remarks>A Proc file-system based ProcessTree. Works only on Linux.</remarks>
	public class ProcfsBasedProcessTree : ResourceCalculatorProcessTree
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Util.ProcfsBasedProcessTree
			));

		private const string Procfs = "/proc/";

		private static readonly Sharpen.Pattern ProcfsStatFileFormat = Sharpen.Pattern.Compile
			("^([0-9-]+)\\s([^\\s]+)\\s[^\\s]\\s([0-9-]+)\\s([0-9-]+)\\s([0-9-]+)\\s" + "([0-9-]+\\s){7}([0-9]+)\\s([0-9]+)\\s([0-9-]+\\s){7}([0-9]+)\\s([0-9]+)"
			 + "(\\s[0-9-]+){15}");

		public const string ProcfsStatFile = "stat";

		public const string ProcfsCmdlineFile = "cmdline";

		public static readonly long PageSize;

		public static readonly long JiffyLengthInMillis;

		private readonly CpuTimeTracker cpuTimeTracker;

		private Clock clock;

		[System.Serializable]
		internal sealed class MemInfo
		{
			public static readonly ProcfsBasedProcessTree.MemInfo Size = new ProcfsBasedProcessTree.MemInfo
				("Size");

			public static readonly ProcfsBasedProcessTree.MemInfo Rss = new ProcfsBasedProcessTree.MemInfo
				("Rss");

			public static readonly ProcfsBasedProcessTree.MemInfo Pss = new ProcfsBasedProcessTree.MemInfo
				("Pss");

			public static readonly ProcfsBasedProcessTree.MemInfo SharedClean = new ProcfsBasedProcessTree.MemInfo
				("Shared_Clean");

			public static readonly ProcfsBasedProcessTree.MemInfo SharedDirty = new ProcfsBasedProcessTree.MemInfo
				("Shared_Dirty");

			public static readonly ProcfsBasedProcessTree.MemInfo PrivateClean = new ProcfsBasedProcessTree.MemInfo
				("Private_Clean");

			public static readonly ProcfsBasedProcessTree.MemInfo PrivateDirty = new ProcfsBasedProcessTree.MemInfo
				("Private_Dirty");

			public static readonly ProcfsBasedProcessTree.MemInfo Referenced = new ProcfsBasedProcessTree.MemInfo
				("Referenced");

			public static readonly ProcfsBasedProcessTree.MemInfo Anonymous = new ProcfsBasedProcessTree.MemInfo
				("Anonymous");

			public static readonly ProcfsBasedProcessTree.MemInfo AnonHugePages = new ProcfsBasedProcessTree.MemInfo
				("AnonHugePages");

			public static readonly ProcfsBasedProcessTree.MemInfo Swap = new ProcfsBasedProcessTree.MemInfo
				("swap");

			public static readonly ProcfsBasedProcessTree.MemInfo KernelPageSize = new ProcfsBasedProcessTree.MemInfo
				("kernelPageSize");

			public static readonly ProcfsBasedProcessTree.MemInfo MmuPageSize = new ProcfsBasedProcessTree.MemInfo
				("mmuPageSize");

			public static readonly ProcfsBasedProcessTree.MemInfo Invalid = new ProcfsBasedProcessTree.MemInfo
				("invalid");

			private string name;

			private MemInfo(string name)
			{
				// in millisecond
				this.name = name;
			}

			public static ProcfsBasedProcessTree.MemInfo GetMemInfoByName(string name)
			{
				foreach (ProcfsBasedProcessTree.MemInfo info in ProcfsBasedProcessTree.MemInfo.Values
					())
				{
					if (Sharpen.Runtime.EqualsIgnoreCase(info.name.Trim(), name.Trim()))
					{
						return info;
					}
				}
				return ProcfsBasedProcessTree.MemInfo.Invalid;
			}
		}

		public const string Smaps = "smaps";

		public const int KbToBytes = 1024;

		private const string Kb = "kB";

		private const string ReadOnlyWithSharedPermission = "r--s";

		private const string ReadExecuteWithSharedPermission = "r-xs";

		private static readonly Sharpen.Pattern AddressPattern = Sharpen.Pattern.Compile(
			"([[a-f]|(0-9)]*)-([[a-f]|(0-9)]*)(\\s)*([rxwps\\-]*)");

		private static readonly Sharpen.Pattern MemInfoPattern = Sharpen.Pattern.Compile(
			"(^[A-Z].*):[\\s ]*(.*)");

		private bool smapsEnabled;

		protected internal IDictionary<string, ProcfsBasedProcessTree.ProcessTreeSmapMemInfo
			> processSMAPTree = new Dictionary<string, ProcfsBasedProcessTree.ProcessTreeSmapMemInfo
			>();

		static ProcfsBasedProcessTree()
		{
			long jiffiesPerSecond = -1;
			long pageSize = -1;
			try
			{
				if (Shell.Linux)
				{
					Shell.ShellCommandExecutor shellExecutorClk = new Shell.ShellCommandExecutor(new 
						string[] { "getconf", "CLK_TCK" });
					shellExecutorClk.Execute();
					jiffiesPerSecond = long.Parse(shellExecutorClk.GetOutput().Replace("\n", string.Empty
						));
					Shell.ShellCommandExecutor shellExecutorPage = new Shell.ShellCommandExecutor(new 
						string[] { "getconf", "PAGESIZE" });
					shellExecutorPage.Execute();
					pageSize = long.Parse(shellExecutorPage.GetOutput().Replace("\n", string.Empty));
				}
			}
			catch (IOException e)
			{
				Log.Error(StringUtils.StringifyException(e));
			}
			finally
			{
				JiffyLengthInMillis = jiffiesPerSecond != -1 ? Math.Round(1000D / jiffiesPerSecond
					) : -1;
				PageSize = pageSize;
			}
		}

		private string procfsDir;

		private static string deadPid = "-1";

		private string pid = deadPid;

		private static Sharpen.Pattern numberPattern = Sharpen.Pattern.Compile("[1-9][0-9]*"
			);

		private long cpuTime = Unavailable;

		protected internal IDictionary<string, ProcfsBasedProcessTree.ProcessInfo> processTree
			 = new Dictionary<string, ProcfsBasedProcessTree.ProcessInfo>();

		public ProcfsBasedProcessTree(string pid)
			: this(pid, Procfs, new SystemClock())
		{
		}

		// to enable testing, using this variable which can be configured
		// to a test directory.
		public override void SetConf(Configuration conf)
		{
			base.SetConf(conf);
			if (conf != null)
			{
				smapsEnabled = conf.GetBoolean(YarnConfiguration.ProcfsUseSmapsBasedRssEnabled, YarnConfiguration
					.DefaultProcfsUseSmapsBasedRssEnabled);
			}
		}

		public ProcfsBasedProcessTree(string pid, string procfsDir)
			: this(pid, procfsDir, new SystemClock())
		{
		}

		/// <summary>Build a new process tree rooted at the pid.</summary>
		/// <remarks>
		/// Build a new process tree rooted at the pid.
		/// This method is provided mainly for testing purposes, where
		/// the root of the proc file system can be adjusted.
		/// </remarks>
		/// <param name="pid">root of the process tree</param>
		/// <param name="procfsDir">the root of a proc file system - only used for testing.</param>
		/// <param name="clock">clock for controlling time for testing</param>
		public ProcfsBasedProcessTree(string pid, string procfsDir, Clock clock)
			: base(pid)
		{
			this.clock = clock;
			this.pid = GetValidPID(pid);
			this.procfsDir = procfsDir;
			this.cpuTimeTracker = new CpuTimeTracker(JiffyLengthInMillis);
		}

		/// <summary>Checks if the ProcfsBasedProcessTree is available on this system.</summary>
		/// <returns>true if ProcfsBasedProcessTree is available. False otherwise.</returns>
		public static bool IsAvailable()
		{
			try
			{
				if (!Shell.Linux)
				{
					Log.Info("ProcfsBasedProcessTree currently is supported only on " + "Linux.");
					return false;
				}
			}
			catch (SecurityException se)
			{
				Log.Warn("Failed to get Operating System name. " + se);
				return false;
			}
			return true;
		}

		/// <summary>Update process-tree with latest state.</summary>
		/// <remarks>
		/// Update process-tree with latest state. If the root-process is not alive,
		/// tree will be empty.
		/// </remarks>
		public override void UpdateProcessTree()
		{
			if (!pid.Equals(deadPid))
			{
				// Get the list of processes
				IList<string> processList = GetProcessList();
				IDictionary<string, ProcfsBasedProcessTree.ProcessInfo> allProcessInfo = new Dictionary
					<string, ProcfsBasedProcessTree.ProcessInfo>();
				// cache the processTree to get the age for processes
				IDictionary<string, ProcfsBasedProcessTree.ProcessInfo> oldProcs = new Dictionary
					<string, ProcfsBasedProcessTree.ProcessInfo>(processTree);
				processTree.Clear();
				ProcfsBasedProcessTree.ProcessInfo me = null;
				foreach (string proc in processList)
				{
					// Get information for each process
					ProcfsBasedProcessTree.ProcessInfo pInfo = new ProcfsBasedProcessTree.ProcessInfo
						(proc);
					if (ConstructProcessInfo(pInfo, procfsDir) != null)
					{
						allProcessInfo[proc] = pInfo;
						if (proc.Equals(this.pid))
						{
							me = pInfo;
							// cache 'me'
							processTree[proc] = pInfo;
						}
					}
				}
				if (me == null)
				{
					return;
				}
				// Add each process to its parent.
				foreach (KeyValuePair<string, ProcfsBasedProcessTree.ProcessInfo> entry in allProcessInfo)
				{
					string pID = entry.Key;
					if (!pID.Equals("1"))
					{
						ProcfsBasedProcessTree.ProcessInfo pInfo = entry.Value;
						ProcfsBasedProcessTree.ProcessInfo parentPInfo = allProcessInfo[pInfo.GetPpid()];
						if (parentPInfo != null)
						{
							parentPInfo.AddChild(pInfo);
						}
					}
				}
				// now start constructing the process-tree
				List<ProcfsBasedProcessTree.ProcessInfo> pInfoQueue = new List<ProcfsBasedProcessTree.ProcessInfo
					>();
				Sharpen.Collections.AddAll(pInfoQueue, me.GetChildren());
				while (!pInfoQueue.IsEmpty())
				{
					ProcfsBasedProcessTree.ProcessInfo pInfo = pInfoQueue.Remove();
					if (!processTree.Contains(pInfo.GetPid()))
					{
						processTree[pInfo.GetPid()] = pInfo;
					}
					Sharpen.Collections.AddAll(pInfoQueue, pInfo.GetChildren());
				}
				// update age values and compute the number of jiffies since last update
				foreach (KeyValuePair<string, ProcfsBasedProcessTree.ProcessInfo> procs in processTree)
				{
					ProcfsBasedProcessTree.ProcessInfo oldInfo = oldProcs[procs.Key];
					if (procs.Value != null)
					{
						procs.Value.UpdateJiffy(oldInfo);
						if (oldInfo != null)
						{
							procs.Value.UpdateAge(oldInfo);
						}
					}
				}
				if (Log.IsDebugEnabled())
				{
					// Log.debug the ProcfsBasedProcessTree
					Log.Debug(this.ToString());
				}
				if (smapsEnabled)
				{
					//Update smaps info
					processSMAPTree.Clear();
					foreach (ProcfsBasedProcessTree.ProcessInfo p in processTree.Values)
					{
						if (p != null)
						{
							// Get information for each process
							ProcfsBasedProcessTree.ProcessTreeSmapMemInfo memInfo = new ProcfsBasedProcessTree.ProcessTreeSmapMemInfo
								(p.GetPid());
							ConstructProcessSMAPInfo(memInfo, procfsDir);
							processSMAPTree[p.GetPid()] = memInfo;
						}
					}
				}
			}
		}

		/// <summary>Verify that the given process id is same as its process group id.</summary>
		/// <returns>true if the process id matches else return false.</returns>
		public override bool CheckPidPgrpidForMatch()
		{
			return CheckPidPgrpidForMatch(pid, Procfs);
		}

		public static bool CheckPidPgrpidForMatch(string _pid, string procfs)
		{
			// Get information for this process
			ProcfsBasedProcessTree.ProcessInfo pInfo = new ProcfsBasedProcessTree.ProcessInfo
				(_pid);
			pInfo = ConstructProcessInfo(pInfo, procfs);
			// null if process group leader finished execution; issue no warning
			// make sure that pid and its pgrpId match
			if (pInfo == null)
			{
				return true;
			}
			string pgrpId = pInfo.GetPgrpId().ToString();
			return pgrpId.Equals(_pid);
		}

		private const string ProcesstreeDumpFormat = "\t|- %s %s %d %d %s %d %d %d %d %s%n";

		public virtual IList<string> GetCurrentProcessIDs()
		{
			IList<string> currentPIDs = new AList<string>();
			Sharpen.Collections.AddAll(currentPIDs, processTree.Keys);
			return currentPIDs;
		}

		/// <summary>Get a dump of the process-tree.</summary>
		/// <returns>
		/// a string concatenating the dump of information of all the processes
		/// in the process-tree
		/// </returns>
		public override string GetProcessTreeDump()
		{
			StringBuilder ret = new StringBuilder();
			// The header.
			ret.Append(string.Format("\t|- PID PPID PGRPID SESSID CMD_NAME " + "USER_MODE_TIME(MILLIS) SYSTEM_TIME(MILLIS) VMEM_USAGE(BYTES) "
				 + "RSSMEM_USAGE(PAGES) FULL_CMD_LINE%n"));
			foreach (ProcfsBasedProcessTree.ProcessInfo p in processTree.Values)
			{
				if (p != null)
				{
					ret.Append(string.Format(ProcesstreeDumpFormat, p.GetPid(), p.GetPpid(), p.GetPgrpId
						(), p.GetSessionId(), p.GetName(), p.GetUtime(), p.GetStime(), p.GetVmem(), p.GetRssmemPage
						(), p.GetCmdLine(procfsDir)));
				}
			}
			return ret.ToString();
		}

		public override long GetVirtualMemorySize(int olderThanAge)
		{
			long total = Unavailable;
			foreach (ProcfsBasedProcessTree.ProcessInfo p in processTree.Values)
			{
				if (p != null)
				{
					if (total == Unavailable)
					{
						total = 0;
					}
					if (p.GetAge() > olderThanAge)
					{
						total += p.GetVmem();
					}
				}
			}
			return total;
		}

		public override long GetCumulativeVmem(int olderThanAge)
		{
			return GetVirtualMemorySize(olderThanAge);
		}

		public override long GetRssMemorySize(int olderThanAge)
		{
			if (PageSize < 0)
			{
				return Unavailable;
			}
			if (smapsEnabled)
			{
				return GetSmapBasedRssMemorySize(olderThanAge);
			}
			bool isAvailable = false;
			long totalPages = 0;
			foreach (ProcfsBasedProcessTree.ProcessInfo p in processTree.Values)
			{
				if ((p != null))
				{
					if (p.GetAge() > olderThanAge)
					{
						totalPages += p.GetRssmemPage();
					}
					isAvailable = true;
				}
			}
			return isAvailable ? totalPages * PageSize : Unavailable;
		}

		// convert # pages to byte
		public override long GetCumulativeRssmem(int olderThanAge)
		{
			return GetRssMemorySize(olderThanAge);
		}

		/// <summary>
		/// Get the resident set size (RSS) memory used by all the processes
		/// in the process-tree that are older than the passed in age.
		/// </summary>
		/// <remarks>
		/// Get the resident set size (RSS) memory used by all the processes
		/// in the process-tree that are older than the passed in age. RSS is
		/// calculated based on SMAP information. Skip mappings with "r--s", "r-xs"
		/// permissions to get real RSS usage of the process.
		/// </remarks>
		/// <param name="olderThanAge">processes above this age are included in the memory addition
		/// 	</param>
		/// <returns>
		/// rss memory used by the process-tree in bytes, for
		/// processes older than this age. return
		/// <see cref="ResourceCalculatorProcessTree.Unavailable"/>
		/// if it cannot
		/// be calculated.
		/// </returns>
		private long GetSmapBasedRssMemorySize(int olderThanAge)
		{
			long total = Unavailable;
			foreach (ProcfsBasedProcessTree.ProcessInfo p in processTree.Values)
			{
				if (p != null)
				{
					// set resource to 0 instead of UNAVAILABLE
					if (total == Unavailable)
					{
						total = 0;
					}
					if (p.GetAge() > olderThanAge)
					{
						ProcfsBasedProcessTree.ProcessTreeSmapMemInfo procMemInfo = processSMAPTree[p.GetPid
							()];
						if (procMemInfo != null)
						{
							foreach (ProcfsBasedProcessTree.ProcessSmapMemoryInfo info in procMemInfo.GetMemoryInfoList
								())
							{
								// Do not account for r--s or r-xs mappings
								if (Sharpen.Runtime.EqualsIgnoreCase(info.GetPermission().Trim(), ReadOnlyWithSharedPermission
									) || Sharpen.Runtime.EqualsIgnoreCase(info.GetPermission().Trim(), ReadExecuteWithSharedPermission
									))
								{
									continue;
								}
								total += Math.Min(info.sharedDirty, info.pss) + info.privateDirty + info.privateClean;
								if (Log.IsDebugEnabled())
								{
									Log.Debug(" total(" + olderThanAge + "): PID : " + p.GetPid() + ", SharedDirty : "
										 + info.sharedDirty + ", PSS : " + info.pss + ", Private_Dirty : " + info.privateDirty
										 + ", Private_Clean : " + info.privateClean + ", total : " + (total * KbToBytes)
										);
								}
							}
						}
						if (Log.IsDebugEnabled())
						{
							Log.Debug(procMemInfo.ToString());
						}
					}
				}
			}
			if (total > 0)
			{
				total *= KbToBytes;
			}
			// convert to bytes
			Log.Info("SmapBasedCumulativeRssmem (bytes) : " + total);
			return total;
		}

		// size
		public override long GetCumulativeCpuTime()
		{
			if (JiffyLengthInMillis < 0)
			{
				return Unavailable;
			}
			long incJiffies = 0;
			bool isAvailable = false;
			foreach (ProcfsBasedProcessTree.ProcessInfo p in processTree.Values)
			{
				if (p != null)
				{
					incJiffies += p.GetDtime();
					// data is available
					isAvailable = true;
				}
			}
			if (isAvailable)
			{
				// reset cpuTime to 0 instead of UNAVAILABLE
				if (cpuTime == Unavailable)
				{
					cpuTime = 0L;
				}
				cpuTime += incJiffies * JiffyLengthInMillis;
			}
			return cpuTime;
		}

		private BigInteger GetTotalProcessJiffies()
		{
			BigInteger totalStime = BigInteger.Zero;
			long totalUtime = 0;
			foreach (ProcfsBasedProcessTree.ProcessInfo p in processTree.Values)
			{
				if (p != null)
				{
					totalUtime += p.GetUtime();
					totalStime = totalStime.Add(p.GetStime());
				}
			}
			return totalStime.Add(BigInteger.ValueOf(totalUtime));
		}

		public override float GetCpuUsagePercent()
		{
			BigInteger processTotalJiffies = GetTotalProcessJiffies();
			cpuTimeTracker.UpdateElapsedJiffies(processTotalJiffies, clock.GetTime());
			return cpuTimeTracker.GetCpuTrackerUsagePercent();
		}

		private static string GetValidPID(string pid)
		{
			if (pid == null)
			{
				return deadPid;
			}
			Matcher m = numberPattern.Matcher(pid);
			if (m.Matches())
			{
				return pid;
			}
			return deadPid;
		}

		/// <summary>Get the list of all processes in the system.</summary>
		private IList<string> GetProcessList()
		{
			string[] processDirs = (new FilePath(procfsDir)).List();
			IList<string> processList = new AList<string>();
			foreach (string dir in processDirs)
			{
				Matcher m = numberPattern.Matcher(dir);
				if (!m.Matches())
				{
					continue;
				}
				try
				{
					if ((new FilePath(procfsDir, dir)).IsDirectory())
					{
						processList.AddItem(dir);
					}
				}
				catch (SecurityException)
				{
				}
			}
			// skip this process
			return processList;
		}

		/// <summary>
		/// Construct the ProcessInfo using the process' PID and procfs rooted at the
		/// specified directory and return the same.
		/// </summary>
		/// <remarks>
		/// Construct the ProcessInfo using the process' PID and procfs rooted at the
		/// specified directory and return the same. It is provided mainly to assist
		/// testing purposes.
		/// Returns null on failing to read from procfs,
		/// </remarks>
		/// <param name="pinfo">ProcessInfo that needs to be updated</param>
		/// <param name="procfsDir">root of the proc file system</param>
		/// <returns>updated ProcessInfo, null on errors.</returns>
		private static ProcfsBasedProcessTree.ProcessInfo ConstructProcessInfo(ProcfsBasedProcessTree.ProcessInfo
			 pinfo, string procfsDir)
		{
			ProcfsBasedProcessTree.ProcessInfo ret = null;
			// Read "procfsDir/<pid>/stat" file - typically /proc/<pid>/stat
			BufferedReader @in = null;
			InputStreamReader fReader = null;
			try
			{
				FilePath pidDir = new FilePath(procfsDir, pinfo.GetPid());
				fReader = new InputStreamReader(new FileInputStream(new FilePath(pidDir, ProcfsStatFile
					)), Sharpen.Extensions.GetEncoding("UTF-8"));
				@in = new BufferedReader(fReader);
			}
			catch (FileNotFoundException)
			{
				// The process vanished in the interim!
				return ret;
			}
			ret = pinfo;
			try
			{
				string str = @in.ReadLine();
				// only one line
				Matcher m = ProcfsStatFileFormat.Matcher(str);
				bool mat = m.Find();
				if (mat)
				{
					// Set (name) (ppid) (pgrpId) (session) (utime) (stime) (vsize) (rss)
					pinfo.UpdateProcessInfo(m.Group(2), m.Group(3), System.Convert.ToInt32(m.Group(4)
						), System.Convert.ToInt32(m.Group(5)), long.Parse(m.Group(7)), new BigInteger(m.
						Group(8)), long.Parse(m.Group(10)), long.Parse(m.Group(11)));
				}
				else
				{
					Log.Warn("Unexpected: procfs stat file is not in the expected format" + " for process with pid "
						 + pinfo.GetPid());
					ret = null;
				}
			}
			catch (IOException io)
			{
				Log.Warn("Error reading the stream " + io);
				ret = null;
			}
			finally
			{
				// Close the streams
				try
				{
					fReader.Close();
					try
					{
						@in.Close();
					}
					catch (IOException)
					{
						Log.Warn("Error closing the stream " + @in);
					}
				}
				catch (IOException)
				{
					Log.Warn("Error closing the stream " + fReader);
				}
			}
			return ret;
		}

		/// <summary>
		/// Returns a string printing PIDs of process present in the
		/// ProcfsBasedProcessTree.
		/// </summary>
		/// <remarks>
		/// Returns a string printing PIDs of process present in the
		/// ProcfsBasedProcessTree. Output format : [pid pid ..]
		/// </remarks>
		public override string ToString()
		{
			StringBuilder pTree = new StringBuilder("[ ");
			foreach (string p in processTree.Keys)
			{
				pTree.Append(p);
				pTree.Append(" ");
			}
			return pTree.Substring(0, pTree.Length) + "]";
		}

		/// <summary>Class containing information of a process.</summary>
		private class ProcessInfo
		{
			private string pid;

			private string name;

			private int pgrpId;

			private string ppid;

			private int sessionId;

			private long vmem;

			private long rssmemPage;

			private long utime = 0L;

			private readonly BigInteger MaxLong = BigInteger.ValueOf(long.MaxValue);

			private BigInteger stime = new BigInteger("0");

			private int age;

			private long dtime = 0L;

			private IList<ProcfsBasedProcessTree.ProcessInfo> children = new AList<ProcfsBasedProcessTree.ProcessInfo
				>();

			public ProcessInfo(string pid)
			{
				// process-id
				// command name
				// process group-id
				// parent process-id
				// session-id
				// virtual memory usage
				// rss memory usage in # of pages
				// # of jiffies in user mode
				// # of jiffies in kernel mode
				// how many times has this process been seen alive
				// # of jiffies used since last update:
				// dtime = (utime + stime) - (utimeOld + stimeOld)
				// We need this to compute the cumulative CPU time
				// because the subprocess may finish earlier than root process
				// list of children
				this.pid = pid;
				// seeing this the first time.
				this.age = 1;
			}

			public virtual string GetPid()
			{
				return pid;
			}

			public virtual string GetName()
			{
				return name;
			}

			public virtual int GetPgrpId()
			{
				return pgrpId;
			}

			public virtual string GetPpid()
			{
				return ppid;
			}

			public virtual int GetSessionId()
			{
				return sessionId;
			}

			public virtual long GetVmem()
			{
				return vmem;
			}

			public virtual long GetUtime()
			{
				return utime;
			}

			public virtual BigInteger GetStime()
			{
				return stime;
			}

			public virtual long GetDtime()
			{
				return dtime;
			}

			public virtual long GetRssmemPage()
			{
				// get rss # of pages
				return rssmemPage;
			}

			public virtual int GetAge()
			{
				return age;
			}

			public virtual void UpdateProcessInfo(string name, string ppid, int pgrpId, int sessionId
				, long utime, BigInteger stime, long vmem, long rssmem)
			{
				this.name = name;
				this.ppid = ppid;
				this.pgrpId = pgrpId;
				this.sessionId = sessionId;
				this.utime = utime;
				this.stime = stime;
				this.vmem = vmem;
				this.rssmemPage = rssmem;
			}

			public virtual void UpdateJiffy(ProcfsBasedProcessTree.ProcessInfo oldInfo)
			{
				if (oldInfo == null)
				{
					BigInteger sum = this.stime.Add(BigInteger.ValueOf(this.utime));
					if (sum.CompareTo(MaxLong) > 0)
					{
						this.dtime = 0L;
						Log.Warn("Sum of stime (" + this.stime + ") and utime (" + this.utime + ") is greater than "
							 + long.MaxValue);
					}
					else
					{
						this.dtime = sum;
					}
					return;
				}
				this.dtime = (this.utime - oldInfo.utime + this.stime.Subtract(oldInfo.stime));
			}

			public virtual void UpdateAge(ProcfsBasedProcessTree.ProcessInfo oldInfo)
			{
				this.age = oldInfo.age + 1;
			}

			public virtual bool AddChild(ProcfsBasedProcessTree.ProcessInfo p)
			{
				return children.AddItem(p);
			}

			public virtual IList<ProcfsBasedProcessTree.ProcessInfo> GetChildren()
			{
				return children;
			}

			public virtual string GetCmdLine(string procfsDir)
			{
				string ret = "N/A";
				if (pid == null)
				{
					return ret;
				}
				BufferedReader @in = null;
				InputStreamReader fReader = null;
				try
				{
					fReader = new InputStreamReader(new FileInputStream(new FilePath(new FilePath(procfsDir
						, pid.ToString()), ProcfsCmdlineFile)), Sharpen.Extensions.GetEncoding("UTF-8"));
				}
				catch (FileNotFoundException)
				{
					// The process vanished in the interim!
					return ret;
				}
				@in = new BufferedReader(fReader);
				try
				{
					ret = @in.ReadLine();
					// only one line
					if (ret == null)
					{
						ret = "N/A";
					}
					else
					{
						ret = ret.Replace('\0', ' ');
						// Replace each null char with a space
						if (ret.Equals(string.Empty))
						{
							// The cmdline might be empty because the process is swapped out or
							// is a zombie.
							ret = "N/A";
						}
					}
				}
				catch (IOException io)
				{
					Log.Warn("Error reading the stream " + io);
					ret = "N/A";
				}
				finally
				{
					// Close the streams
					try
					{
						fReader.Close();
						try
						{
							@in.Close();
						}
						catch (IOException)
						{
							Log.Warn("Error closing the stream " + @in);
						}
					}
					catch (IOException)
					{
						Log.Warn("Error closing the stream " + fReader);
					}
				}
				return ret;
			}
		}

		/// <summary>Update memory related information</summary>
		/// <param name="pInfo"/>
		/// <param name="procfsDir"/>
		private static void ConstructProcessSMAPInfo(ProcfsBasedProcessTree.ProcessTreeSmapMemInfo
			 pInfo, string procfsDir)
		{
			BufferedReader @in = null;
			InputStreamReader fReader = null;
			try
			{
				FilePath pidDir = new FilePath(procfsDir, pInfo.GetPid());
				FilePath file = new FilePath(pidDir, Smaps);
				if (!file.Exists())
				{
					return;
				}
				fReader = new InputStreamReader(new FileInputStream(file), Sharpen.Extensions.GetEncoding
					("UTF-8"));
				@in = new BufferedReader(fReader);
				ProcfsBasedProcessTree.ProcessSmapMemoryInfo memoryMappingInfo = null;
				IList<string> lines = IOUtils.ReadLines(@in);
				foreach (string line in lines)
				{
					line = line.Trim();
					try
					{
						Matcher address = AddressPattern.Matcher(line);
						if (address.Find())
						{
							memoryMappingInfo = new ProcfsBasedProcessTree.ProcessSmapMemoryInfo(line);
							memoryMappingInfo.SetPermission(address.Group(4));
							pInfo.GetMemoryInfoList().AddItem(memoryMappingInfo);
							continue;
						}
						Matcher memInfo = MemInfoPattern.Matcher(line);
						if (memInfo.Find())
						{
							string key = memInfo.Group(1).Trim();
							string value = memInfo.Group(2).Replace(Kb, string.Empty).Trim();
							if (Log.IsDebugEnabled())
							{
								Log.Debug("MemInfo : " + key + " : Value  : " + value);
							}
							memoryMappingInfo.SetMemInfo(key, value);
						}
					}
					catch (Exception t)
					{
						Log.Warn("Error parsing smaps line : " + line + "; " + t.Message);
					}
				}
			}
			catch (FileNotFoundException f)
			{
				Log.Error(f.Message);
			}
			catch (IOException e)
			{
				Log.Error(e.Message);
			}
			catch (Exception t)
			{
				Log.Error(t.Message);
			}
			finally
			{
				IOUtils.CloseQuietly(@in);
			}
		}

		/// <summary>Placeholder for process's SMAPS information</summary>
		internal class ProcessTreeSmapMemInfo
		{
			private string pid;

			private IList<ProcfsBasedProcessTree.ProcessSmapMemoryInfo> memoryInfoList;

			public ProcessTreeSmapMemInfo(string pid)
			{
				this.pid = pid;
				this.memoryInfoList = new List<ProcfsBasedProcessTree.ProcessSmapMemoryInfo>();
			}

			public virtual IList<ProcfsBasedProcessTree.ProcessSmapMemoryInfo> GetMemoryInfoList
				()
			{
				return memoryInfoList;
			}

			public virtual string GetPid()
			{
				return pid;
			}

			public override string ToString()
			{
				StringBuilder sb = new StringBuilder();
				foreach (ProcfsBasedProcessTree.ProcessSmapMemoryInfo info in memoryInfoList)
				{
					sb.Append("\n");
					sb.Append(info.ToString());
				}
				return sb.ToString();
			}
		}

		/// <summary>
		/// <pre>
		/// Private Pages : Pages that were mapped only by the process
		/// Shared Pages : Pages that were shared with other processes
		/// Clean Pages : Pages that have not been modified since they were mapped
		/// Dirty Pages : Pages that have been modified since they were mapped
		/// Private RSS = Private Clean Pages + Private Dirty Pages
		/// Shared RSS = Shared Clean Pages + Shared Dirty Pages
		/// RSS = Private RSS + Shared RSS
		/// PSS = The count of all pages mapped uniquely by the process,
		/// plus a fraction of each shared page, said fraction to be
		/// proportional to the number of processes which have mapped the page.
		/// </summary>
		/// <remarks>
		/// <pre>
		/// Private Pages : Pages that were mapped only by the process
		/// Shared Pages : Pages that were shared with other processes
		/// Clean Pages : Pages that have not been modified since they were mapped
		/// Dirty Pages : Pages that have been modified since they were mapped
		/// Private RSS = Private Clean Pages + Private Dirty Pages
		/// Shared RSS = Shared Clean Pages + Shared Dirty Pages
		/// RSS = Private RSS + Shared RSS
		/// PSS = The count of all pages mapped uniquely by the process,
		/// plus a fraction of each shared page, said fraction to be
		/// proportional to the number of processes which have mapped the page.
		/// </pre>
		/// </remarks>
		internal class ProcessSmapMemoryInfo
		{
			private int size;

			private int rss;

			private int pss;

			private int sharedClean;

			private int sharedDirty;

			private int privateClean;

			private int privateDirty;

			private int referenced;

			private string regionName;

			private string permission;

			public ProcessSmapMemoryInfo(string name)
			{
				this.regionName = name;
			}

			public virtual string GetName()
			{
				return regionName;
			}

			public virtual void SetPermission(string permission)
			{
				this.permission = permission;
			}

			public virtual string GetPermission()
			{
				return permission;
			}

			public virtual int GetSize()
			{
				return size;
			}

			public virtual int GetRss()
			{
				return rss;
			}

			public virtual int GetPss()
			{
				return pss;
			}

			public virtual int GetSharedClean()
			{
				return sharedClean;
			}

			public virtual int GetSharedDirty()
			{
				return sharedDirty;
			}

			public virtual int GetPrivateClean()
			{
				return privateClean;
			}

			public virtual int GetPrivateDirty()
			{
				return privateDirty;
			}

			public virtual int GetReferenced()
			{
				return referenced;
			}

			public virtual void SetMemInfo(string key, string value)
			{
				ProcfsBasedProcessTree.MemInfo info = ProcfsBasedProcessTree.MemInfo.GetMemInfoByName
					(key);
				int val = 0;
				try
				{
					val = System.Convert.ToInt32(value.Trim());
				}
				catch (FormatException)
				{
					Log.Error("Error in parsing : " + info + " : value" + value.Trim());
					return;
				}
				if (info == null)
				{
					return;
				}
				if (Log.IsDebugEnabled())
				{
					Log.Debug("setMemInfo : memInfo : " + info);
				}
				switch (info)
				{
					case ProcfsBasedProcessTree.MemInfo.Size:
					{
						size = val;
						break;
					}

					case ProcfsBasedProcessTree.MemInfo.Rss:
					{
						rss = val;
						break;
					}

					case ProcfsBasedProcessTree.MemInfo.Pss:
					{
						pss = val;
						break;
					}

					case ProcfsBasedProcessTree.MemInfo.SharedClean:
					{
						sharedClean = val;
						break;
					}

					case ProcfsBasedProcessTree.MemInfo.SharedDirty:
					{
						sharedDirty = val;
						break;
					}

					case ProcfsBasedProcessTree.MemInfo.PrivateClean:
					{
						privateClean = val;
						break;
					}

					case ProcfsBasedProcessTree.MemInfo.PrivateDirty:
					{
						privateDirty = val;
						break;
					}

					case ProcfsBasedProcessTree.MemInfo.Referenced:
					{
						referenced = val;
						break;
					}

					default:
					{
						break;
					}
				}
			}

			public override string ToString()
			{
				StringBuilder sb = new StringBuilder();
				sb.Append("\t").Append(this.GetName()).Append("\n");
				sb.Append("\t").Append(ProcfsBasedProcessTree.MemInfo.Size.name + ":" + this.GetSize
					()).Append(" kB\n");
				sb.Append("\t").Append(ProcfsBasedProcessTree.MemInfo.Pss.name + ":" + this.GetPss
					()).Append(" kB\n");
				sb.Append("\t").Append(ProcfsBasedProcessTree.MemInfo.Rss.name + ":" + this.GetRss
					()).Append(" kB\n");
				sb.Append("\t").Append(ProcfsBasedProcessTree.MemInfo.SharedClean.name + ":" + this
					.GetSharedClean()).Append(" kB\n");
				sb.Append("\t").Append(ProcfsBasedProcessTree.MemInfo.SharedDirty.name + ":" + this
					.GetSharedDirty()).Append(" kB\n");
				sb.Append("\t").Append(ProcfsBasedProcessTree.MemInfo.PrivateClean.name + ":" + this
					.GetPrivateClean()).Append(" kB\n");
				sb.Append("\t").Append(ProcfsBasedProcessTree.MemInfo.PrivateDirty.name + ":" + this
					.GetPrivateDirty()).Append(" kB\n");
				sb.Append("\t").Append(ProcfsBasedProcessTree.MemInfo.Referenced.name + ":" + this
					.GetReferenced()).Append(" kB\n");
				sb.Append("\t").Append(ProcfsBasedProcessTree.MemInfo.PrivateDirty.name + ":" + this
					.GetPrivateDirty()).Append(" kB\n");
				sb.Append("\t").Append(ProcfsBasedProcessTree.MemInfo.PrivateDirty.name + ":" + this
					.GetPrivateDirty()).Append(" kB\n");
				return sb.ToString();
			}
		}

		/// <summary>
		/// Test the
		/// <see cref="ProcfsBasedProcessTree"/>
		/// </summary>
		/// <param name="args"/>
		public static void Main(string[] args)
		{
			if (args.Length != 1)
			{
				System.Console.Out.WriteLine("Provide <pid of process to monitor>");
				return;
			}
			int numprocessors = ResourceCalculatorPlugin.GetResourceCalculatorPlugin(null, null
				).GetNumProcessors();
			System.Console.Out.WriteLine("Number of processors " + numprocessors);
			System.Console.Out.WriteLine("Creating ProcfsBasedProcessTree for process " + args
				[0]);
			ProcfsBasedProcessTree procfsBasedProcessTree = new ProcfsBasedProcessTree(args[0
				]);
			procfsBasedProcessTree.UpdateProcessTree();
			System.Console.Out.WriteLine(procfsBasedProcessTree.GetProcessTreeDump());
			System.Console.Out.WriteLine("Get cpu usage " + procfsBasedProcessTree.GetCpuUsagePercent
				());
			try
			{
				// Sleep so we can compute the CPU usage
				Sharpen.Thread.Sleep(500L);
			}
			catch (Exception)
			{
			}
			// do nothing
			procfsBasedProcessTree.UpdateProcessTree();
			System.Console.Out.WriteLine(procfsBasedProcessTree.GetProcessTreeDump());
			System.Console.Out.WriteLine("Cpu usage  " + procfsBasedProcessTree.GetCpuUsagePercent
				());
			System.Console.Out.WriteLine("Vmem usage in bytes " + procfsBasedProcessTree.GetVirtualMemorySize
				());
			System.Console.Out.WriteLine("Rss mem usage in bytes " + procfsBasedProcessTree.GetRssMemorySize
				());
		}
	}
}
