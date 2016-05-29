using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util
{
	public class WindowsBasedProcessTree : ResourceCalculatorProcessTree
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Util.WindowsBasedProcessTree
			));

		internal class ProcessInfo
		{
			internal string pid;

			internal long vmem;

			internal long workingSet;

			internal long cpuTimeMs;

			internal long cpuTimeMsDelta;

			internal int age = 1;
			// process pid
			// virtual memory
			// working set, RAM used
			// total cpuTime in millisec
			// delta of cpuTime since last update
		}

		private string taskProcessId = null;

		private long cpuTimeMs = Unavailable;

		private IDictionary<string, WindowsBasedProcessTree.ProcessInfo> processTree = new 
			Dictionary<string, WindowsBasedProcessTree.ProcessInfo>();

		public static bool IsAvailable()
		{
			if (Shell.Windows)
			{
				Shell.ShellCommandExecutor shellExecutor = new Shell.ShellCommandExecutor(new string
					[] { Shell.Winutils, "help" });
				try
				{
					shellExecutor.Execute();
				}
				catch (IOException e)
				{
					Log.Error(StringUtils.StringifyException(e));
				}
				finally
				{
					string output = shellExecutor.GetOutput();
					if (output != null && output.Contains("Prints to stdout a list of processes in the task"
						))
					{
						return true;
					}
				}
			}
			return false;
		}

		public WindowsBasedProcessTree(string pid)
			: base(pid)
		{
			taskProcessId = pid;
		}

		// helper method to override while testing
		internal virtual string GetAllProcessInfoFromShell()
		{
			Shell.ShellCommandExecutor shellExecutor = new Shell.ShellCommandExecutor(new string
				[] { Shell.Winutils, "task", "processList", taskProcessId });
			try
			{
				shellExecutor.Execute();
				return shellExecutor.GetOutput();
			}
			catch (IOException e)
			{
				Log.Error(StringUtils.StringifyException(e));
			}
			return null;
		}

		/// <summary>Parses string of process info lines into ProcessInfo objects</summary>
		/// <param name="processesInfoStr"/>
		/// <returns>Map of pid string to ProcessInfo objects</returns>
		internal virtual IDictionary<string, WindowsBasedProcessTree.ProcessInfo> CreateProcessInfo
			(string processesInfoStr)
		{
			string[] processesStr = processesInfoStr.Split("\r\n");
			IDictionary<string, WindowsBasedProcessTree.ProcessInfo> allProcs = new Dictionary
				<string, WindowsBasedProcessTree.ProcessInfo>();
			int procInfoSplitCount = 4;
			foreach (string processStr in processesStr)
			{
				if (processStr != null)
				{
					string[] procInfo = processStr.Split(",");
					if (procInfo.Length == procInfoSplitCount)
					{
						try
						{
							WindowsBasedProcessTree.ProcessInfo pInfo = new WindowsBasedProcessTree.ProcessInfo
								();
							pInfo.pid = procInfo[0];
							pInfo.vmem = long.Parse(procInfo[1]);
							pInfo.workingSet = long.Parse(procInfo[2]);
							pInfo.cpuTimeMs = long.Parse(procInfo[3]);
							allProcs[pInfo.pid] = pInfo;
						}
						catch (FormatException nfe)
						{
							Log.Debug("Error parsing procInfo." + nfe);
						}
					}
					else
					{
						Log.Debug("Expected split length of proc info to be " + procInfoSplitCount + ". Got "
							 + procInfo.Length);
					}
				}
			}
			return allProcs;
		}

		public override void UpdateProcessTree()
		{
			if (taskProcessId != null)
			{
				// taskProcessId can be null in some tests
				string processesInfoStr = GetAllProcessInfoFromShell();
				if (processesInfoStr != null && processesInfoStr.Length > 0)
				{
					IDictionary<string, WindowsBasedProcessTree.ProcessInfo> allProcessInfo = CreateProcessInfo
						(processesInfoStr);
					foreach (KeyValuePair<string, WindowsBasedProcessTree.ProcessInfo> entry in allProcessInfo)
					{
						string pid = entry.Key;
						WindowsBasedProcessTree.ProcessInfo pInfo = entry.Value;
						WindowsBasedProcessTree.ProcessInfo oldInfo = processTree[pid];
						if (oldInfo != null)
						{
							// existing process, update age and replace value
							pInfo.age += oldInfo.age;
							// calculate the delta since the last refresh. totals are being kept
							// in the WindowsBasedProcessTree object
							pInfo.cpuTimeMsDelta = pInfo.cpuTimeMs - oldInfo.cpuTimeMs;
						}
						else
						{
							// new process. delta cpu == total cpu
							pInfo.cpuTimeMsDelta = pInfo.cpuTimeMs;
						}
					}
					processTree.Clear();
					processTree = allProcessInfo;
				}
				else
				{
					// clearing process tree to mimic semantics of existing Procfs impl
					processTree.Clear();
				}
			}
		}

		public override bool CheckPidPgrpidForMatch()
		{
			// This is always true on Windows, because the pid doubles as a job object
			// name for task management.
			return true;
		}

		public override string GetProcessTreeDump()
		{
			StringBuilder ret = new StringBuilder();
			// The header.
			ret.Append(string.Format("\t|- PID " + "CPU_TIME(MILLIS) " + "VMEM(BYTES) WORKING_SET(BYTES)%n"
				));
			foreach (WindowsBasedProcessTree.ProcessInfo p in processTree.Values)
			{
				if (p != null)
				{
					ret.Append(string.Format("\t|- %s %d %d %d%n", p.pid, p.cpuTimeMs, p.vmem, p.workingSet
						));
				}
			}
			return ret.ToString();
		}

		public override long GetVirtualMemorySize(int olderThanAge)
		{
			long total = Unavailable;
			foreach (WindowsBasedProcessTree.ProcessInfo p in processTree.Values)
			{
				if (p != null)
				{
					if (total == Unavailable)
					{
						total = 0;
					}
					if (p.age > olderThanAge)
					{
						total += p.vmem;
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
			long total = Unavailable;
			foreach (WindowsBasedProcessTree.ProcessInfo p in processTree.Values)
			{
				if (p != null)
				{
					if (total == Unavailable)
					{
						total = 0;
					}
					if (p.age > olderThanAge)
					{
						total += p.workingSet;
					}
				}
			}
			return total;
		}

		public override long GetCumulativeRssmem(int olderThanAge)
		{
			return GetRssMemorySize(olderThanAge);
		}

		public override long GetCumulativeCpuTime()
		{
			foreach (WindowsBasedProcessTree.ProcessInfo p in processTree.Values)
			{
				if (cpuTimeMs == Unavailable)
				{
					cpuTimeMs = 0;
				}
				cpuTimeMs += p.cpuTimeMsDelta;
			}
			return cpuTimeMs;
		}

		public override float GetCpuUsagePercent()
		{
			return CpuTimeTracker.Unavailable;
		}
	}
}
