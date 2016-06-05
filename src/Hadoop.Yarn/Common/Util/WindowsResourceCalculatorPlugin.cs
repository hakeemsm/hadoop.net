using System;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util
{
	public class WindowsResourceCalculatorPlugin : ResourceCalculatorPlugin
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Util.WindowsResourceCalculatorPlugin
			));

		internal long vmemSize;

		internal long memSize;

		internal long vmemAvailable;

		internal long memAvailable;

		internal int numProcessors;

		internal long cpuFrequencyKhz;

		internal long cumulativeCpuTimeMs;

		internal float cpuUsage;

		internal long lastRefreshTime;

		private readonly int refreshIntervalMs = 1000;

		internal WindowsBasedProcessTree pTree = null;

		public WindowsResourceCalculatorPlugin()
		{
			lastRefreshTime = 0;
			Reset();
		}

		internal virtual void Reset()
		{
			vmemSize = -1;
			memSize = -1;
			vmemAvailable = -1;
			memAvailable = -1;
			numProcessors = -1;
			cpuFrequencyKhz = -1;
			cumulativeCpuTimeMs = -1;
			cpuUsage = -1;
		}

		internal virtual string GetSystemInfoInfoFromShell()
		{
			Shell.ShellCommandExecutor shellExecutor = new Shell.ShellCommandExecutor(new string
				[] { Shell.Winutils, "systeminfo" });
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

		internal virtual void RefreshIfNeeded()
		{
			long now = Runtime.CurrentTimeMillis();
			if (now - lastRefreshTime > refreshIntervalMs)
			{
				long refreshInterval = now - lastRefreshTime;
				lastRefreshTime = now;
				long lastCumCpuTimeMs = cumulativeCpuTimeMs;
				Reset();
				string sysInfoStr = GetSystemInfoInfoFromShell();
				if (sysInfoStr != null)
				{
					int sysInfoSplitCount = 7;
					string[] sysInfo = Sharpen.Runtime.Substring(sysInfoStr, 0, sysInfoStr.IndexOf("\r\n"
						)).Split(",");
					if (sysInfo.Length == sysInfoSplitCount)
					{
						try
						{
							vmemSize = long.Parse(sysInfo[0]);
							memSize = long.Parse(sysInfo[1]);
							vmemAvailable = long.Parse(sysInfo[2]);
							memAvailable = long.Parse(sysInfo[3]);
							numProcessors = System.Convert.ToInt32(sysInfo[4]);
							cpuFrequencyKhz = long.Parse(sysInfo[5]);
							cumulativeCpuTimeMs = long.Parse(sysInfo[6]);
							if (lastCumCpuTimeMs != -1)
							{
								cpuUsage = (cumulativeCpuTimeMs - lastCumCpuTimeMs) / (refreshInterval * 1.0f);
							}
						}
						catch (FormatException nfe)
						{
							Log.Warn("Error parsing sysInfo." + nfe);
						}
					}
					else
					{
						Log.Warn("Expected split length of sysInfo to be " + sysInfoSplitCount + ". Got "
							 + sysInfo.Length);
					}
				}
			}
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public override long GetVirtualMemorySize()
		{
			RefreshIfNeeded();
			return vmemSize;
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public override long GetPhysicalMemorySize()
		{
			RefreshIfNeeded();
			return memSize;
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public override long GetAvailableVirtualMemorySize()
		{
			RefreshIfNeeded();
			return vmemAvailable;
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public override long GetAvailablePhysicalMemorySize()
		{
			RefreshIfNeeded();
			return memAvailable;
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public override int GetNumProcessors()
		{
			RefreshIfNeeded();
			return numProcessors;
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public override long GetCpuFrequency()
		{
			RefreshIfNeeded();
			return -1;
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public override long GetCumulativeCpuTime()
		{
			RefreshIfNeeded();
			return cumulativeCpuTimeMs;
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public override float GetCpuUsage()
		{
			RefreshIfNeeded();
			return cpuUsage;
		}
	}
}
