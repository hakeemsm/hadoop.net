using System;
using System.IO;
using System.Text;
using Mono.Math;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util
{
	/// <summary>Plugin to calculate resource information on Linux systems.</summary>
	public class LinuxResourceCalculatorPlugin : ResourceCalculatorPlugin
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Util.LinuxResourceCalculatorPlugin
			));

		/// <summary>
		/// proc's meminfo virtual file has keys-values in the format
		/// "key:[ \t]*value[ \t]kB".
		/// </summary>
		private const string ProcfsMemfile = "/proc/meminfo";

		private static readonly Sharpen.Pattern ProcfsMemfileFormat = Sharpen.Pattern.Compile
			("^([a-zA-Z]*):[ \t]*([0-9]*)[ \t]kB");

		private const string MemtotalString = "MemTotal";

		private const string SwaptotalString = "SwapTotal";

		private const string MemfreeString = "MemFree";

		private const string SwapfreeString = "SwapFree";

		private const string InactiveString = "Inactive";

		/// <summary>Patterns for parsing /proc/cpuinfo</summary>
		private const string ProcfsCpuinfo = "/proc/cpuinfo";

		private static readonly Sharpen.Pattern ProcessorFormat = Sharpen.Pattern.Compile
			("^processor[ \t]:[ \t]*([0-9]*)");

		private static readonly Sharpen.Pattern FrequencyFormat = Sharpen.Pattern.Compile
			("^cpu MHz[ \t]*:[ \t]*([0-9.]*)");

		/// <summary>Pattern for parsing /proc/stat</summary>
		private const string ProcfsStat = "/proc/stat";

		private static readonly Sharpen.Pattern CpuTimeFormat = Sharpen.Pattern.Compile("^cpu[ \t]*([0-9]*)"
			 + "[ \t]*([0-9]*)[ \t]*([0-9]*)[ \t].*");

		private CpuTimeTracker cpuTimeTracker;

		private string procfsMemFile;

		private string procfsCpuFile;

		private string procfsStatFile;

		internal long jiffyLengthInMillis;

		private long ramSize = 0;

		private long swapSize = 0;

		private long ramSizeFree = 0;

		private long swapSizeFree = 0;

		private long inactiveSize = 0;

		private int numProcessors = 0;

		private long cpuFrequency = 0L;

		internal bool readMemInfoFile = false;

		internal bool readCpuInfoFile = false;

		// We need the values for the following keys in meminfo
		// free ram space on the machine (kB)
		// free swap space on the machine (kB)
		// inactive cache memory (kB)
		// number of processors on the system
		// CPU frequency on the system (kHz)
		/// <summary>Get current time</summary>
		/// <returns>Unix time stamp in millisecond</returns>
		internal virtual long GetCurrentTime()
		{
			return Runtime.CurrentTimeMillis();
		}

		public LinuxResourceCalculatorPlugin()
			: this(ProcfsMemfile, ProcfsCpuinfo, ProcfsStat, ProcfsBasedProcessTree.JiffyLengthInMillis
				)
		{
		}

		/// <summary>Constructor which allows assigning the /proc/ directories.</summary>
		/// <remarks>
		/// Constructor which allows assigning the /proc/ directories. This will be
		/// used only in unit tests
		/// </remarks>
		/// <param name="procfsMemFile">fake file for /proc/meminfo</param>
		/// <param name="procfsCpuFile">fake file for /proc/cpuinfo</param>
		/// <param name="procfsStatFile">fake file for /proc/stat</param>
		/// <param name="jiffyLengthInMillis">fake jiffy length value</param>
		public LinuxResourceCalculatorPlugin(string procfsMemFile, string procfsCpuFile, 
			string procfsStatFile, long jiffyLengthInMillis)
		{
			this.procfsMemFile = procfsMemFile;
			this.procfsCpuFile = procfsCpuFile;
			this.procfsStatFile = procfsStatFile;
			this.jiffyLengthInMillis = jiffyLengthInMillis;
			this.cpuTimeTracker = new CpuTimeTracker(jiffyLengthInMillis);
		}

		/// <summary>Read /proc/meminfo, parse and compute memory information only once</summary>
		private void ReadProcMemInfoFile()
		{
			ReadProcMemInfoFile(false);
		}

		/// <summary>Read /proc/meminfo, parse and compute memory information</summary>
		/// <param name="readAgain">if false, read only on the first time</param>
		private void ReadProcMemInfoFile(bool readAgain)
		{
			if (readMemInfoFile && !readAgain)
			{
				return;
			}
			// Read "/proc/memInfo" file
			BufferedReader @in = null;
			InputStreamReader fReader = null;
			try
			{
				fReader = new InputStreamReader(new FileInputStream(procfsMemFile), Sharpen.Extensions.GetEncoding
					("UTF-8"));
				@in = new BufferedReader(fReader);
			}
			catch (FileNotFoundException)
			{
				// shouldn't happen....
				return;
			}
			Matcher mat = null;
			try
			{
				string str = @in.ReadLine();
				while (str != null)
				{
					mat = ProcfsMemfileFormat.Matcher(str);
					if (mat.Find())
					{
						if (mat.Group(1).Equals(MemtotalString))
						{
							ramSize = long.Parse(mat.Group(2));
						}
						else
						{
							if (mat.Group(1).Equals(SwaptotalString))
							{
								swapSize = long.Parse(mat.Group(2));
							}
							else
							{
								if (mat.Group(1).Equals(MemfreeString))
								{
									ramSizeFree = long.Parse(mat.Group(2));
								}
								else
								{
									if (mat.Group(1).Equals(SwapfreeString))
									{
										swapSizeFree = long.Parse(mat.Group(2));
									}
									else
									{
										if (mat.Group(1).Equals(InactiveString))
										{
											inactiveSize = long.Parse(mat.Group(2));
										}
									}
								}
							}
						}
					}
					str = @in.ReadLine();
				}
			}
			catch (IOException io)
			{
				Log.Warn("Error reading the stream " + io);
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
			readMemInfoFile = true;
		}

		/// <summary>Read /proc/cpuinfo, parse and calculate CPU information</summary>
		private void ReadProcCpuInfoFile()
		{
			// This directory needs to be read only once
			if (readCpuInfoFile)
			{
				return;
			}
			// Read "/proc/cpuinfo" file
			BufferedReader @in = null;
			InputStreamReader fReader = null;
			try
			{
				fReader = new InputStreamReader(new FileInputStream(procfsCpuFile), Sharpen.Extensions.GetEncoding
					("UTF-8"));
				@in = new BufferedReader(fReader);
			}
			catch (FileNotFoundException)
			{
				// shouldn't happen....
				return;
			}
			Matcher mat = null;
			try
			{
				numProcessors = 0;
				string str = @in.ReadLine();
				while (str != null)
				{
					mat = ProcessorFormat.Matcher(str);
					if (mat.Find())
					{
						numProcessors++;
					}
					mat = FrequencyFormat.Matcher(str);
					if (mat.Find())
					{
						cpuFrequency = (long)(double.ParseDouble(mat.Group(1)) * 1000);
					}
					// kHz
					str = @in.ReadLine();
				}
			}
			catch (IOException io)
			{
				Log.Warn("Error reading the stream " + io);
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
			readCpuInfoFile = true;
		}

		/// <summary>Read /proc/stat file, parse and calculate cumulative CPU</summary>
		private void ReadProcStatFile()
		{
			// Read "/proc/stat" file
			BufferedReader @in = null;
			InputStreamReader fReader = null;
			try
			{
				fReader = new InputStreamReader(new FileInputStream(procfsStatFile), Sharpen.Extensions.GetEncoding
					("UTF-8"));
				@in = new BufferedReader(fReader);
			}
			catch (FileNotFoundException)
			{
				// shouldn't happen....
				return;
			}
			Matcher mat = null;
			try
			{
				string str = @in.ReadLine();
				while (str != null)
				{
					mat = CpuTimeFormat.Matcher(str);
					if (mat.Find())
					{
						long uTime = long.Parse(mat.Group(1));
						long nTime = long.Parse(mat.Group(2));
						long sTime = long.Parse(mat.Group(3));
						cpuTimeTracker.UpdateElapsedJiffies(BigInteger.ValueOf(uTime + nTime + sTime), GetCurrentTime
							());
						break;
					}
					str = @in.ReadLine();
				}
			}
			catch (IOException io)
			{
				Log.Warn("Error reading the stream " + io);
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
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public override long GetPhysicalMemorySize()
		{
			ReadProcMemInfoFile();
			return ramSize * 1024;
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public override long GetVirtualMemorySize()
		{
			ReadProcMemInfoFile();
			return (ramSize + swapSize) * 1024;
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public override long GetAvailablePhysicalMemorySize()
		{
			ReadProcMemInfoFile(true);
			return (ramSizeFree + inactiveSize) * 1024;
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public override long GetAvailableVirtualMemorySize()
		{
			ReadProcMemInfoFile(true);
			return (ramSizeFree + swapSizeFree + inactiveSize) * 1024;
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public override int GetNumProcessors()
		{
			ReadProcCpuInfoFile();
			return numProcessors;
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public override long GetCpuFrequency()
		{
			ReadProcCpuInfoFile();
			return cpuFrequency;
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public override long GetCumulativeCpuTime()
		{
			ReadProcStatFile();
			return cpuTimeTracker.cumulativeCpuTime;
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public override float GetCpuUsage()
		{
			ReadProcStatFile();
			float overallCpuUsage = cpuTimeTracker.GetCpuTrackerUsagePercent();
			if (overallCpuUsage != CpuTimeTracker.Unavailable)
			{
				overallCpuUsage = overallCpuUsage / GetNumProcessors();
			}
			return overallCpuUsage;
		}

		/// <summary>
		/// Test the
		/// <see cref="LinuxResourceCalculatorPlugin"/>
		/// </summary>
		/// <param name="args"/>
		public static void Main(string[] args)
		{
			Org.Apache.Hadoop.Yarn.Util.LinuxResourceCalculatorPlugin plugin = new Org.Apache.Hadoop.Yarn.Util.LinuxResourceCalculatorPlugin
				();
			System.Console.Out.WriteLine("Physical memory Size (bytes) : " + plugin.GetPhysicalMemorySize
				());
			System.Console.Out.WriteLine("Total Virtual memory Size (bytes) : " + plugin.GetVirtualMemorySize
				());
			System.Console.Out.WriteLine("Available Physical memory Size (bytes) : " + plugin
				.GetAvailablePhysicalMemorySize());
			System.Console.Out.WriteLine("Total Available Virtual memory Size (bytes) : " + plugin
				.GetAvailableVirtualMemorySize());
			System.Console.Out.WriteLine("Number of Processors : " + plugin.GetNumProcessors(
				));
			System.Console.Out.WriteLine("CPU frequency (kHz) : " + plugin.GetCpuFrequency());
			System.Console.Out.WriteLine("Cumulative CPU time (ms) : " + plugin.GetCumulativeCpuTime
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
			System.Console.Out.WriteLine("CPU usage % : " + plugin.GetCpuUsage());
		}
	}
}
