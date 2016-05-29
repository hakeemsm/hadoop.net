using System.IO;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util
{
	/// <summary>
	/// A JUnit test to test
	/// <see cref="LinuxResourceCalculatorPlugin"/>
	/// Create the fake /proc/ information and verify the parsing and calculation
	/// </summary>
	public class TestLinuxResourceCalculatorPlugin
	{
		/// <summary>LinuxResourceCalculatorPlugin with a fake timer</summary>
		internal class FakeLinuxResourceCalculatorPlugin : LinuxResourceCalculatorPlugin
		{
			internal long currentTime = 0;

			public FakeLinuxResourceCalculatorPlugin(string procfsMemFile, string procfsCpuFile
				, string procfsStatFile, long jiffyLengthInMillis)
				: base(procfsMemFile, procfsCpuFile, procfsStatFile, jiffyLengthInMillis)
			{
			}

			internal override long GetCurrentTime()
			{
				return currentTime;
			}

			public virtual void AdvanceTime(long adv)
			{
				currentTime += adv * jiffyLengthInMillis;
			}
		}

		private static readonly TestLinuxResourceCalculatorPlugin.FakeLinuxResourceCalculatorPlugin
			 plugin;

		private static string TestRootDir = new Path(Runtime.GetProperty("test.build.data"
			, "/tmp")).ToString().Replace(' ', '+');

		private static readonly string FakeMemfile;

		private static readonly string FakeCpufile;

		private static readonly string FakeStatfile;

		private const long FakeJiffyLength = 10L;

		static TestLinuxResourceCalculatorPlugin()
		{
			int randomNum = (new Random()).Next(1000000000);
			FakeMemfile = TestRootDir + FilePath.separator + "MEMINFO_" + randomNum;
			FakeCpufile = TestRootDir + FilePath.separator + "CPUINFO_" + randomNum;
			FakeStatfile = TestRootDir + FilePath.separator + "STATINFO_" + randomNum;
			plugin = new TestLinuxResourceCalculatorPlugin.FakeLinuxResourceCalculatorPlugin(
				FakeMemfile, FakeCpufile, FakeStatfile, FakeJiffyLength);
		}

		internal const string MeminfoFormat = "MemTotal:      %d kB\n" + "MemFree:         %d kB\n"
			 + "Buffers:        138244 kB\n" + "Cached:         947780 kB\n" + "SwapCached:     142880 kB\n"
			 + "Active:        3229888 kB\n" + "Inactive:       %d kB\n" + "SwapTotal:     %d kB\n"
			 + "SwapFree:      %d kB\n" + "Dirty:          122012 kB\n" + "Writeback:           0 kB\n"
			 + "AnonPages:     2710792 kB\n" + "Mapped:          24740 kB\n" + "Slab:           132528 kB\n"
			 + "SReclaimable:   105096 kB\n" + "SUnreclaim:      27432 kB\n" + "PageTables:      11448 kB\n"
			 + "NFS_Unstable:        0 kB\n" + "Bounce:              0 kB\n" + "CommitLimit:   4125904 kB\n"
			 + "Committed_AS:  4143556 kB\n" + "VmallocTotal: 34359738367 kB\n" + "VmallocUsed:      1632 kB\n"
			 + "VmallocChunk: 34359736375 kB\n" + "HugePages_Total:     0\n" + "HugePages_Free:      0\n"
			 + "HugePages_Rsvd:      0\n" + "Hugepagesize:     2048 kB";

		internal const string CpuinfoFormat = "processor : %s\n" + "vendor_id : AuthenticAMD\n"
			 + "cpu family  : 15\n" + "model   : 33\n" + "model name  : Dual Core AMD Opteron(tm) Processor 280\n"
			 + "stepping  : 2\n" + "cpu MHz   : %f\n" + "cache size  : 1024 KB\n" + "physical id : 0\n"
			 + "siblings  : 2\n" + "core id   : 0\n" + "cpu cores : 2\n" + "fpu   : yes\n" +
			 "fpu_exception : yes\n" + "cpuid level : 1\n" + "wp    : yes\n" + "flags   : fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov "
			 + "pat pse36 clflush mmx fxsr sse sse2 ht syscall nx mmxext fxsr_opt lm " + "3dnowext 3dnow pni lahf_lm cmp_legacy\n"
			 + "bogomips  : 4792.41\n" + "TLB size  : 1024 4K pages\n" + "clflush size  : 64\n"
			 + "cache_alignment : 64\n" + "address sizes : 40 bits physical, 48 bits virtual\n"
			 + "power management: ts fid vid ttp";

		internal const string StatFileFormat = "cpu  %d %d %d 1646495089 831319 48713 164346 0\n"
			 + "cpu0 15096055 30805 3823005 411456015 206027 13 14269 0\n" + "cpu1 14760561 89890 6432036 408707910 456857 48074 130857 0\n"
			 + "cpu2 12761169 20842 3758639 413976772 98028 411 10288 0\n" + "cpu3 12355207 47322 5789691 412354390 70406 213 8931 0\n"
			 + "intr 114648668 20010764 2 0 945665 2 0 0 0 0 0 0 0 4 0 0 0 0 0 0\n" + "ctxt 242017731764\n"
			 + "btime 1257808753\n" + "processes 26414943\n" + "procs_running 1\n" + "procs_blocked 0\n";

		/// <summary>Test parsing /proc/stat and /proc/cpuinfo</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void ParsingProcStatAndCpuFile()
		{
			// Write fake /proc/cpuinfo file.
			long numProcessors = 8;
			long cpuFrequencyKHz = 2392781;
			string fileContent = string.Empty;
			for (int i = 0; i < numProcessors; i++)
			{
				fileContent += string.Format(CpuinfoFormat, i, cpuFrequencyKHz / 1000D) + "\n";
			}
			FilePath tempFile = new FilePath(FakeCpufile);
			tempFile.DeleteOnExit();
			FileWriter fWriter = new FileWriter(FakeCpufile);
			fWriter.Write(fileContent);
			fWriter.Close();
			NUnit.Framework.Assert.AreEqual(plugin.GetNumProcessors(), numProcessors);
			NUnit.Framework.Assert.AreEqual(plugin.GetCpuFrequency(), cpuFrequencyKHz);
			// Write fake /proc/stat file.
			long uTime = 54972994;
			long nTime = 188860;
			long sTime = 19803373;
			tempFile = new FilePath(FakeStatfile);
			tempFile.DeleteOnExit();
			UpdateStatFile(uTime, nTime, sTime);
			NUnit.Framework.Assert.AreEqual(plugin.GetCumulativeCpuTime(), FakeJiffyLength * 
				(uTime + nTime + sTime));
			NUnit.Framework.Assert.AreEqual(plugin.GetCpuUsage(), (float)(CpuTimeTracker.Unavailable
				), 0.0);
			// Advance the time and sample again to test the CPU usage calculation
			uTime += 100L;
			plugin.AdvanceTime(200L);
			UpdateStatFile(uTime, nTime, sTime);
			NUnit.Framework.Assert.AreEqual(plugin.GetCumulativeCpuTime(), FakeJiffyLength * 
				(uTime + nTime + sTime));
			NUnit.Framework.Assert.AreEqual(plugin.GetCpuUsage(), 6.25F, 0.0);
			// Advance the time and sample again. This time, we call getCpuUsage() only.
			uTime += 600L;
			plugin.AdvanceTime(300L);
			UpdateStatFile(uTime, nTime, sTime);
			NUnit.Framework.Assert.AreEqual(plugin.GetCpuUsage(), 25F, 0.0);
			// Advance very short period of time (one jiffy length).
			// In this case, CPU usage should not be updated.
			uTime += 1L;
			plugin.AdvanceTime(1L);
			UpdateStatFile(uTime, nTime, sTime);
			NUnit.Framework.Assert.AreEqual(plugin.GetCumulativeCpuTime(), FakeJiffyLength * 
				(uTime + nTime + sTime));
			NUnit.Framework.Assert.AreEqual(plugin.GetCpuUsage(), 25F, 0.0);
		}

		// CPU usage is not updated.
		/// <summary>Write information to fake /proc/stat file</summary>
		/// <exception cref="System.IO.IOException"/>
		private void UpdateStatFile(long uTime, long nTime, long sTime)
		{
			FileWriter fWriter = new FileWriter(FakeStatfile);
			fWriter.Write(string.Format(StatFileFormat, uTime, nTime, sTime));
			fWriter.Close();
		}

		/// <summary>Test parsing /proc/meminfo</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void ParsingProcMemFile()
		{
			long memTotal = 4058864L;
			long memFree = 99632L;
			long inactive = 567732L;
			long swapTotal = 2096472L;
			long swapFree = 1818480L;
			FilePath tempFile = new FilePath(FakeMemfile);
			tempFile.DeleteOnExit();
			FileWriter fWriter = new FileWriter(FakeMemfile);
			fWriter.Write(string.Format(MeminfoFormat, memTotal, memFree, inactive, swapTotal
				, swapFree));
			fWriter.Close();
			NUnit.Framework.Assert.AreEqual(plugin.GetAvailablePhysicalMemorySize(), 1024L * 
				(memFree + inactive));
			NUnit.Framework.Assert.AreEqual(plugin.GetAvailableVirtualMemorySize(), 1024L * (
				memFree + inactive + swapFree));
			NUnit.Framework.Assert.AreEqual(plugin.GetPhysicalMemorySize(), 1024L * memTotal);
			NUnit.Framework.Assert.AreEqual(plugin.GetVirtualMemorySize(), 1024L * (memTotal 
				+ swapTotal));
		}
	}
}
