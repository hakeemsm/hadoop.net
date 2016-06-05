using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util
{
	public class TestWindowsResourceCalculatorPlugin
	{
		internal class WindowsResourceCalculatorPluginTester : WindowsResourceCalculatorPlugin
		{
			private string infoStr = null;

			internal override string GetSystemInfoInfoFromShell()
			{
				return this.infoStr;
			}

			internal WindowsResourceCalculatorPluginTester(TestWindowsResourceCalculatorPlugin
				 _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestWindowsResourceCalculatorPlugin _enclosing;
		}

		public virtual void ParseSystemInfoString()
		{
			TestWindowsResourceCalculatorPlugin.WindowsResourceCalculatorPluginTester tester = 
				new TestWindowsResourceCalculatorPlugin.WindowsResourceCalculatorPluginTester(this
				);
			// info str derived from windows shell command has \r\n termination
			tester.infoStr = "17177038848,8589467648,15232745472,6400417792,1,2805000,6261812\r\n";
			// call a method to refresh values
			tester.GetAvailablePhysicalMemorySize();
			// verify information has been refreshed
			NUnit.Framework.Assert.IsTrue(tester.vmemSize == 17177038848L);
			NUnit.Framework.Assert.IsTrue(tester.memSize == 8589467648L);
			NUnit.Framework.Assert.IsTrue(tester.vmemAvailable == 15232745472L);
			NUnit.Framework.Assert.IsTrue(tester.memAvailable == 6400417792L);
			NUnit.Framework.Assert.IsTrue(tester.numProcessors == 1);
			NUnit.Framework.Assert.IsTrue(tester.cpuFrequencyKhz == 2805000L);
			NUnit.Framework.Assert.IsTrue(tester.cumulativeCpuTimeMs == 6261812L);
			NUnit.Framework.Assert.IsTrue(tester.cpuUsage == -1);
		}

		/// <exception cref="System.Exception"/>
		public virtual void RefreshAndCpuUsage()
		{
			TestWindowsResourceCalculatorPlugin.WindowsResourceCalculatorPluginTester tester = 
				new TestWindowsResourceCalculatorPlugin.WindowsResourceCalculatorPluginTester(this
				);
			// info str derived from windows shell command has \r\n termination
			tester.infoStr = "17177038848,8589467648,15232745472,6400417792,1,2805000,6261812\r\n";
			tester.GetAvailablePhysicalMemorySize();
			// verify information has been refreshed
			NUnit.Framework.Assert.IsTrue(tester.memAvailable == 6400417792L);
			NUnit.Framework.Assert.IsTrue(tester.cpuUsage == -1);
			tester.infoStr = "17177038848,8589467648,15232745472,5400417792,1,2805000,6261812\r\n";
			tester.GetAvailablePhysicalMemorySize();
			// verify information has not been refreshed
			NUnit.Framework.Assert.IsTrue(tester.memAvailable == 6400417792L);
			NUnit.Framework.Assert.IsTrue(tester.cpuUsage == -1);
			Sharpen.Thread.Sleep(1500);
			tester.infoStr = "17177038848,8589467648,15232745472,5400417792,1,2805000,6286812\r\n";
			tester.GetAvailablePhysicalMemorySize();
			// verify information has been refreshed
			NUnit.Framework.Assert.IsTrue(tester.memAvailable == 5400417792L);
			NUnit.Framework.Assert.IsTrue(tester.cpuUsage >= 0.1);
		}

		public virtual void ErrorInGetSystemInfo()
		{
			TestWindowsResourceCalculatorPlugin.WindowsResourceCalculatorPluginTester tester = 
				new TestWindowsResourceCalculatorPlugin.WindowsResourceCalculatorPluginTester(this
				);
			// info str derived from windows shell command has \r\n termination
			tester.infoStr = null;
			// call a method to refresh values
			tester.GetAvailablePhysicalMemorySize();
		}
	}
}
