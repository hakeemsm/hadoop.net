using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util
{
	public class TestWindowsBasedProcessTree
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestWindowsBasedProcessTree
			));

		internal class WindowsBasedProcessTreeTester : WindowsBasedProcessTree
		{
			internal string infoStr = null;

			public WindowsBasedProcessTreeTester(TestWindowsBasedProcessTree _enclosing, string
				 pid)
				: base(pid)
			{
				this._enclosing = _enclosing;
			}

			internal override string GetAllProcessInfoFromShell()
			{
				return this.infoStr;
			}

			private readonly TestWindowsBasedProcessTree _enclosing;
		}

		public virtual void Tree()
		{
			if (!Shell.Windows)
			{
				Log.Info("Platform not Windows. Not testing");
				return;
			}
			NUnit.Framework.Assert.IsTrue("WindowsBasedProcessTree should be available on Windows"
				, WindowsBasedProcessTree.IsAvailable());
			TestWindowsBasedProcessTree.WindowsBasedProcessTreeTester pTree = new TestWindowsBasedProcessTree.WindowsBasedProcessTreeTester
				(this, "-1");
			pTree.infoStr = "3524,1024,1024,500\r\n2844,1024,1024,500\r\n";
			pTree.UpdateProcessTree();
			NUnit.Framework.Assert.IsTrue(pTree.GetVirtualMemorySize() == 2048);
			NUnit.Framework.Assert.IsTrue(pTree.GetCumulativeVmem() == 2048);
			NUnit.Framework.Assert.IsTrue(pTree.GetVirtualMemorySize(0) == 2048);
			NUnit.Framework.Assert.IsTrue(pTree.GetCumulativeVmem(0) == 2048);
			NUnit.Framework.Assert.IsTrue(pTree.GetRssMemorySize() == 2048);
			NUnit.Framework.Assert.IsTrue(pTree.GetCumulativeRssmem() == 2048);
			NUnit.Framework.Assert.IsTrue(pTree.GetRssMemorySize(0) == 2048);
			NUnit.Framework.Assert.IsTrue(pTree.GetCumulativeRssmem(0) == 2048);
			NUnit.Framework.Assert.IsTrue(pTree.GetCumulativeCpuTime() == 1000);
			pTree.infoStr = "3524,1024,1024,1000\r\n2844,1024,1024,1000\r\n1234,1024,1024,1000\r\n";
			pTree.UpdateProcessTree();
			NUnit.Framework.Assert.IsTrue(pTree.GetVirtualMemorySize() == 3072);
			NUnit.Framework.Assert.IsTrue(pTree.GetCumulativeVmem() == 3072);
			NUnit.Framework.Assert.IsTrue(pTree.GetVirtualMemorySize(1) == 2048);
			NUnit.Framework.Assert.IsTrue(pTree.GetCumulativeVmem(1) == 2048);
			NUnit.Framework.Assert.IsTrue(pTree.GetRssMemorySize() == 3072);
			NUnit.Framework.Assert.IsTrue(pTree.GetCumulativeRssmem() == 3072);
			NUnit.Framework.Assert.IsTrue(pTree.GetRssMemorySize(1) == 2048);
			NUnit.Framework.Assert.IsTrue(pTree.GetCumulativeRssmem(1) == 2048);
			NUnit.Framework.Assert.IsTrue(pTree.GetCumulativeCpuTime() == 3000);
			pTree.infoStr = "3524,1024,1024,1500\r\n2844,1024,1024,1500\r\n";
			pTree.UpdateProcessTree();
			NUnit.Framework.Assert.IsTrue(pTree.GetVirtualMemorySize() == 2048);
			NUnit.Framework.Assert.IsTrue(pTree.GetCumulativeVmem() == 2048);
			NUnit.Framework.Assert.IsTrue(pTree.GetVirtualMemorySize(2) == 2048);
			NUnit.Framework.Assert.IsTrue(pTree.GetCumulativeVmem(2) == 2048);
			NUnit.Framework.Assert.IsTrue(pTree.GetRssMemorySize() == 2048);
			NUnit.Framework.Assert.IsTrue(pTree.GetCumulativeRssmem() == 2048);
			NUnit.Framework.Assert.IsTrue(pTree.GetRssMemorySize(2) == 2048);
			NUnit.Framework.Assert.IsTrue(pTree.GetCumulativeRssmem(2) == 2048);
			NUnit.Framework.Assert.IsTrue(pTree.GetCumulativeCpuTime() == 4000);
		}
	}
}
