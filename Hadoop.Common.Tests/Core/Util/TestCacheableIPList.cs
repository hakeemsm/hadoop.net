using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	public class TestCacheableIPList : TestCase
	{
		/// <summary>
		/// Add a bunch of subnets and IPSs to the file
		/// setup a low cache refresh
		/// test for inclusion
		/// Check for exclusion
		/// Add a bunch of subnets and Ips
		/// wait for cache timeout.
		/// </summary>
		/// <remarks>
		/// Add a bunch of subnets and IPSs to the file
		/// setup a low cache refresh
		/// test for inclusion
		/// Check for exclusion
		/// Add a bunch of subnets and Ips
		/// wait for cache timeout.
		/// test for inclusion
		/// Check for exclusion
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestAddWithSleepForCacheTimeout()
		{
			string[] ips = new string[] { "10.119.103.112", "10.221.102.0/23", "10.113.221.221"
				 };
			TestFileBasedIPList.CreateFileWithEntries("ips.txt", ips);
			CacheableIPList cipl = new CacheableIPList(new FileBasedIPList("ips.txt"), 100);
			NUnit.Framework.Assert.IsFalse("10.113.221.222 is in the list", cipl.IsIn("10.113.221.222"
				));
			NUnit.Framework.Assert.IsFalse("10.222.103.121 is  in the list", cipl.IsIn("10.222.103.121"
				));
			TestFileBasedIPList.RemoveFile("ips.txt");
			string[] ips2 = new string[] { "10.119.103.112", "10.221.102.0/23", "10.222.0.0/16"
				, "10.113.221.221", "10.113.221.222" };
			TestFileBasedIPList.CreateFileWithEntries("ips.txt", ips2);
			Sharpen.Thread.Sleep(101);
			NUnit.Framework.Assert.IsTrue("10.113.221.222 is not in the list", cipl.IsIn("10.113.221.222"
				));
			NUnit.Framework.Assert.IsTrue("10.222.103.121 is not in the list", cipl.IsIn("10.222.103.121"
				));
			TestFileBasedIPList.RemoveFile("ips.txt");
		}

		/// <summary>
		/// Add a bunch of subnets and IPSs to the file
		/// setup a low cache refresh
		/// test for inclusion
		/// Check for exclusion
		/// Remove a bunch of subnets and Ips
		/// wait for cache timeout.
		/// </summary>
		/// <remarks>
		/// Add a bunch of subnets and IPSs to the file
		/// setup a low cache refresh
		/// test for inclusion
		/// Check for exclusion
		/// Remove a bunch of subnets and Ips
		/// wait for cache timeout.
		/// test for inclusion
		/// Check for exclusion
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestRemovalWithSleepForCacheTimeout()
		{
			string[] ips = new string[] { "10.119.103.112", "10.221.102.0/23", "10.222.0.0/16"
				, "10.113.221.221", "10.113.221.222" };
			TestFileBasedIPList.CreateFileWithEntries("ips.txt", ips);
			CacheableIPList cipl = new CacheableIPList(new FileBasedIPList("ips.txt"), 100);
			NUnit.Framework.Assert.IsTrue("10.113.221.222 is not in the list", cipl.IsIn("10.113.221.222"
				));
			NUnit.Framework.Assert.IsTrue("10.222.103.121 is not in the list", cipl.IsIn("10.222.103.121"
				));
			TestFileBasedIPList.RemoveFile("ips.txt");
			string[] ips2 = new string[] { "10.119.103.112", "10.221.102.0/23", "10.113.221.221"
				 };
			TestFileBasedIPList.CreateFileWithEntries("ips.txt", ips2);
			Sharpen.Thread.Sleep(1005);
			NUnit.Framework.Assert.IsFalse("10.113.221.222 is in the list", cipl.IsIn("10.113.221.222"
				));
			NUnit.Framework.Assert.IsFalse("10.222.103.121 is  in the list", cipl.IsIn("10.222.103.121"
				));
			TestFileBasedIPList.RemoveFile("ips.txt");
		}

		/// <summary>
		/// Add a bunch of subnets and IPSs to the file
		/// setup a low cache refresh
		/// test for inclusion
		/// Check for exclusion
		/// Add a bunch of subnets and Ips
		/// do a refresh
		/// test for inclusion
		/// Check for exclusion
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestAddWithRefresh()
		{
			string[] ips = new string[] { "10.119.103.112", "10.221.102.0/23", "10.113.221.221"
				 };
			TestFileBasedIPList.CreateFileWithEntries("ips.txt", ips);
			CacheableIPList cipl = new CacheableIPList(new FileBasedIPList("ips.txt"), 100);
			NUnit.Framework.Assert.IsFalse("10.113.221.222 is in the list", cipl.IsIn("10.113.221.222"
				));
			NUnit.Framework.Assert.IsFalse("10.222.103.121 is  in the list", cipl.IsIn("10.222.103.121"
				));
			TestFileBasedIPList.RemoveFile("ips.txt");
			string[] ips2 = new string[] { "10.119.103.112", "10.221.102.0/23", "10.222.0.0/16"
				, "10.113.221.221", "10.113.221.222" };
			TestFileBasedIPList.CreateFileWithEntries("ips.txt", ips2);
			cipl.Refresh();
			NUnit.Framework.Assert.IsTrue("10.113.221.222 is not in the list", cipl.IsIn("10.113.221.222"
				));
			NUnit.Framework.Assert.IsTrue("10.222.103.121 is not in the list", cipl.IsIn("10.222.103.121"
				));
			TestFileBasedIPList.RemoveFile("ips.txt");
		}

		/// <summary>
		/// Add a bunch of subnets and IPSs to the file
		/// setup a low cache refresh
		/// test for inclusion
		/// Check for exclusion
		/// Remove a bunch of subnets and Ips
		/// wait for cache timeout.
		/// </summary>
		/// <remarks>
		/// Add a bunch of subnets and IPSs to the file
		/// setup a low cache refresh
		/// test for inclusion
		/// Check for exclusion
		/// Remove a bunch of subnets and Ips
		/// wait for cache timeout.
		/// test for inclusion
		/// Check for exclusion
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestRemovalWithRefresh()
		{
			string[] ips = new string[] { "10.119.103.112", "10.221.102.0/23", "10.222.0.0/16"
				, "10.113.221.221", "10.113.221.222" };
			TestFileBasedIPList.CreateFileWithEntries("ips.txt", ips);
			CacheableIPList cipl = new CacheableIPList(new FileBasedIPList("ips.txt"), 100);
			NUnit.Framework.Assert.IsTrue("10.113.221.222 is not in the list", cipl.IsIn("10.113.221.222"
				));
			NUnit.Framework.Assert.IsTrue("10.222.103.121 is not in the list", cipl.IsIn("10.222.103.121"
				));
			TestFileBasedIPList.RemoveFile("ips.txt");
			string[] ips2 = new string[] { "10.119.103.112", "10.221.102.0/23", "10.113.221.221"
				 };
			TestFileBasedIPList.CreateFileWithEntries("ips.txt", ips2);
			cipl.Refresh();
			NUnit.Framework.Assert.IsFalse("10.113.221.222 is in the list", cipl.IsIn("10.113.221.222"
				));
			NUnit.Framework.Assert.IsFalse("10.222.103.121 is  in the list", cipl.IsIn("10.222.103.121"
				));
			TestFileBasedIPList.RemoveFile("ips.txt");
		}
	}
}
