using System;
using NUnit.Framework;
using Org.Apache.Commons.IO;


namespace Org.Apache.Hadoop.Util
{
	public class TestFileBasedIPList : TestCase
	{
		[NUnit.Framework.TearDown]
		protected override void TearDown()
		{
			RemoveFile("ips.txt");
		}

		/// <summary>
		/// Add a bunch of IPS  to the file
		/// Check  for inclusion
		/// Check for exclusion
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestSubnetsAndIPs()
		{
			string[] ips = new string[] { "10.119.103.112", "10.221.102.0/23" };
			CreateFileWithEntries("ips.txt", ips);
			IPList ipList = new FileBasedIPList("ips.txt");
			Assert.True("10.119.103.112 is not in the list", ipList.IsIn("10.119.103.112"
				));
			NUnit.Framework.Assert.IsFalse("10.119.103.113 is in the list", ipList.IsIn("10.119.103.113"
				));
			Assert.True("10.221.102.0 is not in the list", ipList.IsIn("10.221.102.0"
				));
			Assert.True("10.221.102.1 is not in the list", ipList.IsIn("10.221.102.1"
				));
			Assert.True("10.221.103.1 is not in the list", ipList.IsIn("10.221.103.1"
				));
			Assert.True("10.221.103.255 is not in the list", ipList.IsIn("10.221.103.255"
				));
			NUnit.Framework.Assert.IsFalse("10.221.104.0 is in the list", ipList.IsIn("10.221.104.0"
				));
			NUnit.Framework.Assert.IsFalse("10.221.104.1 is in the list", ipList.IsIn("10.221.104.1"
				));
		}

		/// <summary>
		/// Add a bunch of IPS  to the file
		/// Check  for inclusion
		/// Check for exclusion
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestNullIP()
		{
			string[] ips = new string[] { "10.119.103.112", "10.221.102.0/23" };
			CreateFileWithEntries("ips.txt", ips);
			IPList ipList = new FileBasedIPList("ips.txt");
			NUnit.Framework.Assert.IsFalse("Null Ip is in the list", ipList.IsIn(null));
		}

		/// <summary>
		/// Add a bunch of subnets and IPSs to the file
		/// Check  for inclusion
		/// Check for exclusion
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestWithMultipleSubnetAndIPs()
		{
			string[] ips = new string[] { "10.119.103.112", "10.221.102.0/23", "10.222.0.0/16"
				, "10.113.221.221" };
			CreateFileWithEntries("ips.txt", ips);
			IPList ipList = new FileBasedIPList("ips.txt");
			Assert.True("10.119.103.112 is not in the list", ipList.IsIn("10.119.103.112"
				));
			NUnit.Framework.Assert.IsFalse("10.119.103.113 is in the list", ipList.IsIn("10.119.103.113"
				));
			Assert.True("10.221.103.121 is not in the list", ipList.IsIn("10.221.103.121"
				));
			NUnit.Framework.Assert.IsFalse("10.221.104.0 is in the list", ipList.IsIn("10.221.104.0"
				));
			Assert.True("10.222.103.121 is not in the list", ipList.IsIn("10.222.103.121"
				));
			NUnit.Framework.Assert.IsFalse("10.223.104.0 is in the list", ipList.IsIn("10.223.104.0"
				));
			Assert.True("10.113.221.221 is not in the list", ipList.IsIn("10.113.221.221"
				));
			NUnit.Framework.Assert.IsFalse("10.113.221.222 is in the list", ipList.IsIn("10.113.221.222"
				));
		}

		/// <summary>
		/// Do not specify the file
		/// test for inclusion
		/// should be true as if the feature is turned off
		/// </summary>
		public virtual void TestFileNotSpecified()
		{
			IPList ipl = new FileBasedIPList(null);
			NUnit.Framework.Assert.IsFalse("110.113.221.222 is in the list", ipl.IsIn("110.113.221.222"
				));
		}

		/// <summary>
		/// Specify a non existent file
		/// test for inclusion
		/// should be true as if the feature is turned off
		/// </summary>
		public virtual void TestFileMissing()
		{
			IPList ipl = new FileBasedIPList("missingips.txt");
			NUnit.Framework.Assert.IsFalse("110.113.221.222 is in the list", ipl.IsIn("110.113.221.222"
				));
		}

		/// <summary>
		/// Specify an existing file, but empty
		/// test for inclusion
		/// should be true as if the feature is turned off
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestWithEmptyList()
		{
			string[] ips = new string[] {  };
			CreateFileWithEntries("ips.txt", ips);
			IPList ipl = new FileBasedIPList("ips.txt");
			NUnit.Framework.Assert.IsFalse("110.113.221.222 is in the list", ipl.IsIn("110.113.221.222"
				));
		}

		/// <summary>
		/// Specify an existing file, but ips in wrong format
		/// test for inclusion
		/// should be true as if the feature is turned off
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestForBadFIle()
		{
			string[] ips = new string[] { "10.221.102/23" };
			CreateFileWithEntries("ips.txt", ips);
			try
			{
				new FileBasedIPList("ips.txt");
				Fail();
			}
			catch (Exception)
			{
			}
		}

		//expects Exception
		/// <summary>Add a bunch of subnets and IPSs to the file.</summary>
		/// <remarks>
		/// Add a bunch of subnets and IPSs to the file. Keep one entry wrong.
		/// The good entries will still be used.
		/// Check  for inclusion with good entries
		/// Check for exclusion
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestWithAWrongEntry()
		{
			string[] ips = new string[] { "10.119.103.112", "10.221.102/23", "10.221.204.1/23"
				 };
			CreateFileWithEntries("ips.txt", ips);
			try
			{
				new FileBasedIPList("ips.txt");
				Fail();
			}
			catch (Exception)
			{
			}
		}

		//expects Exception
		/// <exception cref="System.IO.IOException"/>
		public static void CreateFileWithEntries(string fileName, string[] ips)
		{
			FileUtils.WriteLines(new FilePath(fileName), Arrays.AsList(ips));
		}

		public static void RemoveFile(string fileName)
		{
			FilePath file = new FilePath(fileName);
			if (file.Exists())
			{
				new FilePath(fileName).Delete();
			}
		}
	}
}
