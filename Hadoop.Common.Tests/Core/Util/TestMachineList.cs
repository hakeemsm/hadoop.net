using System;
using System.Collections.Generic;
using System.Net;


namespace Org.Apache.Hadoop.Util
{
	public class TestMachineList
	{
		private static string IpList = "10.119.103.110,10.119.103.112,10.119.103.114";

		private static string IpListSpaces = " 10.119.103.110 , 10.119.103.112,10.119.103.114 ,10.119.103.110, ";

		private static string CidrList = "10.222.0.0/16,10.241.23.0/24";

		private static string CidrList1 = "10.222.0.0/16";

		private static string CidrList2 = "10.241.23.0/24";

		private static string InvalidCidr = "10.241/24";

		private static string IpCidrList = "10.222.0.0/16,10.119.103.110,10.119.103.112,10.119.103.114,10.241.23.0/24";

		private static string HostList = "host1,host4";

		private static string HostnameIpCidrList = "host1,10.222.0.0/16,10.119.103.110,10.119.103.112,10.119.103.114,10.241.23.0/24,host4,";

		[Fact]
		public virtual void TestWildCard()
		{
			//create MachineList with a list of of IPs
			MachineList ml = new MachineList("*");
			//test for inclusion with any IP
			Assert.True(ml.Includes("10.119.103.112"));
			Assert.True(ml.Includes("1.2.3.4"));
		}

		[Fact]
		public virtual void TestIPList()
		{
			//create MachineList with a list of of IPs
			MachineList ml = new MachineList(IpList);
			//test for inclusion with an known IP
			Assert.True(ml.Includes("10.119.103.112"));
			//test for exclusion with an unknown IP
			NUnit.Framework.Assert.IsFalse(ml.Includes("10.119.103.111"));
		}

		[Fact]
		public virtual void TestIPListSpaces()
		{
			//create MachineList with a ip string which has duplicate ip and spaces
			MachineList ml = new MachineList(IpListSpaces);
			//test for inclusion with an known IP
			Assert.True(ml.Includes("10.119.103.112"));
			//test for exclusion with an unknown IP
			NUnit.Framework.Assert.IsFalse(ml.Includes("10.119.103.111"));
		}

		/// <exception cref="UnknownHostException"/>
		[Fact]
		public virtual void TestStaticIPHostNameList()
		{
			//create MachineList with a list of of Hostnames
			IPAddress addressHost1 = Extensions.GetAddressByName("1.2.3.1");
			IPAddress addressHost4 = Extensions.GetAddressByName("1.2.3.4");
			MachineList.InetAddressFactory addressFactory = Org.Mockito.Mockito.Mock<MachineList.InetAddressFactory
				>();
			Org.Mockito.Mockito.When(addressFactory.GetByName("host1")).ThenReturn(addressHost1
				);
			Org.Mockito.Mockito.When(addressFactory.GetByName("host4")).ThenReturn(addressHost4
				);
			MachineList ml = new MachineList(StringUtils.GetTrimmedStringCollection(HostList)
				, addressFactory);
			//test for inclusion with an known IP
			Assert.True(ml.Includes("1.2.3.4"));
			//test for exclusion with an unknown IP
			NUnit.Framework.Assert.IsFalse(ml.Includes("1.2.3.5"));
		}

		/// <exception cref="UnknownHostException"/>
		[Fact]
		public virtual void TestHostNames()
		{
			//create MachineList with a list of of Hostnames
			IPAddress addressHost1 = Extensions.GetAddressByName("1.2.3.1");
			IPAddress addressHost4 = Extensions.GetAddressByName("1.2.3.4");
			IPAddress addressMockHost4 = Org.Mockito.Mockito.Mock<IPAddress>();
			Org.Mockito.Mockito.When(addressMockHost4.ToString()).ThenReturn("differentName");
			IPAddress addressMockHost5 = Org.Mockito.Mockito.Mock<IPAddress>();
			Org.Mockito.Mockito.When(addressMockHost5.ToString()).ThenReturn("host5");
			MachineList.InetAddressFactory addressFactory = Org.Mockito.Mockito.Mock<MachineList.InetAddressFactory
				>();
			Org.Mockito.Mockito.When(addressFactory.GetByName("1.2.3.4")).ThenReturn(addressMockHost4
				);
			Org.Mockito.Mockito.When(addressFactory.GetByName("1.2.3.5")).ThenReturn(addressMockHost5
				);
			Org.Mockito.Mockito.When(addressFactory.GetByName("host1")).ThenReturn(addressHost1
				);
			Org.Mockito.Mockito.When(addressFactory.GetByName("host4")).ThenReturn(addressHost4
				);
			MachineList ml = new MachineList(StringUtils.GetTrimmedStringCollection(HostList)
				, addressFactory);
			//test for inclusion with an known IP
			Assert.True(ml.Includes("1.2.3.4"));
			//test for exclusion with an unknown IP
			NUnit.Framework.Assert.IsFalse(ml.Includes("1.2.3.5"));
		}

		/// <exception cref="UnknownHostException"/>
		[Fact]
		public virtual void TestHostNamesReverserIpMatch()
		{
			//create MachineList with a list of of Hostnames
			IPAddress addressHost1 = Extensions.GetAddressByName("1.2.3.1");
			IPAddress addressHost4 = Extensions.GetAddressByName("1.2.3.4");
			IPAddress addressMockHost4 = Org.Mockito.Mockito.Mock<IPAddress>();
			Org.Mockito.Mockito.When(addressMockHost4.ToString()).ThenReturn("host4");
			IPAddress addressMockHost5 = Org.Mockito.Mockito.Mock<IPAddress>();
			Org.Mockito.Mockito.When(addressMockHost5.ToString()).ThenReturn("host5");
			MachineList.InetAddressFactory addressFactory = Org.Mockito.Mockito.Mock<MachineList.InetAddressFactory
				>();
			Org.Mockito.Mockito.When(addressFactory.GetByName("1.2.3.4")).ThenReturn(addressMockHost4
				);
			Org.Mockito.Mockito.When(addressFactory.GetByName("1.2.3.5")).ThenReturn(addressMockHost5
				);
			Org.Mockito.Mockito.When(addressFactory.GetByName("host1")).ThenReturn(addressHost1
				);
			Org.Mockito.Mockito.When(addressFactory.GetByName("host4")).ThenReturn(addressHost4
				);
			MachineList ml = new MachineList(StringUtils.GetTrimmedStringCollection(HostList)
				, addressFactory);
			//test for inclusion with an known IP
			Assert.True(ml.Includes("1.2.3.4"));
			//test for exclusion with an unknown IP
			NUnit.Framework.Assert.IsFalse(ml.Includes("1.2.3.5"));
		}

		[Fact]
		public virtual void TestCIDRs()
		{
			//create MachineList with a list of of ip ranges specified in CIDR format
			MachineList ml = new MachineList(CidrList);
			//test for inclusion/exclusion 
			NUnit.Framework.Assert.IsFalse(ml.Includes("10.221.255.255"));
			Assert.True(ml.Includes("10.222.0.0"));
			Assert.True(ml.Includes("10.222.0.1"));
			Assert.True(ml.Includes("10.222.0.255"));
			Assert.True(ml.Includes("10.222.255.0"));
			Assert.True(ml.Includes("10.222.255.254"));
			Assert.True(ml.Includes("10.222.255.255"));
			NUnit.Framework.Assert.IsFalse(ml.Includes("10.223.0.0"));
			Assert.True(ml.Includes("10.241.23.0"));
			Assert.True(ml.Includes("10.241.23.1"));
			Assert.True(ml.Includes("10.241.23.254"));
			Assert.True(ml.Includes("10.241.23.255"));
			//test for exclusion with an unknown IP
			NUnit.Framework.Assert.IsFalse(ml.Includes("10.119.103.111"));
		}

		[Fact]
		public virtual void TestCIDRWith16bitmask()
		{
			//create MachineList with a list of of ip ranges specified in CIDR format
			MachineList ml = new MachineList(CidrList1);
			//test for inclusion/exclusion 
			NUnit.Framework.Assert.IsFalse(ml.Includes("10.221.255.255"));
			Assert.True(ml.Includes("10.222.0.0"));
			Assert.True(ml.Includes("10.222.0.1"));
			Assert.True(ml.Includes("10.222.0.255"));
			Assert.True(ml.Includes("10.222.255.0"));
			Assert.True(ml.Includes("10.222.255.254"));
			Assert.True(ml.Includes("10.222.255.255"));
			NUnit.Framework.Assert.IsFalse(ml.Includes("10.223.0.0"));
			//test for exclusion with an unknown IP
			NUnit.Framework.Assert.IsFalse(ml.Includes("10.119.103.111"));
		}

		[Fact]
		public virtual void TestCIDRWith8BitMask()
		{
			//create MachineList with a list of of ip ranges specified in CIDR format
			MachineList ml = new MachineList(CidrList2);
			//test for inclusion/exclusion  
			NUnit.Framework.Assert.IsFalse(ml.Includes("10.241.22.255"));
			Assert.True(ml.Includes("10.241.23.0"));
			Assert.True(ml.Includes("10.241.23.1"));
			Assert.True(ml.Includes("10.241.23.254"));
			Assert.True(ml.Includes("10.241.23.255"));
			NUnit.Framework.Assert.IsFalse(ml.Includes("10.241.24.0"));
			//test for exclusion with an unknown IP
			NUnit.Framework.Assert.IsFalse(ml.Includes("10.119.103.111"));
		}

		//test invalid cidr
		[Fact]
		public virtual void TestInvalidCIDR()
		{
			//create MachineList with an Invalid CIDR
			try
			{
				new MachineList(InvalidCidr);
				NUnit.Framework.Assert.Fail("Expected IllegalArgumentException");
			}
			catch (ArgumentException)
			{
			}
			catch
			{
				//expected Exception
				NUnit.Framework.Assert.Fail("Expected only IllegalArgumentException");
			}
		}

		//
		[Fact]
		public virtual void TestIPandCIDRs()
		{
			//create MachineList with a list of of ip ranges and ip addresses
			MachineList ml = new MachineList(IpCidrList);
			//test for inclusion with an known IP
			Assert.True(ml.Includes("10.119.103.112"));
			//test for exclusion with an unknown IP
			NUnit.Framework.Assert.IsFalse(ml.Includes("10.119.103.111"));
			//CIDR Ranges
			NUnit.Framework.Assert.IsFalse(ml.Includes("10.221.255.255"));
			Assert.True(ml.Includes("10.222.0.0"));
			Assert.True(ml.Includes("10.222.255.255"));
			NUnit.Framework.Assert.IsFalse(ml.Includes("10.223.0.0"));
			NUnit.Framework.Assert.IsFalse(ml.Includes("10.241.22.255"));
			Assert.True(ml.Includes("10.241.23.0"));
			Assert.True(ml.Includes("10.241.23.255"));
			NUnit.Framework.Assert.IsFalse(ml.Includes("10.241.24.0"));
		}

		[Fact]
		public virtual void TestHostNameIPandCIDRs()
		{
			//create MachineList with a mix of ip addresses , hostnames and ip ranges
			MachineList ml = new MachineList(HostnameIpCidrList);
			//test for inclusion with an known IP
			Assert.True(ml.Includes("10.119.103.112"));
			//test for exclusion with an unknown IP
			NUnit.Framework.Assert.IsFalse(ml.Includes("10.119.103.111"));
			//CIDR Ranges
			NUnit.Framework.Assert.IsFalse(ml.Includes("10.221.255.255"));
			Assert.True(ml.Includes("10.222.0.0"));
			Assert.True(ml.Includes("10.222.255.255"));
			NUnit.Framework.Assert.IsFalse(ml.Includes("10.223.0.0"));
			NUnit.Framework.Assert.IsFalse(ml.Includes("10.241.22.255"));
			Assert.True(ml.Includes("10.241.23.0"));
			Assert.True(ml.Includes("10.241.23.255"));
			NUnit.Framework.Assert.IsFalse(ml.Includes("10.241.24.0"));
		}

		[Fact]
		public virtual void TestGetCollection()
		{
			//create MachineList with a mix of ip addresses , hostnames and ip ranges
			MachineList ml = new MachineList(HostnameIpCidrList);
			ICollection<string> col = ml.GetCollection();
			//test getCollectionton to return the full collection
			Assert.Equal(7, ml.GetCollection().Count);
			foreach (string item in StringUtils.GetTrimmedStringCollection(HostnameIpCidrList
				))
			{
				Assert.True(col.Contains(item));
			}
		}
	}
}
