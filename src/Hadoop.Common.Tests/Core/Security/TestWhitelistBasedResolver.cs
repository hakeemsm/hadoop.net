using System.Collections.Generic;
using System.Net;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.Security
{
	public class TestWhitelistBasedResolver : TestCase
	{
		public static readonly IDictionary<string, string> SaslPrivacyProps = WhitelistBasedResolver
			.GetSaslProperties(new Configuration());

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFixedVariableAndLocalWhiteList()
		{
			string[] fixedIps = new string[] { "10.119.103.112", "10.221.102.0/23" };
			TestFileBasedIPList.CreateFileWithEntries("fixedwhitelist.txt", fixedIps);
			string[] variableIps = new string[] { "10.222.0.0/16", "10.113.221.221" };
			TestFileBasedIPList.CreateFileWithEntries("variablewhitelist.txt", variableIps);
			Configuration conf = new Configuration();
			conf.Set(WhitelistBasedResolver.HadoopSecuritySaslFixedwhitelistFile, "fixedwhitelist.txt"
				);
			conf.SetBoolean(WhitelistBasedResolver.HadoopSecuritySaslVariablewhitelistEnable, 
				true);
			conf.SetLong(WhitelistBasedResolver.HadoopSecuritySaslVariablewhitelistCacheSecs, 
				1);
			conf.Set(WhitelistBasedResolver.HadoopSecuritySaslVariablewhitelistFile, "variablewhitelist.txt"
				);
			WhitelistBasedResolver wqr = new WhitelistBasedResolver();
			wqr.SetConf(conf);
			Assert.Equal(wqr.GetDefaultProperties(), wqr.GetServerProperties
				(Extensions.GetAddressByName("10.119.103.112")));
			Assert.Equal(SaslPrivacyProps, wqr.GetServerProperties("10.119.103.113"
				));
			Assert.Equal(wqr.GetDefaultProperties(), wqr.GetServerProperties
				("10.221.103.121"));
			Assert.Equal(SaslPrivacyProps, wqr.GetServerProperties("10.221.104.0"
				));
			Assert.Equal(wqr.GetDefaultProperties(), wqr.GetServerProperties
				("10.222.103.121"));
			Assert.Equal(SaslPrivacyProps, wqr.GetServerProperties("10.223.104.0"
				));
			Assert.Equal(wqr.GetDefaultProperties(), wqr.GetServerProperties
				("10.113.221.221"));
			Assert.Equal(SaslPrivacyProps, wqr.GetServerProperties("10.113.221.222"
				));
			Assert.Equal(wqr.GetDefaultProperties(), wqr.GetServerProperties
				("127.0.0.1"));
			TestFileBasedIPList.RemoveFile("fixedwhitelist.txt");
			TestFileBasedIPList.RemoveFile("variablewhitelist.txt");
		}

		/// <summary>
		/// Add a bunch of subnets and IPSs to the whitelist
		/// Check  for inclusion in whitelist
		/// Check for exclusion from whitelist
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFixedAndLocalWhiteList()
		{
			string[] fixedIps = new string[] { "10.119.103.112", "10.221.102.0/23" };
			TestFileBasedIPList.CreateFileWithEntries("fixedwhitelist.txt", fixedIps);
			string[] variableIps = new string[] { "10.222.0.0/16", "10.113.221.221" };
			TestFileBasedIPList.CreateFileWithEntries("variablewhitelist.txt", variableIps);
			Configuration conf = new Configuration();
			conf.Set(WhitelistBasedResolver.HadoopSecuritySaslFixedwhitelistFile, "fixedwhitelist.txt"
				);
			conf.SetBoolean(WhitelistBasedResolver.HadoopSecuritySaslVariablewhitelistEnable, 
				false);
			conf.SetLong(WhitelistBasedResolver.HadoopSecuritySaslVariablewhitelistCacheSecs, 
				100);
			conf.Set(WhitelistBasedResolver.HadoopSecuritySaslVariablewhitelistFile, "variablewhitelist.txt"
				);
			WhitelistBasedResolver wqr = new WhitelistBasedResolver();
			wqr.SetConf(conf);
			Assert.Equal(wqr.GetDefaultProperties(), wqr.GetServerProperties
				(Extensions.GetAddressByName("10.119.103.112")));
			Assert.Equal(SaslPrivacyProps, wqr.GetServerProperties("10.119.103.113"
				));
			Assert.Equal(wqr.GetDefaultProperties(), wqr.GetServerProperties
				("10.221.103.121"));
			Assert.Equal(SaslPrivacyProps, wqr.GetServerProperties("10.221.104.0"
				));
			Assert.Equal(SaslPrivacyProps, wqr.GetServerProperties("10.222.103.121"
				));
			Assert.Equal(SaslPrivacyProps, wqr.GetServerProperties("10.223.104.0"
				));
			Assert.Equal(SaslPrivacyProps, wqr.GetServerProperties("10.113.221.221"
				));
			Assert.Equal(SaslPrivacyProps, wqr.GetServerProperties("10.113.221.222"
				));
			Assert.Equal(wqr.GetDefaultProperties(), wqr.GetServerProperties
				("127.0.0.1"));
			TestFileBasedIPList.RemoveFile("fixedwhitelist.txt");
			TestFileBasedIPList.RemoveFile("variablewhitelist.txt");
		}

		/// <summary>
		/// Add a bunch of subnets and IPSs to the whitelist
		/// Check  for inclusion in whitelist with a null value
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestNullIPAddress()
		{
			string[] fixedIps = new string[] { "10.119.103.112", "10.221.102.0/23" };
			TestFileBasedIPList.CreateFileWithEntries("fixedwhitelist.txt", fixedIps);
			string[] variableIps = new string[] { "10.222.0.0/16", "10.113.221.221" };
			TestFileBasedIPList.CreateFileWithEntries("variablewhitelist.txt", variableIps);
			Configuration conf = new Configuration();
			conf.Set(WhitelistBasedResolver.HadoopSecuritySaslFixedwhitelistFile, "fixedwhitelist.txt"
				);
			conf.SetBoolean(WhitelistBasedResolver.HadoopSecuritySaslVariablewhitelistEnable, 
				true);
			conf.SetLong(WhitelistBasedResolver.HadoopSecuritySaslVariablewhitelistCacheSecs, 
				100);
			conf.Set(WhitelistBasedResolver.HadoopSecuritySaslVariablewhitelistFile, "variablewhitelist.txt"
				);
			WhitelistBasedResolver wqr = new WhitelistBasedResolver();
			wqr.SetConf(conf);
			Assert.Equal(SaslPrivacyProps, wqr.GetServerProperties((IPAddress
				)null));
			Assert.Equal(SaslPrivacyProps, wqr.GetServerProperties((string
				)null));
			TestFileBasedIPList.RemoveFile("fixedwhitelist.txt");
			TestFileBasedIPList.RemoveFile("variablewhitelist.txt");
		}
	}
}
