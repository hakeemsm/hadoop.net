using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Com.Google.Common.Base;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools
{
	/// <summary>
	/// Test for
	/// <see cref="GetConf"/>
	/// </summary>
	public class TestGetConf
	{
		internal enum TestType
		{
			Namenode,
			Backup,
			Secondary,
			Nnrpcaddresses
		}

		internal FileSystem localFileSys;

		/// <summary>Setup federation nameServiceIds in the configuration</summary>
		private void SetupNameServices(HdfsConfiguration conf, int nameServiceIdCount)
		{
			StringBuilder nsList = new StringBuilder();
			for (int i = 0; i < nameServiceIdCount; i++)
			{
				if (nsList.Length > 0)
				{
					nsList.Append(",");
				}
				nsList.Append(GetNameServiceId(i));
			}
			conf.Set(DFSConfigKeys.DfsNameservices, nsList.ToString());
		}

		/// <summary>Set a given key with value as address, for all the nameServiceIds.</summary>
		/// <param name="conf">configuration to set the addresses in</param>
		/// <param name="key">configuration key</param>
		/// <param name="nameServiceIdCount">Number of nameServices for which the key is set</param>
		/// <param name="portOffset">starting port offset</param>
		/// <returns>list of addresses that are set in the configuration</returns>
		private string[] SetupAddress(HdfsConfiguration conf, string key, int nameServiceIdCount
			, int portOffset)
		{
			string[] values = new string[nameServiceIdCount];
			for (int i = 0; i < nameServiceIdCount; i++, portOffset++)
			{
				string nsID = GetNameServiceId(i);
				string specificKey = DFSUtil.AddKeySuffixes(key, nsID);
				values[i] = "nn" + i + ":" + portOffset;
				conf.Set(specificKey, values[i]);
			}
			return values;
		}

		/// <summary>
		/// Add namenodes to the static resolution list to avoid going
		/// through DNS which can be really slow in some configurations.
		/// </summary>
		private void SetupStaticHostResolution(int nameServiceIdCount)
		{
			for (int i = 0; i < nameServiceIdCount; i++)
			{
				NetUtils.AddStaticResolution("nn" + i, "localhost");
			}
		}

		/*
		* Convert the map returned from DFSUtil functions to an array of
		* addresses represented as "host:port"
		*/
		private string[] ToStringArray(IList<DFSUtil.ConfiguredNNAddress> list)
		{
			string[] ret = new string[list.Count];
			for (int i = 0; i < list.Count; i++)
			{
				ret[i] = NetUtils.GetHostPortString(list[i].GetAddress());
			}
			return ret;
		}

		/// <summary>
		/// Using DFSUtil methods get the list of given
		/// <paramref name="type"/>
		/// of address
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private IDictionary<string, IDictionary<string, IPEndPoint>> GetAddressListFromConf
			(TestGetConf.TestType type, HdfsConfiguration conf)
		{
			switch (type)
			{
				case TestGetConf.TestType.Namenode:
				{
					return DFSUtil.GetNNServiceRpcAddressesForCluster(conf);
				}

				case TestGetConf.TestType.Backup:
				{
					return DFSUtil.GetBackupNodeAddresses(conf);
				}

				case TestGetConf.TestType.Secondary:
				{
					return DFSUtil.GetSecondaryNameNodeAddresses(conf);
				}

				case TestGetConf.TestType.Nnrpcaddresses:
				{
					return DFSUtil.GetNNServiceRpcAddressesForCluster(conf);
				}
			}
			return null;
		}

		/// <exception cref="System.Exception"/>
		private string RunTool(HdfsConfiguration conf, string[] args, bool success)
		{
			ByteArrayOutputStream o = new ByteArrayOutputStream();
			TextWriter @out = new TextWriter(o, true);
			try
			{
				int ret = ToolRunner.Run(new GetConf(conf, @out, @out), args);
				@out.Flush();
				System.Console.Error.WriteLine("Output: " + o.ToString());
				NUnit.Framework.Assert.AreEqual("Expected " + (success ? "success" : "failure") +
					 " for args: " + Joiner.On(" ").Join(args) + "\n" + "Output: " + o.ToString(), success
					, ret == 0);
				return o.ToString();
			}
			finally
			{
				o.Close();
				@out.Close();
			}
		}

		/// <summary>Get address list for a given type of address.</summary>
		/// <remarks>
		/// Get address list for a given type of address. Command expected to
		/// fail if
		/// <paramref name="success"/>
		/// is false.
		/// </remarks>
		/// <returns>returns the success or error output from the tool.</returns>
		/// <exception cref="System.Exception"/>
		private string GetAddressListFromTool(TestGetConf.TestType type, HdfsConfiguration
			 conf, bool success)
		{
			string[] args = new string[1];
			switch (type)
			{
				case TestGetConf.TestType.Namenode:
				{
					args[0] = GetConf.Command.Namenode.GetName();
					break;
				}

				case TestGetConf.TestType.Backup:
				{
					args[0] = GetConf.Command.Backup.GetName();
					break;
				}

				case TestGetConf.TestType.Secondary:
				{
					args[0] = GetConf.Command.Secondary.GetName();
					break;
				}

				case TestGetConf.TestType.Nnrpcaddresses:
				{
					args[0] = GetConf.Command.Nnrpcaddresses.GetName();
					break;
				}
			}
			return RunTool(conf, args, success);
		}

		/// <summary>
		/// Using
		/// <see cref="GetConf"/>
		/// methods get the list of given
		/// <paramref name="type"/>
		/// of
		/// addresses
		/// </summary>
		/// <?/>
		/// <?/>
		/// <?/>
		/// <?/>
		/// <exception cref="System.Exception"/>
		private void GetAddressListFromTool(TestGetConf.TestType type, HdfsConfiguration 
			conf, bool checkPort, IList<DFSUtil.ConfiguredNNAddress> expected)
		{
			string @out = GetAddressListFromTool(type, conf, expected.Count != 0);
			IList<string> values = new AList<string>();
			// Convert list of addresses returned to an array of string
			StringTokenizer tokenizer = new StringTokenizer(@out);
			while (tokenizer.HasMoreTokens())
			{
				string s = tokenizer.NextToken().Trim();
				values.AddItem(s);
			}
			string[] actual = Sharpen.Collections.ToArray(values, new string[values.Count]);
			// Convert expected list to String[] of hosts
			int i = 0;
			string[] expectedHosts = new string[expected.Count];
			foreach (DFSUtil.ConfiguredNNAddress cnn in expected)
			{
				IPEndPoint addr = cnn.GetAddress();
				if (!checkPort)
				{
					expectedHosts[i++] = addr.GetHostName();
				}
				else
				{
					expectedHosts[i++] = addr.GetHostName() + ":" + addr.Port;
				}
			}
			// Compare two arrays
			NUnit.Framework.Assert.IsTrue(Arrays.Equals(expectedHosts, actual));
		}

		/// <exception cref="System.Exception"/>
		private void VerifyAddresses(HdfsConfiguration conf, TestGetConf.TestType type, bool
			 checkPort, params string[] expected)
		{
			// Ensure DFSUtil returned the right set of addresses
			IDictionary<string, IDictionary<string, IPEndPoint>> map = GetAddressListFromConf
				(type, conf);
			IList<DFSUtil.ConfiguredNNAddress> list = DFSUtil.FlattenAddressMap(map);
			string[] actual = ToStringArray(list);
			Arrays.Sort(actual);
			Arrays.Sort(expected);
			Assert.AssertArrayEquals(expected, actual);
			// Test GetConf returned addresses
			GetAddressListFromTool(type, conf, checkPort, list);
		}

		private static string GetNameServiceId(int index)
		{
			return "ns" + index;
		}

		/// <summary>Test empty configuration</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestEmptyConf()
		{
			HdfsConfiguration conf = new HdfsConfiguration(false);
			// Verify getting addresses fails
			GetAddressListFromTool(TestGetConf.TestType.Namenode, conf, false);
			System.Console.Out.WriteLine(GetAddressListFromTool(TestGetConf.TestType.Backup, 
				conf, false));
			GetAddressListFromTool(TestGetConf.TestType.Secondary, conf, false);
			GetAddressListFromTool(TestGetConf.TestType.Nnrpcaddresses, conf, false);
			foreach (GetConf.Command cmd in GetConf.Command.Values())
			{
				string arg = cmd.GetName();
				GetConf.CommandHandler handler = GetConf.Command.GetHandler(arg);
				NUnit.Framework.Assert.IsNotNull("missing handler: " + cmd, handler);
				if (handler.key != null)
				{
					// First test with configuration missing the required key
					string[] args = new string[] { handler.key };
					RunTool(conf, args, false);
				}
			}
		}

		/// <summary>Test invalid argument to the tool</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestInvalidArgument()
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			string[] args = new string[] { "-invalidArgument" };
			string ret = RunTool(conf, args, false);
			NUnit.Framework.Assert.IsTrue(ret.Contains(GetConf.Usage));
		}

		/// <summary>
		/// Tests to make sure the returned addresses are correct in case of default
		/// configuration with no federation
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestNonFederation()
		{
			HdfsConfiguration conf = new HdfsConfiguration(false);
			// Returned namenode address should match default address
			conf.Set(CommonConfigurationKeysPublic.FsDefaultNameKey, "hdfs://localhost:1000");
			VerifyAddresses(conf, TestGetConf.TestType.Namenode, false, "localhost:1000");
			VerifyAddresses(conf, TestGetConf.TestType.Nnrpcaddresses, true, "localhost:1000"
				);
			// Returned address should match backupnode RPC address
			conf.Set(DFSConfigKeys.DfsNamenodeBackupAddressKey, "localhost:1001");
			VerifyAddresses(conf, TestGetConf.TestType.Backup, false, "localhost:1001");
			// Returned address should match secondary http address
			conf.Set(DFSConfigKeys.DfsNamenodeSecondaryHttpAddressKey, "localhost:1002");
			VerifyAddresses(conf, TestGetConf.TestType.Secondary, false, "localhost:1002");
			// Returned namenode address should match service RPC address
			conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsNamenodeServiceRpcAddressKey, "localhost:1000");
			conf.Set(DFSConfigKeys.DfsNamenodeRpcAddressKey, "localhost:1001");
			VerifyAddresses(conf, TestGetConf.TestType.Namenode, false, "localhost:1000");
			VerifyAddresses(conf, TestGetConf.TestType.Nnrpcaddresses, true, "localhost:1000"
				);
			// Returned address should match RPC address
			conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsNamenodeRpcAddressKey, "localhost:1001");
			VerifyAddresses(conf, TestGetConf.TestType.Namenode, false, "localhost:1001");
			VerifyAddresses(conf, TestGetConf.TestType.Nnrpcaddresses, true, "localhost:1001"
				);
		}

		/// <summary>
		/// Tests to make sure the returned addresses are correct in case of federation
		/// of setup.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestFederation()
		{
			int nsCount = 10;
			HdfsConfiguration conf = new HdfsConfiguration(false);
			// Test to ensure namenode, backup and secondary namenode addresses are
			// returned from federation configuration. Returned namenode addresses are
			// based on service RPC address and not regular RPC address
			SetupNameServices(conf, nsCount);
			string[] nnAddresses = SetupAddress(conf, DFSConfigKeys.DfsNamenodeServiceRpcAddressKey
				, nsCount, 1000);
			SetupAddress(conf, DFSConfigKeys.DfsNamenodeRpcAddressKey, nsCount, 1500);
			SetupStaticHostResolution(nsCount);
			string[] backupAddresses = SetupAddress(conf, DFSConfigKeys.DfsNamenodeBackupAddressKey
				, nsCount, 2000);
			string[] secondaryAddresses = SetupAddress(conf, DFSConfigKeys.DfsNamenodeSecondaryHttpAddressKey
				, nsCount, 3000);
			VerifyAddresses(conf, TestGetConf.TestType.Namenode, false, nnAddresses);
			VerifyAddresses(conf, TestGetConf.TestType.Backup, false, backupAddresses);
			VerifyAddresses(conf, TestGetConf.TestType.Secondary, false, secondaryAddresses);
			VerifyAddresses(conf, TestGetConf.TestType.Nnrpcaddresses, true, nnAddresses);
			// Test to ensure namenode, backup, secondary namenode addresses and 
			// namenode rpc addresses are  returned from federation configuration. 
			// Returned namenode addresses are based on regular RPC address
			// in the absence of service RPC address.
			conf = new HdfsConfiguration(false);
			SetupNameServices(conf, nsCount);
			nnAddresses = SetupAddress(conf, DFSConfigKeys.DfsNamenodeRpcAddressKey, nsCount, 
				1000);
			backupAddresses = SetupAddress(conf, DFSConfigKeys.DfsNamenodeBackupAddressKey, nsCount
				, 2000);
			secondaryAddresses = SetupAddress(conf, DFSConfigKeys.DfsNamenodeSecondaryHttpAddressKey
				, nsCount, 3000);
			VerifyAddresses(conf, TestGetConf.TestType.Namenode, false, nnAddresses);
			VerifyAddresses(conf, TestGetConf.TestType.Backup, false, backupAddresses);
			VerifyAddresses(conf, TestGetConf.TestType.Secondary, false, secondaryAddresses);
			VerifyAddresses(conf, TestGetConf.TestType.Nnrpcaddresses, true, nnAddresses);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetSpecificKey()
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			conf.Set("mykey", " myval ");
			string[] args = new string[] { "-confKey", "mykey" };
			string toolResult = RunTool(conf, args, true);
			NUnit.Framework.Assert.AreEqual(string.Format("myval%n"), toolResult);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestExtraArgsThrowsError()
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			conf.Set("mykey", "myval");
			string[] args = new string[] { "-namenodes", "unexpected-arg" };
			NUnit.Framework.Assert.IsTrue(RunTool(conf, args, false).Contains("Did not expect argument: unexpected-arg"
				));
		}

		/// <summary>
		/// Tests commands other than
		/// <see cref="Command.Namenode"/>
		/// ,
		/// <see cref="Command.Backup"/>
		/// ,
		/// <see cref="Command.Secondary"/>
		/// and
		/// <see cref="Command.Nnrpcaddresses"/>
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestTool()
		{
			HdfsConfiguration conf = new HdfsConfiguration(false);
			foreach (GetConf.Command cmd in GetConf.Command.Values())
			{
				GetConf.CommandHandler handler = GetConf.Command.GetHandler(cmd.GetName());
				if (handler.key != null && !"-confKey".Equals(cmd.GetName()))
				{
					// Add the key to the conf and ensure tool returns the right value
					string[] args = new string[] { cmd.GetName() };
					conf.Set(handler.key, "value");
					NUnit.Framework.Assert.IsTrue(RunTool(conf, args, true).Contains("value"));
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetConfExcludeCommand()
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			// Set up the hosts/exclude files.
			localFileSys = FileSystem.GetLocal(conf);
			Path workingDir = localFileSys.GetWorkingDirectory();
			Path dir = new Path(workingDir, Runtime.GetProperty("test.build.data", "target/test/data"
				) + "/Getconf/");
			Path hostsFile = new Path(dir, "hosts");
			Path excludeFile = new Path(dir, "exclude");
			// Setup conf
			conf.Set(DFSConfigKeys.DfsHosts, hostsFile.ToUri().GetPath());
			conf.Set(DFSConfigKeys.DfsHostsExclude, excludeFile.ToUri().GetPath());
			WriteConfigFile(hostsFile, null);
			WriteConfigFile(excludeFile, null);
			string[] args = new string[] { "-excludeFile" };
			string ret = RunTool(conf, args, true);
			NUnit.Framework.Assert.AreEqual(excludeFile.ToUri().GetPath(), ret.Trim());
			CleanupFile(localFileSys, excludeFile.GetParent());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetConfIncludeCommand()
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			// Set up the hosts/exclude files.
			localFileSys = FileSystem.GetLocal(conf);
			Path workingDir = localFileSys.GetWorkingDirectory();
			Path dir = new Path(workingDir, Runtime.GetProperty("test.build.data", "target/test/data"
				) + "/Getconf/");
			Path hostsFile = new Path(dir, "hosts");
			Path excludeFile = new Path(dir, "exclude");
			// Setup conf
			conf.Set(DFSConfigKeys.DfsHosts, hostsFile.ToUri().GetPath());
			conf.Set(DFSConfigKeys.DfsHostsExclude, excludeFile.ToUri().GetPath());
			WriteConfigFile(hostsFile, null);
			WriteConfigFile(excludeFile, null);
			string[] args = new string[] { "-includeFile" };
			string ret = RunTool(conf, args, true);
			NUnit.Framework.Assert.AreEqual(hostsFile.ToUri().GetPath(), ret.Trim());
			CleanupFile(localFileSys, excludeFile.GetParent());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestIncludeInternalNameServices()
		{
			int nsCount = 10;
			int remoteNsCount = 4;
			HdfsConfiguration conf = new HdfsConfiguration();
			SetupNameServices(conf, nsCount);
			SetupAddress(conf, DFSConfigKeys.DfsNamenodeServiceRpcAddressKey, nsCount, 1000);
			SetupAddress(conf, DFSConfigKeys.DfsNamenodeRpcAddressKey, nsCount, 1500);
			conf.Set(DFSConfigKeys.DfsInternalNameservicesKey, "ns1");
			SetupStaticHostResolution(nsCount);
			string[] includedNN = new string[] { "nn1:1001" };
			VerifyAddresses(conf, TestGetConf.TestType.Namenode, false, includedNN);
			VerifyAddresses(conf, TestGetConf.TestType.Nnrpcaddresses, true, includedNN);
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteConfigFile(Path name, AList<string> nodes)
		{
			// delete if it already exists
			if (localFileSys.Exists(name))
			{
				localFileSys.Delete(name, true);
			}
			FSDataOutputStream stm = localFileSys.Create(name);
			if (nodes != null)
			{
				for (IEnumerator<string> it = nodes.GetEnumerator(); it.HasNext(); )
				{
					string node = it.Next();
					stm.WriteBytes(node);
					stm.WriteBytes("\n");
				}
			}
			stm.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void CleanupFile(FileSystem fileSys, Path name)
		{
			NUnit.Framework.Assert.IsTrue(fileSys.Exists(name));
			fileSys.Delete(name, true);
			NUnit.Framework.Assert.IsTrue(!fileSys.Exists(name));
		}
	}
}
