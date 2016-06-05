using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.HA;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Alias;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Hamcrest;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestDFSUtil
	{
		/// <summary>Reset to default UGI settings since some tests change them.</summary>
		[SetUp]
		public virtual void ResetUGI()
		{
			UserGroupInformation.SetConfiguration(new Configuration());
		}

		/// <summary>Test conversion of LocatedBlock to BlockLocation</summary>
		[NUnit.Framework.Test]
		public virtual void TestLocatedBlocks2Locations()
		{
			DatanodeInfo d = DFSTestUtil.GetLocalDatanodeInfo();
			DatanodeInfo[] ds = new DatanodeInfo[1];
			ds[0] = d;
			// ok
			ExtendedBlock b1 = new ExtendedBlock("bpid", 1, 1, 1);
			LocatedBlock l1 = new LocatedBlock(b1, ds, 0, false);
			// corrupt
			ExtendedBlock b2 = new ExtendedBlock("bpid", 2, 1, 1);
			LocatedBlock l2 = new LocatedBlock(b2, ds, 0, true);
			IList<LocatedBlock> ls = Arrays.AsList(l1, l2);
			LocatedBlocks lbs = new LocatedBlocks(10, false, ls, l2, true, null);
			BlockLocation[] bs = DFSUtil.LocatedBlocks2Locations(lbs);
			NUnit.Framework.Assert.IsTrue("expected 2 blocks but got " + bs.Length, bs.Length
				 == 2);
			int corruptCount = 0;
			foreach (BlockLocation b in bs)
			{
				if (b.IsCorrupt())
				{
					corruptCount++;
				}
			}
			NUnit.Framework.Assert.IsTrue("expected 1 corrupt files but got " + corruptCount, 
				corruptCount == 1);
			// test an empty location
			bs = DFSUtil.LocatedBlocks2Locations(new LocatedBlocks());
			NUnit.Framework.Assert.AreEqual(0, bs.Length);
		}

		/// <summary>Test constructing LocatedBlock with null cachedLocs</summary>
		[NUnit.Framework.Test]
		public virtual void TestLocatedBlockConstructorWithNullCachedLocs()
		{
			DatanodeInfo d = DFSTestUtil.GetLocalDatanodeInfo();
			DatanodeInfo[] ds = new DatanodeInfo[1];
			ds[0] = d;
			ExtendedBlock b1 = new ExtendedBlock("bpid", 1, 1, 1);
			LocatedBlock l1 = new LocatedBlock(b1, ds, null, null, 0, false, null);
			DatanodeInfo[] cachedLocs = l1.GetCachedLocations();
			NUnit.Framework.Assert.IsTrue(cachedLocs.Length == 0);
		}

		private Configuration SetupAddress(string key)
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsNameservices, "nn1");
			conf.Set(DFSUtil.AddKeySuffixes(key, "nn1"), "localhost:9000");
			return conf;
		}

		/// <summary>
		/// Test
		/// <see cref="DFSUtil.GetNamenodeNameServiceId(Org.Apache.Hadoop.Conf.Configuration)
		/// 	"/>
		/// to ensure
		/// nameserviceId from the configuration returned
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void GetNameServiceId()
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsNameserviceId, "nn1");
			NUnit.Framework.Assert.AreEqual("nn1", DFSUtil.GetNamenodeNameServiceId(conf));
		}

		/// <summary>
		/// Test
		/// <see cref="DFSUtil.GetNamenodeNameServiceId(Org.Apache.Hadoop.Conf.Configuration)
		/// 	"/>
		/// to ensure
		/// nameserviceId for namenode is determined based on matching the address with
		/// local node's address
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void GetNameNodeNameServiceId()
		{
			Configuration conf = SetupAddress(DFSConfigKeys.DfsNamenodeRpcAddressKey);
			NUnit.Framework.Assert.AreEqual("nn1", DFSUtil.GetNamenodeNameServiceId(conf));
		}

		/// <summary>
		/// Test
		/// <see cref="DFSUtil.GetBackupNameServiceId(Org.Apache.Hadoop.Conf.Configuration)"/
		/// 	>
		/// to ensure
		/// nameserviceId for backup node is determined based on matching the address
		/// with local node's address
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void GetBackupNameServiceId()
		{
			Configuration conf = SetupAddress(DFSConfigKeys.DfsNamenodeBackupAddressKey);
			NUnit.Framework.Assert.AreEqual("nn1", DFSUtil.GetBackupNameServiceId(conf));
		}

		/// <summary>
		/// Test
		/// <see cref="DFSUtil.GetSecondaryNameServiceId(Org.Apache.Hadoop.Conf.Configuration)
		/// 	"/>
		/// to ensure
		/// nameserviceId for backup node is determined based on matching the address
		/// with local node's address
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void GetSecondaryNameServiceId()
		{
			Configuration conf = SetupAddress(DFSConfigKeys.DfsNamenodeSecondaryHttpAddressKey
				);
			NUnit.Framework.Assert.AreEqual("nn1", DFSUtil.GetSecondaryNameServiceId(conf));
		}

		/// <summary>
		/// Test
		/// <see cref="DFSUtil.GetNamenodeNameServiceId(Org.Apache.Hadoop.Conf.Configuration)
		/// 	"/>
		/// to ensure
		/// exception is thrown when multiple rpc addresses match the local node's
		/// address
		/// </summary>
		public virtual void TestGetNameServiceIdException()
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsNameservices, "nn1,nn2");
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeRpcAddressKey, "nn1"), "localhost:9000"
				);
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeRpcAddressKey, "nn2"), "localhost:9001"
				);
			DFSUtil.GetNamenodeNameServiceId(conf);
			NUnit.Framework.Assert.Fail("Expected exception is not thrown");
		}

		/// <summary>
		/// Test
		/// <see cref="DFSUtil.GetNameServiceIds(Org.Apache.Hadoop.Conf.Configuration)"/>
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestGetNameServiceIds()
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsNameservices, "nn1,nn2");
			ICollection<string> nameserviceIds = DFSUtil.GetNameServiceIds(conf);
			IEnumerator<string> it = nameserviceIds.GetEnumerator();
			NUnit.Framework.Assert.AreEqual(2, nameserviceIds.Count);
			NUnit.Framework.Assert.AreEqual("nn1", it.Next().ToString());
			NUnit.Framework.Assert.AreEqual("nn2", it.Next().ToString());
		}

		[NUnit.Framework.Test]
		public virtual void TestGetOnlyNameServiceIdOrNull()
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsNameservices, "ns1,ns2");
			NUnit.Framework.Assert.IsNull(DFSUtil.GetOnlyNameServiceIdOrNull(conf));
			conf.Set(DFSConfigKeys.DfsNameservices, string.Empty);
			NUnit.Framework.Assert.IsNull(DFSUtil.GetOnlyNameServiceIdOrNull(conf));
			conf.Set(DFSConfigKeys.DfsNameservices, "ns1");
			NUnit.Framework.Assert.AreEqual("ns1", DFSUtil.GetOnlyNameServiceIdOrNull(conf));
		}

		/// <summary>
		/// Test for
		/// <see cref="DFSUtil.GetNNServiceRpcAddresses(Org.Apache.Hadoop.Conf.Configuration)
		/// 	"/>
		/// <see cref="DFSUtil.GetNameServiceIdFromAddress(Org.Apache.Hadoop.Conf.Configuration, System.Net.IPEndPoint, string[])
		/// 	">(Configuration)</see>
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleNamenodes()
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsNameservices, "nn1,nn2");
			// Test - configured list of namenodes are returned
			string Nn1Address = "localhost:9000";
			string Nn2Address = "localhost:9001";
			string Nn3Address = "localhost:9002";
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeRpcAddressKey, "nn1"), Nn1Address
				);
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeRpcAddressKey, "nn2"), Nn2Address
				);
			IDictionary<string, IDictionary<string, IPEndPoint>> nnMap = DFSUtil.GetNNServiceRpcAddresses
				(conf);
			NUnit.Framework.Assert.AreEqual(2, nnMap.Count);
			IDictionary<string, IPEndPoint> nn1Map = nnMap["nn1"];
			NUnit.Framework.Assert.AreEqual(1, nn1Map.Count);
			IPEndPoint addr = nn1Map[null];
			NUnit.Framework.Assert.AreEqual("localhost", addr.GetHostName());
			NUnit.Framework.Assert.AreEqual(9000, addr.Port);
			IDictionary<string, IPEndPoint> nn2Map = nnMap["nn2"];
			NUnit.Framework.Assert.AreEqual(1, nn2Map.Count);
			addr = nn2Map[null];
			NUnit.Framework.Assert.AreEqual("localhost", addr.GetHostName());
			NUnit.Framework.Assert.AreEqual(9001, addr.Port);
			// Test - can look up nameservice ID from service address
			CheckNameServiceId(conf, Nn1Address, "nn1");
			CheckNameServiceId(conf, Nn2Address, "nn2");
			CheckNameServiceId(conf, Nn3Address, null);
			// HA is not enabled in a purely federated config
			NUnit.Framework.Assert.IsFalse(HAUtil.IsHAEnabled(conf, "nn1"));
			NUnit.Framework.Assert.IsFalse(HAUtil.IsHAEnabled(conf, "nn2"));
		}

		public virtual void CheckNameServiceId(Configuration conf, string addr, string expectedNameServiceId
			)
		{
			IPEndPoint s = NetUtils.CreateSocketAddr(addr);
			string nameserviceId = DFSUtil.GetNameServiceIdFromAddress(conf, s, DFSConfigKeys
				.DfsNamenodeServiceRpcAddressKey, DFSConfigKeys.DfsNamenodeRpcAddressKey);
			NUnit.Framework.Assert.AreEqual(expectedNameServiceId, nameserviceId);
		}

		/// <summary>Tests to ensure default namenode is used as fallback</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDefaultNamenode()
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			string hdfs_default = "hdfs://localhost:9999/";
			conf.Set(CommonConfigurationKeysPublic.FsDefaultNameKey, hdfs_default);
			// If DFS_FEDERATION_NAMESERVICES is not set, verify that
			// default namenode address is returned.
			IDictionary<string, IDictionary<string, IPEndPoint>> addrMap = DFSUtil.GetNNServiceRpcAddresses
				(conf);
			NUnit.Framework.Assert.AreEqual(1, addrMap.Count);
			IDictionary<string, IPEndPoint> defaultNsMap = addrMap[null];
			NUnit.Framework.Assert.AreEqual(1, defaultNsMap.Count);
			NUnit.Framework.Assert.AreEqual(9999, defaultNsMap[null].Port);
		}

		/// <summary>
		/// Test to ensure nameservice specific keys in the configuration are
		/// copied to generic keys when the namenode starts.
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestConfModificationFederationOnly()
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			string nsId = "ns1";
			conf.Set(DFSConfigKeys.DfsNameservices, nsId);
			conf.Set(DFSConfigKeys.DfsNameserviceId, nsId);
			// Set the nameservice specific keys with nameserviceId in the config key
			foreach (string key in NameNode.NamenodeSpecificKeys)
			{
				// Note: value is same as the key
				conf.Set(DFSUtil.AddKeySuffixes(key, nsId), key);
			}
			// Initialize generic keys from specific keys
			NameNode.InitializeGenericKeys(conf, nsId, null);
			// Retrieve the keys without nameserviceId and Ensure generic keys are set
			// to the correct value
			foreach (string key_1 in NameNode.NamenodeSpecificKeys)
			{
				NUnit.Framework.Assert.AreEqual(key_1, conf.Get(key_1));
			}
		}

		/// <summary>
		/// Test to ensure nameservice specific keys in the configuration are
		/// copied to generic keys when the namenode starts.
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestConfModificationFederationAndHa()
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			string nsId = "ns1";
			string nnId = "nn1";
			conf.Set(DFSConfigKeys.DfsNameservices, nsId);
			conf.Set(DFSConfigKeys.DfsNameserviceId, nsId);
			conf.Set(DFSConfigKeys.DfsHaNamenodesKeyPrefix + "." + nsId, nnId);
			// Set the nameservice specific keys with nameserviceId in the config key
			foreach (string key in NameNode.NamenodeSpecificKeys)
			{
				// Note: value is same as the key
				conf.Set(DFSUtil.AddKeySuffixes(key, nsId, nnId), key);
			}
			// Initialize generic keys from specific keys
			NameNode.InitializeGenericKeys(conf, nsId, nnId);
			// Retrieve the keys without nameserviceId and Ensure generic keys are set
			// to the correct value
			foreach (string key_1 in NameNode.NamenodeSpecificKeys)
			{
				NUnit.Framework.Assert.AreEqual(key_1, conf.Get(key_1));
			}
		}

		/// <summary>
		/// Ensure that fs.defaultFS is set in the configuration even if neither HA nor
		/// Federation is enabled.
		/// </summary>
		/// <remarks>
		/// Ensure that fs.defaultFS is set in the configuration even if neither HA nor
		/// Federation is enabled.
		/// Regression test for HDFS-3351.
		/// </remarks>
		[NUnit.Framework.Test]
		public virtual void TestConfModificationNoFederationOrHa()
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			string nsId = null;
			string nnId = null;
			conf.Set(DFSConfigKeys.DfsNamenodeRpcAddressKey, "localhost:1234");
			NUnit.Framework.Assert.IsFalse("hdfs://localhost:1234".Equals(conf.Get(CommonConfigurationKeysPublic
				.FsDefaultNameKey)));
			NameNode.InitializeGenericKeys(conf, nsId, nnId);
			NUnit.Framework.Assert.AreEqual("hdfs://localhost:1234", conf.Get(CommonConfigurationKeysPublic
				.FsDefaultNameKey));
		}

		/// <summary>Regression test for HDFS-2934.</summary>
		[NUnit.Framework.Test]
		public virtual void TestSomeConfsNNSpecificSomeNSSpecific()
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			string key = DFSConfigKeys.DfsNamenodeSharedEditsDirKey;
			conf.Set(key, "global-default");
			conf.Set(key + ".ns1", "ns1-override");
			conf.Set(key + ".ns1.nn1", "nn1-override");
			// A namenode in another nameservice should get the global default.
			Configuration newConf = new Configuration(conf);
			NameNode.InitializeGenericKeys(newConf, "ns2", "nn1");
			NUnit.Framework.Assert.AreEqual("global-default", newConf.Get(key));
			// A namenode in another non-HA nameservice should get global default.
			newConf = new Configuration(conf);
			NameNode.InitializeGenericKeys(newConf, "ns2", null);
			NUnit.Framework.Assert.AreEqual("global-default", newConf.Get(key));
			// A namenode in the same nameservice should get the ns setting
			newConf = new Configuration(conf);
			NameNode.InitializeGenericKeys(newConf, "ns1", "nn2");
			NUnit.Framework.Assert.AreEqual("ns1-override", newConf.Get(key));
			// The nn with the nn-specific setting should get its own override
			newConf = new Configuration(conf);
			NameNode.InitializeGenericKeys(newConf, "ns1", "nn1");
			NUnit.Framework.Assert.AreEqual("nn1-override", newConf.Get(key));
		}

		/// <summary>
		/// Tests for empty configuration, an exception is thrown from
		/// <see cref="DFSUtil.GetNNServiceRpcAddresses(Org.Apache.Hadoop.Conf.Configuration)
		/// 	"/>
		/// <see cref="DFSUtil.GetBackupNodeAddresses(Org.Apache.Hadoop.Conf.Configuration)"/
		/// 	>
		/// <see cref="DFSUtil.GetSecondaryNameNodeAddresses(Org.Apache.Hadoop.Conf.Configuration)
		/// 	"/>
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestEmptyConf()
		{
			HdfsConfiguration conf = new HdfsConfiguration(false);
			try
			{
				IDictionary<string, IDictionary<string, IPEndPoint>> map = DFSUtil.GetNNServiceRpcAddresses
					(conf);
				NUnit.Framework.Assert.Fail("Expected IOException is not thrown, result was: " + 
					DFSUtil.AddressMapToString(map));
			}
			catch (IOException)
			{
			}
			try
			{
				IDictionary<string, IDictionary<string, IPEndPoint>> map = DFSUtil.GetBackupNodeAddresses
					(conf);
				NUnit.Framework.Assert.Fail("Expected IOException is not thrown, result was: " + 
					DFSUtil.AddressMapToString(map));
			}
			catch (IOException)
			{
			}
			try
			{
				IDictionary<string, IDictionary<string, IPEndPoint>> map = DFSUtil.GetSecondaryNameNodeAddresses
					(conf);
				NUnit.Framework.Assert.Fail("Expected IOException is not thrown, result was: " + 
					DFSUtil.AddressMapToString(map));
			}
			catch (IOException)
			{
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetInfoServer()
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			URI httpsport = DFSUtil.GetInfoServer(null, conf, "https");
			NUnit.Framework.Assert.AreEqual(new URI("https", null, "0.0.0.0", DFSConfigKeys.DfsNamenodeHttpsPortDefault
				, null, null, null), httpsport);
			URI httpport = DFSUtil.GetInfoServer(null, conf, "http");
			NUnit.Framework.Assert.AreEqual(new URI("http", null, "0.0.0.0", DFSConfigKeys.DfsNamenodeHttpPortDefault
				, null, null, null), httpport);
			URI httpAddress = DFSUtil.GetInfoServer(new IPEndPoint("localhost", 8020), conf, 
				"http");
			NUnit.Framework.Assert.AreEqual(URI.Create("http://localhost:" + DFSConfigKeys.DfsNamenodeHttpPortDefault
				), httpAddress);
		}

		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestHANameNodesWithFederation()
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			string Ns1Nn1Host = "ns1-nn1.example.com:8020";
			string Ns1Nn2Host = "ns1-nn2.example.com:8020";
			string Ns2Nn1Host = "ns2-nn1.example.com:8020";
			string Ns2Nn2Host = "ns2-nn2.example.com:8020";
			conf.Set(CommonConfigurationKeys.FsDefaultNameKey, "hdfs://ns1");
			// Two nameservices, each with two NNs.
			conf.Set(DFSConfigKeys.DfsNameservices, "ns1,ns2");
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsHaNamenodesKeyPrefix, "ns1"), "ns1-nn1,ns1-nn2"
				);
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsHaNamenodesKeyPrefix, "ns2"), "ns2-nn1,ns2-nn2"
				);
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeRpcAddressKey, "ns1", "ns1-nn1"
				), Ns1Nn1Host);
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeRpcAddressKey, "ns1", "ns1-nn2"
				), Ns1Nn2Host);
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeRpcAddressKey, "ns2", "ns2-nn1"
				), Ns2Nn1Host);
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeRpcAddressKey, "ns2", "ns2-nn2"
				), Ns2Nn2Host);
			IDictionary<string, IDictionary<string, IPEndPoint>> map = DFSUtil.GetHaNnRpcAddresses
				(conf);
			NUnit.Framework.Assert.IsTrue(HAUtil.IsHAEnabled(conf, "ns1"));
			NUnit.Framework.Assert.IsTrue(HAUtil.IsHAEnabled(conf, "ns2"));
			NUnit.Framework.Assert.IsFalse(HAUtil.IsHAEnabled(conf, "ns3"));
			NUnit.Framework.Assert.AreEqual(Ns1Nn1Host, map["ns1"]["ns1-nn1"].ToString());
			NUnit.Framework.Assert.AreEqual(Ns1Nn2Host, map["ns1"]["ns1-nn2"].ToString());
			NUnit.Framework.Assert.AreEqual(Ns2Nn1Host, map["ns2"]["ns2-nn1"].ToString());
			NUnit.Framework.Assert.AreEqual(Ns2Nn2Host, map["ns2"]["ns2-nn2"].ToString());
			NUnit.Framework.Assert.AreEqual(Ns1Nn1Host, DFSUtil.GetNamenodeServiceAddr(conf, 
				"ns1", "ns1-nn1"));
			NUnit.Framework.Assert.AreEqual(Ns1Nn2Host, DFSUtil.GetNamenodeServiceAddr(conf, 
				"ns1", "ns1-nn2"));
			NUnit.Framework.Assert.AreEqual(Ns2Nn1Host, DFSUtil.GetNamenodeServiceAddr(conf, 
				"ns2", "ns2-nn1"));
			// No nameservice was given and we can't determine which service addr
			// to use as two nameservices could share a namenode ID.
			NUnit.Framework.Assert.AreEqual(null, DFSUtil.GetNamenodeServiceAddr(conf, null, 
				"ns1-nn1"));
			// Ditto for nameservice IDs, if multiple are defined
			NUnit.Framework.Assert.AreEqual(null, DFSUtil.GetNamenodeNameServiceId(conf));
			NUnit.Framework.Assert.AreEqual(null, DFSUtil.GetSecondaryNameServiceId(conf));
			ICollection<URI> uris = DFSUtil.GetNameServiceUris(conf, DFSConfigKeys.DfsNamenodeRpcAddressKey
				);
			NUnit.Framework.Assert.AreEqual(2, uris.Count);
			NUnit.Framework.Assert.IsTrue(uris.Contains(new URI("hdfs://ns1")));
			NUnit.Framework.Assert.IsTrue(uris.Contains(new URI("hdfs://ns2")));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void GetNameNodeServiceAddr()
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			// One nameservice with two NNs
			string Ns1Nn1Host = "ns1-nn1.example.com:8020";
			string Ns1Nn1HostSvc = "ns1-nn2.example.com:8021";
			string Ns1Nn2Host = "ns1-nn1.example.com:8020";
			string Ns1Nn2HostSvc = "ns1-nn2.example.com:8021";
			conf.Set(DFSConfigKeys.DfsNameservices, "ns1");
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsHaNamenodesKeyPrefix, "ns1"), "nn1,nn2"
				);
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeRpcAddressKey, "ns1", "nn1"
				), Ns1Nn1Host);
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeRpcAddressKey, "ns1", "nn2"
				), Ns1Nn2Host);
			// The rpc address is used if no service address is defined
			NUnit.Framework.Assert.AreEqual(Ns1Nn1Host, DFSUtil.GetNamenodeServiceAddr(conf, 
				null, "nn1"));
			NUnit.Framework.Assert.AreEqual(Ns1Nn2Host, DFSUtil.GetNamenodeServiceAddr(conf, 
				null, "nn2"));
			// A nameservice is specified explicitly
			NUnit.Framework.Assert.AreEqual(Ns1Nn1Host, DFSUtil.GetNamenodeServiceAddr(conf, 
				"ns1", "nn1"));
			NUnit.Framework.Assert.AreEqual(null, DFSUtil.GetNamenodeServiceAddr(conf, "invalid"
				, "nn1"));
			// The service addrs are used when they are defined
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeServiceRpcAddressKey, "ns1"
				, "nn1"), Ns1Nn1HostSvc);
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeServiceRpcAddressKey, "ns1"
				, "nn2"), Ns1Nn2HostSvc);
			NUnit.Framework.Assert.AreEqual(Ns1Nn1HostSvc, DFSUtil.GetNamenodeServiceAddr(conf
				, null, "nn1"));
			NUnit.Framework.Assert.AreEqual(Ns1Nn2HostSvc, DFSUtil.GetNamenodeServiceAddr(conf
				, null, "nn2"));
			// We can determine the nameservice ID, there's only one listed
			NUnit.Framework.Assert.AreEqual("ns1", DFSUtil.GetNamenodeNameServiceId(conf));
			NUnit.Framework.Assert.AreEqual("ns1", DFSUtil.GetSecondaryNameServiceId(conf));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetHaNnHttpAddresses()
		{
			string LogicalHostName = "ns1";
			string Ns1Nn1Addr = "ns1-nn1.example.com:8020";
			string Ns1Nn2Addr = "ns1-nn2.example.com:8020";
			Configuration conf = CreateWebHDFSHAConfiguration(LogicalHostName, Ns1Nn1Addr, Ns1Nn2Addr
				);
			IDictionary<string, IDictionary<string, IPEndPoint>> map = DFSUtil.GetHaNnWebHdfsAddresses
				(conf, "webhdfs");
			NUnit.Framework.Assert.AreEqual(Ns1Nn1Addr, map["ns1"]["nn1"].ToString());
			NUnit.Framework.Assert.AreEqual(Ns1Nn2Addr, map["ns1"]["nn2"].ToString());
		}

		private static Configuration CreateWebHDFSHAConfiguration(string logicalHostName, 
			string nnaddr1, string nnaddr2)
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsNameservices, "ns1");
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsHaNamenodesKeyPrefix, "ns1"), "nn1,nn2"
				);
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeHttpAddressKey, "ns1", "nn1"
				), nnaddr1);
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeHttpAddressKey, "ns1", "nn2"
				), nnaddr2);
			conf.Set(DFSConfigKeys.DfsClientFailoverProxyProviderKeyPrefix + "." + logicalHostName
				, typeof(ConfiguredFailoverProxyProvider).FullName);
			return conf;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSubstituteForWildcardAddress()
		{
			NUnit.Framework.Assert.AreEqual("foo:12345", DFSUtil.SubstituteForWildcardAddress
				("0.0.0.0:12345", "foo"));
			NUnit.Framework.Assert.AreEqual("127.0.0.1:12345", DFSUtil.SubstituteForWildcardAddress
				("127.0.0.1:12345", "foo"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetNNUris()
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			string Ns1Nn1Addr = "ns1-nn1.example.com:8020";
			string Ns1Nn2Addr = "ns1-nn2.example.com:8020";
			string Ns2NnAddr = "ns2-nn.example.com:8020";
			string Nn1Addr = "nn.example.com:8020";
			string Nn1SrvcAddr = "nn.example.com:8021";
			string Nn2Addr = "nn2.example.com:8020";
			conf.Set(DFSConfigKeys.DfsNameservices, "ns1,ns2");
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsHaNamenodesKeyPrefix, "ns1"), "nn1,nn2"
				);
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeRpcAddressKey, "ns1", "nn1"
				), Ns1Nn1Addr);
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeRpcAddressKey, "ns1", "nn2"
				), Ns1Nn2Addr);
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeServiceRpcAddressKey, "ns2"
				), Ns2NnAddr);
			conf.Set(DFSConfigKeys.DfsNamenodeRpcAddressKey, "hdfs://" + Nn1Addr);
			conf.Set(CommonConfigurationKeys.FsDefaultNameKey, "hdfs://" + Nn2Addr);
			ICollection<URI> uris = DFSUtil.GetNameServiceUris(conf, DFSConfigKeys.DfsNamenodeServiceRpcAddressKey
				, DFSConfigKeys.DfsNamenodeRpcAddressKey);
			NUnit.Framework.Assert.AreEqual(4, uris.Count);
			NUnit.Framework.Assert.IsTrue(uris.Contains(new URI("hdfs://ns1")));
			NUnit.Framework.Assert.IsTrue(uris.Contains(new URI("hdfs://" + Ns2NnAddr)));
			NUnit.Framework.Assert.IsTrue(uris.Contains(new URI("hdfs://" + Nn1Addr)));
			NUnit.Framework.Assert.IsTrue(uris.Contains(new URI("hdfs://" + Nn2Addr)));
			// Make sure that non-HDFS URIs in fs.defaultFS don't get included.
			conf.Set(CommonConfigurationKeys.FsDefaultNameKey, "viewfs://vfs-name.example.com"
				);
			uris = DFSUtil.GetNameServiceUris(conf, DFSConfigKeys.DfsNamenodeServiceRpcAddressKey
				, DFSConfigKeys.DfsNamenodeRpcAddressKey);
			NUnit.Framework.Assert.AreEqual(3, uris.Count);
			NUnit.Framework.Assert.IsTrue(uris.Contains(new URI("hdfs://ns1")));
			NUnit.Framework.Assert.IsTrue(uris.Contains(new URI("hdfs://" + Ns2NnAddr)));
			NUnit.Framework.Assert.IsTrue(uris.Contains(new URI("hdfs://" + Nn1Addr)));
			// Make sure that an HA URI being the default URI doesn't result in multiple
			// entries being returned.
			conf.Set(CommonConfigurationKeys.FsDefaultNameKey, "hdfs://ns1");
			uris = DFSUtil.GetNameServiceUris(conf, DFSConfigKeys.DfsNamenodeServiceRpcAddressKey
				, DFSConfigKeys.DfsNamenodeRpcAddressKey);
			NUnit.Framework.Assert.AreEqual(3, uris.Count);
			NUnit.Framework.Assert.IsTrue(uris.Contains(new URI("hdfs://ns1")));
			NUnit.Framework.Assert.IsTrue(uris.Contains(new URI("hdfs://" + Ns2NnAddr)));
			NUnit.Framework.Assert.IsTrue(uris.Contains(new URI("hdfs://" + Nn1Addr)));
			// Make sure that when a service RPC address is used that is distinct from
			// the client RPC address, and that client RPC address is also used as the
			// default URI, that the client URI does not end up in the set of URIs
			// returned.
			conf = new HdfsConfiguration();
			conf.Set(CommonConfigurationKeys.FsDefaultNameKey, "hdfs://" + Nn1Addr);
			conf.Set(DFSConfigKeys.DfsNamenodeRpcAddressKey, Nn1Addr);
			conf.Set(DFSConfigKeys.DfsNamenodeServiceRpcAddressKey, Nn1SrvcAddr);
			uris = DFSUtil.GetNameServiceUris(conf, DFSConfigKeys.DfsNamenodeServiceRpcAddressKey
				, DFSConfigKeys.DfsNamenodeRpcAddressKey);
			NUnit.Framework.Assert.AreEqual(1, uris.Count);
			NUnit.Framework.Assert.IsTrue(uris.Contains(new URI("hdfs://" + Nn1SrvcAddr)));
		}

		public virtual void TestLocalhostReverseLookup()
		{
			// 127.0.0.1 -> localhost reverse resolution does not happen on Windows.
			Assume.AssumeTrue(!Shell.Windows);
			// Make sure when config FS_DEFAULT_NAME_KEY using IP address,
			// it will automatically convert it to hostname
			HdfsConfiguration conf = new HdfsConfiguration();
			conf.Set(CommonConfigurationKeys.FsDefaultNameKey, "hdfs://127.0.0.1:8020");
			ICollection<URI> uris = DFSUtil.GetNameServiceUris(conf);
			NUnit.Framework.Assert.AreEqual(1, uris.Count);
			foreach (URI uri in uris)
			{
				Assert.AssertThat(uri.GetHost(), CoreMatchers.Not("127.0.0.1"));
			}
		}

		public virtual void TestIsValidName()
		{
			NUnit.Framework.Assert.IsFalse(DFSUtil.IsValidName("/foo/../bar"));
			NUnit.Framework.Assert.IsFalse(DFSUtil.IsValidName("/foo/./bar"));
			NUnit.Framework.Assert.IsFalse(DFSUtil.IsValidName("/foo//bar"));
			NUnit.Framework.Assert.IsTrue(DFSUtil.IsValidName("/"));
			NUnit.Framework.Assert.IsTrue(DFSUtil.IsValidName("/bar/"));
			NUnit.Framework.Assert.IsFalse(DFSUtil.IsValidName("/foo/:/bar"));
			NUnit.Framework.Assert.IsFalse(DFSUtil.IsValidName("/foo:bar"));
		}

		public virtual void TestGetSpnegoKeytabKey()
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			string defaultKey = "default.spengo.key";
			conf.Unset(DFSConfigKeys.DfsWebAuthenticationKerberosKeytabKey);
			NUnit.Framework.Assert.AreEqual("Test spnego key in config is null", defaultKey, 
				DFSUtil.GetSpnegoKeytabKey(conf, defaultKey));
			conf.Set(DFSConfigKeys.DfsWebAuthenticationKerberosKeytabKey, string.Empty);
			NUnit.Framework.Assert.AreEqual("Test spnego key is empty", defaultKey, DFSUtil.GetSpnegoKeytabKey
				(conf, defaultKey));
			string spengoKey = "spengo.key";
			conf.Set(DFSConfigKeys.DfsWebAuthenticationKerberosKeytabKey, spengoKey);
			NUnit.Framework.Assert.AreEqual("Test spnego key is NOT null", DFSConfigKeys.DfsWebAuthenticationKerberosKeytabKey
				, DFSUtil.GetSpnegoKeytabKey(conf, defaultKey));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDurationToString()
		{
			NUnit.Framework.Assert.AreEqual("000:00:00:00.000", DFSUtil.DurationToString(0));
			NUnit.Framework.Assert.AreEqual("001:01:01:01.000", DFSUtil.DurationToString(((24
				 * 60 * 60) + (60 * 60) + (60) + 1) * 1000));
			NUnit.Framework.Assert.AreEqual("000:23:59:59.999", DFSUtil.DurationToString(((23
				 * 60 * 60) + (59 * 60) + (59)) * 1000 + 999));
			NUnit.Framework.Assert.AreEqual("-001:01:01:01.000", DFSUtil.DurationToString(-((
				24 * 60 * 60) + (60 * 60) + (60) + 1) * 1000));
			NUnit.Framework.Assert.AreEqual("-000:23:59:59.574", DFSUtil.DurationToString(-((
				(23 * 60 * 60) + (59 * 60) + (59)) * 1000 + 574)));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRelativeTimeConversion()
		{
			try
			{
				DFSUtil.ParseRelativeTime("1");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("too short", e);
			}
			try
			{
				DFSUtil.ParseRelativeTime("1z");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("unknown time unit", e);
			}
			try
			{
				DFSUtil.ParseRelativeTime("yyz");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("is not a number", e);
			}
			NUnit.Framework.Assert.AreEqual(61 * 1000, DFSUtil.ParseRelativeTime("61s"));
			NUnit.Framework.Assert.AreEqual(61 * 60 * 1000, DFSUtil.ParseRelativeTime("61m"));
			NUnit.Framework.Assert.AreEqual(0, DFSUtil.ParseRelativeTime("0s"));
			NUnit.Framework.Assert.AreEqual(25 * 60 * 60 * 1000, DFSUtil.ParseRelativeTime("25h"
				));
			NUnit.Framework.Assert.AreEqual(4 * 24 * 60 * 60 * 1000l, DFSUtil.ParseRelativeTime
				("4d"));
			NUnit.Framework.Assert.AreEqual(999 * 24 * 60 * 60 * 1000l, DFSUtil.ParseRelativeTime
				("999d"));
		}

		[NUnit.Framework.Test]
		public virtual void TestAssertAllResultsEqual()
		{
			CheckAllResults(new long[] {  }, true);
			CheckAllResults(new long[] { 1l }, true);
			CheckAllResults(new long[] { 1l, 1l }, true);
			CheckAllResults(new long[] { 1l, 1l, 1l }, true);
			CheckAllResults(new long[] { System.Convert.ToInt64(1), System.Convert.ToInt64(1)
				 }, true);
			CheckAllResults(new long[] { null, null, null }, true);
			CheckAllResults(new long[] { 1l, 2l }, false);
			CheckAllResults(new long[] { 2l, 1l }, false);
			CheckAllResults(new long[] { 1l, 2l, 1l }, false);
			CheckAllResults(new long[] { 2l, 1l, 1l }, false);
			CheckAllResults(new long[] { 1l, 1l, 2l }, false);
			CheckAllResults(new long[] { 1l, null }, false);
			CheckAllResults(new long[] { null, 1l }, false);
			CheckAllResults(new long[] { 1l, null, 1l }, false);
		}

		private static void CheckAllResults(long[] toCheck, bool shouldSucceed)
		{
			if (shouldSucceed)
			{
				DFSUtil.AssertAllResultsEqual(Arrays.AsList(toCheck));
			}
			else
			{
				try
				{
					DFSUtil.AssertAllResultsEqual(Arrays.AsList(toCheck));
					NUnit.Framework.Assert.Fail("Should not have succeeded with input: " + Arrays.ToString
						(toCheck));
				}
				catch (Exception ae)
				{
					GenericTestUtils.AssertExceptionContains("Not all elements match", ae);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetPassword()
		{
			FilePath testDir = new FilePath(Runtime.GetProperty("test.build.data", "target/test-dir"
				));
			Configuration conf = new Configuration();
			Path jksPath = new Path(testDir.ToString(), "test.jks");
			string ourUrl = JavaKeyStoreProvider.SchemeName + "://file" + jksPath.ToUri();
			FilePath file = new FilePath(testDir, "test.jks");
			file.Delete();
			conf.Set(CredentialProviderFactory.CredentialProviderPath, ourUrl);
			CredentialProvider provider = CredentialProviderFactory.GetProviders(conf)[0];
			char[] keypass = new char[] { 'k', 'e', 'y', 'p', 'a', 's', 's' };
			char[] storepass = new char[] { 's', 't', 'o', 'r', 'e', 'p', 'a', 's', 's' };
			char[] trustpass = new char[] { 't', 'r', 'u', 's', 't', 'p', 'a', 's', 's' };
			// ensure that we get nulls when the key isn't there
			NUnit.Framework.Assert.AreEqual(null, provider.GetCredentialEntry(DFSConfigKeys.DfsServerHttpsKeypasswordKey
				));
			NUnit.Framework.Assert.AreEqual(null, provider.GetCredentialEntry(DFSConfigKeys.DfsServerHttpsKeystorePasswordKey
				));
			NUnit.Framework.Assert.AreEqual(null, provider.GetCredentialEntry(DFSConfigKeys.DfsServerHttpsTruststorePasswordKey
				));
			// create new aliases
			try
			{
				provider.CreateCredentialEntry(DFSConfigKeys.DfsServerHttpsKeypasswordKey, keypass
					);
				provider.CreateCredentialEntry(DFSConfigKeys.DfsServerHttpsKeystorePasswordKey, storepass
					);
				provider.CreateCredentialEntry(DFSConfigKeys.DfsServerHttpsTruststorePasswordKey, 
					trustpass);
				// write out so that it can be found in checks
				provider.Flush();
			}
			catch (Exception e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
				throw;
			}
			// make sure we get back the right key directly from api
			Assert.AssertArrayEquals(keypass, provider.GetCredentialEntry(DFSConfigKeys.DfsServerHttpsKeypasswordKey
				).GetCredential());
			Assert.AssertArrayEquals(storepass, provider.GetCredentialEntry(DFSConfigKeys.DfsServerHttpsKeystorePasswordKey
				).GetCredential());
			Assert.AssertArrayEquals(trustpass, provider.GetCredentialEntry(DFSConfigKeys.DfsServerHttpsTruststorePasswordKey
				).GetCredential());
			// use WebAppUtils as would be used by loadSslConfiguration
			NUnit.Framework.Assert.AreEqual("keypass", DFSUtil.GetPassword(conf, DFSConfigKeys
				.DfsServerHttpsKeypasswordKey));
			NUnit.Framework.Assert.AreEqual("storepass", DFSUtil.GetPassword(conf, DFSConfigKeys
				.DfsServerHttpsKeystorePasswordKey));
			NUnit.Framework.Assert.AreEqual("trustpass", DFSUtil.GetPassword(conf, DFSConfigKeys
				.DfsServerHttpsTruststorePasswordKey));
			// let's make sure that a password that doesn't exist returns null
			NUnit.Framework.Assert.AreEqual(null, DFSUtil.GetPassword(conf, "invalid-alias"));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetNNServiceRpcAddressesForNsIds()
		{
			Configuration conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsNameservices, "nn1,nn2");
			conf.Set(DFSConfigKeys.DfsInternalNameservicesKey, "nn1");
			// Test - configured list of namenodes are returned
			string Nn1Address = "localhost:9000";
			string Nn2Address = "localhost:9001";
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeRpcAddressKey, "nn1"), Nn1Address
				);
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeRpcAddressKey, "nn2"), Nn2Address
				);
			IDictionary<string, IDictionary<string, IPEndPoint>> nnMap = DFSUtil.GetNNServiceRpcAddressesForCluster
				(conf);
			NUnit.Framework.Assert.AreEqual(1, nnMap.Count);
			NUnit.Framework.Assert.IsTrue(nnMap.Contains("nn1"));
			conf.Set(DFSConfigKeys.DfsInternalNameservicesKey, "nn3");
			try
			{
				DFSUtil.GetNNServiceRpcAddressesForCluster(conf);
				NUnit.Framework.Assert.Fail("Should fail for misconfiguration");
			}
			catch (IOException)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEncryptionProbe()
		{
			Configuration conf = new Configuration(false);
			conf.Unset(DFSConfigKeys.DfsEncryptionKeyProviderUri);
			NUnit.Framework.Assert.IsFalse("encryption enabled on no provider key", DFSUtil.IsHDFSEncryptionEnabled
				(conf));
			conf.Set(DFSConfigKeys.DfsEncryptionKeyProviderUri, string.Empty);
			NUnit.Framework.Assert.IsFalse("encryption enabled on empty provider key", DFSUtil
				.IsHDFSEncryptionEnabled(conf));
			conf.Set(DFSConfigKeys.DfsEncryptionKeyProviderUri, "\n\t\n");
			NUnit.Framework.Assert.IsFalse("encryption enabled on whitespace provider key", DFSUtil
				.IsHDFSEncryptionEnabled(conf));
			conf.Set(DFSConfigKeys.DfsEncryptionKeyProviderUri, "http://hadoop.apache.org");
			NUnit.Framework.Assert.IsTrue("encryption disabled on valid provider key", DFSUtil
				.IsHDFSEncryptionEnabled(conf));
		}
	}
}
