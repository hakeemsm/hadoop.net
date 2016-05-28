using System;
using System.Collections.Generic;
using System.Net;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Net;
using Org.Hamcrest.Core;
using Org.Mockito.Internal.Util.Reflection;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	public class TestDatanodeManager
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestDatanodeManager));

		internal readonly int NumIterations = 500;

		//The number of times the registration / removal of nodes should happen
		/// <exception cref="System.IO.IOException"/>
		private static DatanodeManager MockDatanodeManager(FSNamesystem fsn, Configuration
			 conf)
		{
			BlockManager bm = Org.Mockito.Mockito.Mock<BlockManager>();
			DatanodeManager dm = new DatanodeManager(bm, fsn, conf);
			return dm;
		}

		/// <summary>Create an InetSocketAddress for a host:port string</summary>
		/// <param name="host">a host identifier in host:port format</param>
		/// <returns>a corresponding InetSocketAddress object</returns>
		private static IPEndPoint Entry(string host)
		{
			return HostFileManager.ParseEntry("dummy", "dummy", host);
		}

		/// <summary>
		/// This test sends a random sequence of node registrations and node removals
		/// to the DatanodeManager (of nodes with different IDs and versions), and
		/// checks that the DatanodeManager keeps a correct count of different software
		/// versions at all times.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestNumVersionsReportedCorrect()
		{
			//Create the DatanodeManager which will be tested
			FSNamesystem fsn = Org.Mockito.Mockito.Mock<FSNamesystem>();
			Org.Mockito.Mockito.When(fsn.HasWriteLock()).ThenReturn(true);
			DatanodeManager dm = new DatanodeManager(Org.Mockito.Mockito.Mock<BlockManager>()
				, fsn, new Configuration());
			//Seed the RNG with a known value so test failures are easier to reproduce
			Random rng = new Random();
			int seed = rng.Next();
			rng = new Random(seed);
			Log.Info("Using seed " + seed + " for testing");
			//A map of the Storage IDs to the DN registration it was registered with
			Dictionary<string, DatanodeRegistration> sIdToDnReg = new Dictionary<string, DatanodeRegistration
				>();
			for (int i = 0; i < NumIterations; ++i)
			{
				//If true, remove a node for every 3rd time (if there's one)
				if (rng.NextBoolean() && i % 3 == 0 && sIdToDnReg.Count != 0)
				{
					//Pick a random node.
					int randomIndex = rng.Next() % sIdToDnReg.Count;
					//Iterate to that random position 
					IEnumerator<KeyValuePair<string, DatanodeRegistration>> it = sIdToDnReg.GetEnumerator
						();
					for (int j = 0; j < randomIndex - 1; ++j)
					{
						it.Next();
					}
					DatanodeRegistration toRemove = it.Next().Value;
					Log.Info("Removing node " + toRemove.GetDatanodeUuid() + " ip " + toRemove.GetXferAddr
						() + " version : " + toRemove.GetSoftwareVersion());
					//Remove that random node
					dm.RemoveDatanode(toRemove);
					it.Remove();
				}
				else
				{
					// Otherwise register a node. This node may be a new / an old one
					//Pick a random storageID to register.
					string storageID = "someStorageID" + rng.Next(5000);
					DatanodeRegistration dr = Org.Mockito.Mockito.Mock<DatanodeRegistration>();
					Org.Mockito.Mockito.When(dr.GetDatanodeUuid()).ThenReturn(storageID);
					//If this storageID had already been registered before
					if (sIdToDnReg.Contains(storageID))
					{
						dr = sIdToDnReg[storageID];
						//Half of the times, change the IP address
						if (rng.NextBoolean())
						{
							dr.SetIpAddr(dr.GetIpAddr() + "newIP");
						}
					}
					else
					{
						//This storageID has never been registered
						//Ensure IP address is unique to storageID
						string ip = "someIP" + storageID;
						Org.Mockito.Mockito.When(dr.GetIpAddr()).ThenReturn(ip);
						Org.Mockito.Mockito.When(dr.GetXferAddr()).ThenReturn(ip + ":9000");
						Org.Mockito.Mockito.When(dr.GetXferPort()).ThenReturn(9000);
					}
					//Pick a random version to register with
					Org.Mockito.Mockito.When(dr.GetSoftwareVersion()).ThenReturn("version" + rng.Next
						(5));
					Log.Info("Registering node storageID: " + dr.GetDatanodeUuid() + ", version: " + 
						dr.GetSoftwareVersion() + ", IP address: " + dr.GetXferAddr());
					//Register this random node
					dm.RegisterDatanode(dr);
					sIdToDnReg[storageID] = dr;
				}
				//Verify DatanodeManager still has the right count
				IDictionary<string, int> mapToCheck = dm.GetDatanodesSoftwareVersions();
				//Remove counts from versions and make sure that after removing all nodes
				//mapToCheck is empty
				foreach (KeyValuePair<string, DatanodeRegistration> it_1 in sIdToDnReg)
				{
					string ver = it_1.Value.GetSoftwareVersion();
					if (!mapToCheck.Contains(ver))
					{
						throw new Exception("The correct number of datanodes of a " + "version was not found on iteration "
							 + i);
					}
					mapToCheck[ver] = mapToCheck[ver] - 1;
					if (mapToCheck[ver] == 0)
					{
						Sharpen.Collections.Remove(mapToCheck, ver);
					}
				}
				foreach (KeyValuePair<string, int> entry in mapToCheck)
				{
					Log.Info("Still in map: " + entry.Key + " has " + entry.Value);
				}
				NUnit.Framework.Assert.AreEqual("The map of version counts returned by DatanodeManager was"
					 + " not what it was expected to be on iteration " + i, 0, mapToCheck.Count);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRejectUnresolvedDatanodes()
		{
			//Create the DatanodeManager which will be tested
			FSNamesystem fsn = Org.Mockito.Mockito.Mock<FSNamesystem>();
			Org.Mockito.Mockito.When(fsn.HasWriteLock()).ThenReturn(true);
			Configuration conf = new Configuration();
			//Set configuration property for rejecting unresolved topology mapping
			conf.SetBoolean(DFSConfigKeys.DfsRejectUnresolvedDnTopologyMappingKey, true);
			//set TestDatanodeManager.MyResolver to be used for topology resolving
			conf.SetClass(CommonConfigurationKeysPublic.NetTopologyNodeSwitchMappingImplKey, 
				typeof(TestDatanodeManager.MyResolver), typeof(DNSToSwitchMapping));
			//create DatanodeManager
			DatanodeManager dm = new DatanodeManager(Org.Mockito.Mockito.Mock<BlockManager>()
				, fsn, conf);
			//storageID to register.
			string storageID = "someStorageID-123";
			DatanodeRegistration dr = Org.Mockito.Mockito.Mock<DatanodeRegistration>();
			Org.Mockito.Mockito.When(dr.GetDatanodeUuid()).ThenReturn(storageID);
			try
			{
				//Register this node
				dm.RegisterDatanode(dr);
				NUnit.Framework.Assert.Fail("Expected an UnresolvedTopologyException");
			}
			catch (UnresolvedTopologyException)
			{
				Log.Info("Expected - topology is not resolved and " + "registration is rejected."
					);
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("Expected an UnresolvedTopologyException");
			}
		}

		/// <summary>
		/// MyResolver class provides resolve method which always returns null
		/// in order to simulate unresolved topology mapping.
		/// </summary>
		public class MyResolver : DNSToSwitchMapping
		{
			public virtual IList<string> Resolve(IList<string> names)
			{
				return null;
			}

			public virtual void ReloadCachedMappings()
			{
			}

			public virtual void ReloadCachedMappings(IList<string> names)
			{
			}
		}

		/// <summary>
		/// This test creates a LocatedBlock with 5 locations, sorts the locations
		/// based on the network topology, and ensures the locations are still aligned
		/// with the storage ids and storage types.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSortLocatedBlocks()
		{
			// create the DatanodeManager which will be tested
			FSNamesystem fsn = Org.Mockito.Mockito.Mock<FSNamesystem>();
			Org.Mockito.Mockito.When(fsn.HasWriteLock()).ThenReturn(true);
			DatanodeManager dm = new DatanodeManager(Org.Mockito.Mockito.Mock<BlockManager>()
				, fsn, new Configuration());
			// register 5 datanodes, each with different storage ID and type
			DatanodeInfo[] locs = new DatanodeInfo[5];
			string[] storageIDs = new string[5];
			StorageType[] storageTypes = new StorageType[] { StorageType.Archive, StorageType
				.Default, StorageType.Disk, StorageType.RamDisk, StorageType.Ssd };
			for (int i = 0; i < 5; i++)
			{
				// register new datanode
				string uuid = "UUID-" + i;
				string ip = "IP-" + i;
				DatanodeRegistration dr = Org.Mockito.Mockito.Mock<DatanodeRegistration>();
				Org.Mockito.Mockito.When(dr.GetDatanodeUuid()).ThenReturn(uuid);
				Org.Mockito.Mockito.When(dr.GetIpAddr()).ThenReturn(ip);
				Org.Mockito.Mockito.When(dr.GetXferAddr()).ThenReturn(ip + ":9000");
				Org.Mockito.Mockito.When(dr.GetXferPort()).ThenReturn(9000);
				Org.Mockito.Mockito.When(dr.GetSoftwareVersion()).ThenReturn("version1");
				dm.RegisterDatanode(dr);
				// get location and storage information
				locs[i] = dm.GetDatanode(uuid);
				storageIDs[i] = "storageID-" + i;
			}
			// set first 2 locations as decomissioned
			locs[0].SetDecommissioned();
			locs[1].SetDecommissioned();
			// create LocatedBlock with above locations
			ExtendedBlock b = new ExtendedBlock("somePoolID", 1234);
			LocatedBlock block = new LocatedBlock(b, locs, storageIDs, storageTypes);
			IList<LocatedBlock> blocks = new AList<LocatedBlock>();
			blocks.AddItem(block);
			string targetIp = locs[4].GetIpAddr();
			// sort block locations
			dm.SortLocatedBlocks(targetIp, blocks);
			// check that storage IDs/types are aligned with datanode locs
			DatanodeInfo[] sortedLocs = block.GetLocations();
			storageIDs = block.GetStorageIDs();
			storageTypes = block.GetStorageTypes();
			Assert.AssertThat(sortedLocs.Length, IS.Is(5));
			Assert.AssertThat(storageIDs.Length, IS.Is(5));
			Assert.AssertThat(storageTypes.Length, IS.Is(5));
			for (int i_1 = 0; i_1 < sortedLocs.Length; i_1++)
			{
				Assert.AssertThat(((DatanodeInfoWithStorage)sortedLocs[i_1]).GetStorageID(), IS.Is
					(storageIDs[i_1]));
				Assert.AssertThat(((DatanodeInfoWithStorage)sortedLocs[i_1]).GetStorageType(), IS.Is
					(storageTypes[i_1]));
			}
			// Ensure the local node is first.
			Assert.AssertThat(sortedLocs[0].GetIpAddr(), IS.Is(targetIp));
			// Ensure the two decommissioned DNs were moved to the end.
			Assert.AssertThat(sortedLocs[sortedLocs.Length - 1].GetAdminState(), IS.Is(DatanodeInfo.AdminStates
				.Decommissioned));
			Assert.AssertThat(sortedLocs[sortedLocs.Length - 2].GetAdminState(), IS.Is(DatanodeInfo.AdminStates
				.Decommissioned));
		}

		/// <summary>
		/// Test whether removing a host from the includes list without adding it to
		/// the excludes list will exclude it from data node reports.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveIncludedNode()
		{
			FSNamesystem fsn = Org.Mockito.Mockito.Mock<FSNamesystem>();
			// Set the write lock so that the DatanodeManager can start
			Org.Mockito.Mockito.When(fsn.HasWriteLock()).ThenReturn(true);
			DatanodeManager dm = MockDatanodeManager(fsn, new Configuration());
			HostFileManager hm = new HostFileManager();
			HostFileManager.HostSet noNodes = new HostFileManager.HostSet();
			HostFileManager.HostSet oneNode = new HostFileManager.HostSet();
			HostFileManager.HostSet twoNodes = new HostFileManager.HostSet();
			DatanodeRegistration dr1 = new DatanodeRegistration(new DatanodeID("127.0.0.1", "127.0.0.1"
				, "someStorageID-123", 12345, 12345, 12345, 12345), new StorageInfo(HdfsServerConstants.NodeType
				.DataNode), new ExportedBlockKeys(), "test");
			DatanodeRegistration dr2 = new DatanodeRegistration(new DatanodeID("127.0.0.1", "127.0.0.1"
				, "someStorageID-234", 23456, 23456, 23456, 23456), new StorageInfo(HdfsServerConstants.NodeType
				.DataNode), new ExportedBlockKeys(), "test");
			twoNodes.Add(Entry("127.0.0.1:12345"));
			twoNodes.Add(Entry("127.0.0.1:23456"));
			oneNode.Add(Entry("127.0.0.1:23456"));
			hm.Refresh(twoNodes, noNodes);
			Whitebox.SetInternalState(dm, "hostFileManager", hm);
			// Register two data nodes to simulate them coming up.
			// We need to add two nodes, because if we have only one node, removing it
			// will cause the includes list to be empty, which means all hosts will be
			// allowed.
			dm.RegisterDatanode(dr1);
			dm.RegisterDatanode(dr2);
			// Make sure that both nodes are reported
			IList<DatanodeDescriptor> both = dm.GetDatanodeListForReport(HdfsConstants.DatanodeReportType
				.All);
			// Sort the list so that we know which one is which
			both.Sort();
			NUnit.Framework.Assert.AreEqual("Incorrect number of hosts reported", 2, both.Count
				);
			NUnit.Framework.Assert.AreEqual("Unexpected host or host in unexpected position", 
				"127.0.0.1:12345", both[0].GetInfoAddr());
			NUnit.Framework.Assert.AreEqual("Unexpected host or host in unexpected position", 
				"127.0.0.1:23456", both[1].GetInfoAddr());
			// Remove one node from includes, but do not add it to excludes.
			hm.Refresh(oneNode, noNodes);
			// Make sure that only one node is still reported
			IList<DatanodeDescriptor> onlyOne = dm.GetDatanodeListForReport(HdfsConstants.DatanodeReportType
				.All);
			NUnit.Framework.Assert.AreEqual("Incorrect number of hosts reported", 1, onlyOne.
				Count);
			NUnit.Framework.Assert.AreEqual("Unexpected host reported", "127.0.0.1:23456", onlyOne
				[0].GetInfoAddr());
			// Remove all nodes from includes
			hm.Refresh(noNodes, noNodes);
			// Check that both nodes are reported again
			IList<DatanodeDescriptor> bothAgain = dm.GetDatanodeListForReport(HdfsConstants.DatanodeReportType
				.All);
			// Sort the list so that we know which one is which
			bothAgain.Sort();
			NUnit.Framework.Assert.AreEqual("Incorrect number of hosts reported", 2, bothAgain
				.Count);
			NUnit.Framework.Assert.AreEqual("Unexpected host or host in unexpected position", 
				"127.0.0.1:12345", bothAgain[0].GetInfoAddr());
			NUnit.Framework.Assert.AreEqual("Unexpected host or host in unexpected position", 
				"127.0.0.1:23456", bothAgain[1].GetInfoAddr());
		}
	}
}
