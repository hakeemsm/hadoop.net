using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// This program starts a mini cluster of data nodes
	/// (ie a mini cluster without the name node), all within one address space.
	/// </summary>
	/// <remarks>
	/// This program starts a mini cluster of data nodes
	/// (ie a mini cluster without the name node), all within one address space.
	/// It is assumed that the name node has been started separately prior
	/// to running this program.
	/// A use case of this is to run a real name node with a large number of
	/// simulated data nodes for say a NN benchmark.
	/// Synopisis:
	/// DataNodeCluster -n numDatNodes [-racks numRacks] -simulated
	/// [-inject startingBlockId numBlocksPerDN]
	/// [ -r replicationForInjectedBlocks ]
	/// [-d editsLogDirectory]
	/// if -simulated is specified then simulated data nodes are started.
	/// if -inject is specified then blocks are injected in each datanode;
	/// -inject option is valid only for simulated data nodes.
	/// See Also @link #CreateEditsLog for creating a edits log file to
	/// inject a matching set of blocks into into a name node.
	/// Typical use of -inject is to inject blocks into a set of datanodes
	/// using this DataNodeCLuster command
	/// and then to inject the same blocks into a name node using the
	/// CreateEditsLog command.
	/// </remarks>
	public class DataNodeCluster
	{
		internal const string DatanodeDirs = "/tmp/DataNodeCluster";

		internal static string dataNodeDirs = DatanodeDirs;

		internal const string Usage = "Usage: datanodecluster " + " -n <numDataNodes> " +
			 " -bpid <bpid>" + " [-racks <numRacks>] " + " [-simulated [<simulatedCapacityPerDn>]] "
			 + " [-inject startingBlockId numBlocksPerDN]" + " [-r replicationFactorForInjectedBlocks]"
			 + " [-d dataNodeDirs]\n" + " [-checkDataNodeAddrConfig]\n" + "      Default datanode direcory is "
			 + DatanodeDirs + "\n" + "      Default replication factor for injected blocks is 1\n"
			 + "      Defaul rack is used if -racks is not specified\n" + "      Data nodes are simulated if -simulated OR conf file specifies simulated\n"
			 + "      -checkDataNodeAddrConfig tells DataNodeConf to use data node addresses from conf file, if it is set. If not set, use .localhost'.";

		internal static void PrintUsageExit()
		{
			System.Console.Out.WriteLine(Usage);
			System.Environment.Exit(-1);
		}

		internal static void PrintUsageExit(string err)
		{
			System.Console.Out.WriteLine(err);
			PrintUsageExit();
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int numDataNodes = 0;
			int numRacks = 0;
			bool inject = false;
			long startingBlockId = 1;
			int numBlocksPerDNtoInject = 0;
			int replication = 1;
			bool checkDataNodeAddrConfig = false;
			long simulatedCapacityPerDn = SimulatedFSDataset.DefaultCapacity;
			string bpid = null;
			Configuration conf = new HdfsConfiguration();
			for (int i = 0; i < args.Length; i++)
			{
				// parse command line
				if (args[i].Equals("-n"))
				{
					if (++i >= args.Length || args[i].StartsWith("-"))
					{
						PrintUsageExit("missing number of nodes");
					}
					numDataNodes = System.Convert.ToInt32(args[i]);
				}
				else
				{
					if (args[i].Equals("-racks"))
					{
						if (++i >= args.Length || args[i].StartsWith("-"))
						{
							PrintUsageExit("Missing number of racks");
						}
						numRacks = System.Convert.ToInt32(args[i]);
					}
					else
					{
						if (args[i].Equals("-r"))
						{
							if (++i >= args.Length || args[i].StartsWith("-"))
							{
								PrintUsageExit("Missing replication factor");
							}
							replication = System.Convert.ToInt32(args[i]);
						}
						else
						{
							if (args[i].Equals("-d"))
							{
								if (++i >= args.Length || args[i].StartsWith("-"))
								{
									PrintUsageExit("Missing datanode dirs parameter");
								}
								dataNodeDirs = args[i];
							}
							else
							{
								if (args[i].Equals("-simulated"))
								{
									SimulatedFSDataset.SetFactory(conf);
									if ((i + 1) < args.Length && !args[i + 1].StartsWith("-"))
									{
										simulatedCapacityPerDn = long.Parse(args[++i]);
									}
								}
								else
								{
									if (args[i].Equals("-bpid"))
									{
										if (++i >= args.Length || args[i].StartsWith("-"))
										{
											PrintUsageExit("Missing blockpoolid parameter");
										}
										bpid = args[i];
									}
									else
									{
										if (args[i].Equals("-inject"))
										{
											if (!FsDatasetSpi.Factory.GetFactory(conf).IsSimulated())
											{
												System.Console.Out.Write("-inject is valid only for simulated");
												PrintUsageExit();
											}
											inject = true;
											if (++i >= args.Length || args[i].StartsWith("-"))
											{
												PrintUsageExit("Missing starting block and number of blocks per DN to inject");
											}
											startingBlockId = System.Convert.ToInt32(args[i]);
											if (++i >= args.Length || args[i].StartsWith("-"))
											{
												PrintUsageExit("Missing number of blocks to inject");
											}
											numBlocksPerDNtoInject = System.Convert.ToInt32(args[i]);
										}
										else
										{
											if (args[i].Equals("-checkDataNodeAddrConfig"))
											{
												checkDataNodeAddrConfig = true;
											}
											else
											{
												PrintUsageExit();
											}
										}
									}
								}
							}
						}
					}
				}
			}
			if (numDataNodes <= 0 || replication <= 0)
			{
				PrintUsageExit("numDataNodes and replication have to be greater than zero");
			}
			if (replication > numDataNodes)
			{
				PrintUsageExit("Replication must be less than or equal to numDataNodes");
			}
			if (bpid == null)
			{
				PrintUsageExit("BlockPoolId must be provided");
			}
			string nameNodeAdr = FileSystem.GetDefaultUri(conf).GetAuthority();
			if (nameNodeAdr == null)
			{
				System.Console.Out.WriteLine("No name node address and port in config");
				System.Environment.Exit(-1);
			}
			bool simulated = FsDatasetSpi.Factory.GetFactory(conf).IsSimulated();
			System.Console.Out.WriteLine("Starting " + numDataNodes + (simulated ? " Simulated "
				 : " ") + " Data Nodes that will connect to Name Node at " + nameNodeAdr);
			Runtime.SetProperty("test.build.data", dataNodeDirs);
			long[] simulatedCapacities = new long[numDataNodes];
			for (int i_1 = 0; i_1 < numDataNodes; ++i_1)
			{
				simulatedCapacities[i_1] = simulatedCapacityPerDn;
			}
			MiniDFSCluster mc = new MiniDFSCluster();
			try
			{
				mc.FormatDataNodeDirs();
			}
			catch (IOException e)
			{
				System.Console.Out.WriteLine("Error formating data node dirs:" + e);
			}
			string[] rack4DataNode = null;
			if (numRacks > 0)
			{
				System.Console.Out.WriteLine("Using " + numRacks + " racks: ");
				string rackPrefix = GetUniqueRackPrefix();
				rack4DataNode = new string[numDataNodes];
				for (int i_2 = 0; i_2 < numDataNodes; ++i_2)
				{
					//rack4DataNode[i] = racks[i%numRacks];
					rack4DataNode[i_2] = rackPrefix + "-" + i_2 % numRacks;
					System.Console.Out.WriteLine("Data Node " + i_2 + " using " + rack4DataNode[i_2]);
				}
			}
			try
			{
				mc.StartDataNodes(conf, numDataNodes, true, HdfsServerConstants.StartupOption.Regular
					, rack4DataNode, null, simulatedCapacities, false, checkDataNodeAddrConfig);
				Sharpen.Thread.Sleep(10 * 1000);
				// Give the DN some time to connect to NN and init storage directories.
				if (inject)
				{
					long blockSize = 10;
					System.Console.Out.WriteLine("Injecting " + numBlocksPerDNtoInject + " blocks in each DN starting at blockId "
						 + startingBlockId + " with blocksize of " + blockSize);
					Block[] blocks = new Block[numBlocksPerDNtoInject];
					long blkid = startingBlockId;
					for (int i_dn = 0; i_dn < numDataNodes; ++i_dn)
					{
						for (int i_2 = 0; i_2 < blocks.Length; ++i_2)
						{
							blocks[i_2] = new Block(blkid++, blockSize, CreateEditsLog.BlockGenerationStamp);
						}
						for (int i_3 = 1; i_3 <= replication; ++i_3)
						{
							// inject blocks for dn_i into dn_i and replica in dn_i's neighbors 
							mc.InjectBlocks((i_dn + i_3 - 1) % numDataNodes, Arrays.AsList(blocks), bpid);
							System.Console.Out.WriteLine("Injecting blocks of dn " + i_dn + " into dn" + ((i_dn
								 + i_3 - 1) % numDataNodes));
						}
					}
					System.Console.Out.WriteLine("Created blocks from Bids " + startingBlockId + " to "
						 + (blkid - 1));
				}
			}
			catch (IOException e)
			{
				System.Console.Out.WriteLine("Error creating data node:" + e);
			}
		}

		/*
		* There is high probability that the rack id generated here will
		* not conflict with those of other data node cluster.
		* Not perfect but mostly unique rack ids are good enough
		*/
		private static string GetUniqueRackPrefix()
		{
			string ip = "unknownIP";
			try
			{
				ip = DNS.GetDefaultIP("default");
			}
			catch (UnknownHostException)
			{
				System.Console.Out.WriteLine("Could not find ip address of \"default\" inteface."
					);
			}
			int rand = DFSUtil.GetSecureRandom().Next(int.MaxValue);
			return "/Rack-" + rand + "-" + ip + "-" + Time.Now();
		}
	}
}
