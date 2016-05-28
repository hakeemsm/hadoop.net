using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>This class tests the creation and validation of metasave</summary>
	public class TestMetaSave
	{
		internal const int NumDataNodes = 2;

		internal const long seed = unchecked((long)(0xDEADBEEFL));

		internal const int blockSize = 8192;

		private static MiniDFSCluster cluster = null;

		private static FileSystem fileSys = null;

		private static NamenodeProtocols nnRpc = null;

		/// <exception cref="System.IO.IOException"/>
		private void CreateFile(FileSystem fileSys, Path name)
		{
			FSDataOutputStream stm = fileSys.Create(name, true, fileSys.GetConf().GetInt(CommonConfigurationKeys
				.IoFileBufferSizeKey, 4096), (short)2, blockSize);
			byte[] buffer = new byte[1024];
			Random rand = new Random(seed);
			rand.NextBytes(buffer);
			stm.Write(buffer);
			stm.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void SetUp()
		{
			// start a cluster
			Configuration conf = new HdfsConfiguration();
			// High value of replication interval
			// so that blocks remain under-replicated
			conf.SetInt(DFSConfigKeys.DfsNamenodeReplicationIntervalKey, 1000);
			conf.SetLong(DFSConfigKeys.DfsHeartbeatIntervalKey, 1L);
			conf.SetLong(DFSConfigKeys.DfsNamenodeHeartbeatRecheckIntervalKey, 1L);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDataNodes).Build();
			cluster.WaitActive();
			fileSys = cluster.GetFileSystem();
			nnRpc = cluster.GetNameNodeRpc();
		}

		/// <summary>Tests metasave</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMetaSave()
		{
			for (int i = 0; i < 2; i++)
			{
				Path file = new Path("/filestatus" + i);
				CreateFile(fileSys, file);
			}
			cluster.StopDataNode(1);
			// wait for namenode to discover that a datanode is dead
			Sharpen.Thread.Sleep(15000);
			nnRpc.SetReplication("/filestatus0", (short)4);
			nnRpc.MetaSave("metasave.out.txt");
			// Verification
			FileInputStream fstream = new FileInputStream(GetLogFile("metasave.out.txt"));
			DataInputStream @in = new DataInputStream(fstream);
			BufferedReader reader = null;
			try
			{
				reader = new BufferedReader(new InputStreamReader(@in));
				string line = reader.ReadLine();
				NUnit.Framework.Assert.IsTrue(line.Equals("3 files and directories, 2 blocks = 5 total"
					));
				line = reader.ReadLine();
				NUnit.Framework.Assert.IsTrue(line.Equals("Live Datanodes: 1"));
				line = reader.ReadLine();
				NUnit.Framework.Assert.IsTrue(line.Equals("Dead Datanodes: 1"));
				reader.ReadLine();
				line = reader.ReadLine();
				NUnit.Framework.Assert.IsTrue(line.Matches("^/filestatus[01]:.*"));
			}
			finally
			{
				if (reader != null)
				{
					reader.Close();
				}
			}
		}

		/// <summary>Tests metasave after delete, to make sure there are no orphaned blocks</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMetasaveAfterDelete()
		{
			for (int i = 0; i < 2; i++)
			{
				Path file = new Path("/filestatus" + i);
				CreateFile(fileSys, file);
			}
			cluster.StopDataNode(1);
			// wait for namenode to discover that a datanode is dead
			Sharpen.Thread.Sleep(15000);
			nnRpc.SetReplication("/filestatus0", (short)4);
			nnRpc.Delete("/filestatus0", true);
			nnRpc.Delete("/filestatus1", true);
			nnRpc.MetaSave("metasaveAfterDelete.out.txt");
			// Verification
			BufferedReader reader = null;
			try
			{
				FileInputStream fstream = new FileInputStream(GetLogFile("metasaveAfterDelete.out.txt"
					));
				DataInputStream @in = new DataInputStream(fstream);
				reader = new BufferedReader(new InputStreamReader(@in));
				reader.ReadLine();
				string line = reader.ReadLine();
				NUnit.Framework.Assert.IsTrue(line.Equals("Live Datanodes: 1"));
				line = reader.ReadLine();
				NUnit.Framework.Assert.IsTrue(line.Equals("Dead Datanodes: 1"));
				line = reader.ReadLine();
				NUnit.Framework.Assert.IsTrue(line.Equals("Metasave: Blocks waiting for replication: 0"
					));
				line = reader.ReadLine();
				NUnit.Framework.Assert.IsTrue(line.Equals("Mis-replicated blocks that have been postponed:"
					));
				line = reader.ReadLine();
				NUnit.Framework.Assert.IsTrue(line.Equals("Metasave: Blocks being replicated: 0")
					);
				line = reader.ReadLine();
				NUnit.Framework.Assert.IsTrue(line.Equals("Metasave: Blocks 2 waiting deletion from 1 datanodes."
					));
				//skip 2 lines to reach HDFS-9033 scenario.
				line = reader.ReadLine();
				line = reader.ReadLine();
				line = reader.ReadLine();
				NUnit.Framework.Assert.IsTrue(line.Equals("Metasave: Number of datanodes: 2"));
				line = reader.ReadLine();
				NUnit.Framework.Assert.IsFalse(line.Contains("NaN"));
			}
			finally
			{
				if (reader != null)
				{
					reader.Close();
				}
			}
		}

		/// <summary>Tests that metasave overwrites the output file (not append).</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMetaSaveOverwrite()
		{
			// metaSave twice.
			nnRpc.MetaSave("metaSaveOverwrite.out.txt");
			nnRpc.MetaSave("metaSaveOverwrite.out.txt");
			// Read output file.
			FileInputStream fis = null;
			InputStreamReader isr = null;
			BufferedReader rdr = null;
			try
			{
				fis = new FileInputStream(GetLogFile("metaSaveOverwrite.out.txt"));
				isr = new InputStreamReader(fis);
				rdr = new BufferedReader(isr);
				// Validate that file was overwritten (not appended) by checking for
				// presence of only one "Live Datanodes" line.
				bool foundLiveDatanodesLine = false;
				string line = rdr.ReadLine();
				while (line != null)
				{
					if (line.StartsWith("Live Datanodes"))
					{
						if (foundLiveDatanodesLine)
						{
							NUnit.Framework.Assert.Fail("multiple Live Datanodes lines, output file not overwritten"
								);
						}
						foundLiveDatanodesLine = true;
					}
					line = rdr.ReadLine();
				}
			}
			finally
			{
				IOUtils.Cleanup(null, rdr, isr, fis);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[AfterClass]
		public static void TearDown()
		{
			if (fileSys != null)
			{
				fileSys.Close();
			}
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Returns a File for the given name inside the log directory.</summary>
		/// <param name="name">String file name</param>
		/// <returns>File for given name inside log directory</returns>
		private static FilePath GetLogFile(string name)
		{
			return new FilePath(Runtime.GetProperty("hadoop.log.dir"), name);
		}
	}
}
