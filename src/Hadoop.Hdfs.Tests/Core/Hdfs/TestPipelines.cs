using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestPipelines
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestPipelines));

		private const short ReplFactor = 3;

		private const int RandLimit = 2000;

		private const int FileSize = 10000;

		private MiniDFSCluster cluster;

		private DistributedFileSystem fs;

		private static Configuration conf;

		internal static readonly Random rand = new Random(RandLimit);

		static TestPipelines()
		{
			InitLoggers();
			SetConfiguration();
		}

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void StartUpCluster()
		{
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(ReplFactor).Build();
			fs = cluster.GetFileSystem();
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void ShutDownCluster()
		{
			if (fs != null)
			{
				fs.Close();
			}
			if (cluster != null)
			{
				cluster.ShutdownDataNodes();
				cluster.Shutdown();
			}
		}

		/// <summary>Creates and closes a file of certain length.</summary>
		/// <remarks>
		/// Creates and closes a file of certain length.
		/// Calls append to allow next write() operation to add to the end of it
		/// After write() invocation, calls hflush() to make sure that data sunk through
		/// the pipeline and check the state of the last block's replica.
		/// It supposes to be in RBW state
		/// </remarks>
		/// <exception cref="System.IO.IOException">in case of an error</exception>
		[NUnit.Framework.Test]
		public virtual void Pipeline_01()
		{
			string MethodName = GenericTestUtils.GetMethodName();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Running " + MethodName);
			}
			Path filePath = new Path("/" + MethodName + ".dat");
			DFSTestUtil.CreateFile(fs, filePath, FileSize, ReplFactor, rand.NextLong());
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Invoking append but doing nothing otherwise...");
			}
			FSDataOutputStream ofs = fs.Append(filePath);
			ofs.WriteBytes("Some more stuff to write");
			((DFSOutputStream)ofs.GetWrappedStream()).Hflush();
			IList<LocatedBlock> lb = cluster.GetNameNodeRpc().GetBlockLocations(filePath.ToString
				(), FileSize - 1, FileSize).GetLocatedBlocks();
			string bpid = cluster.GetNamesystem().GetBlockPoolId();
			foreach (DataNode dn in cluster.GetDataNodes())
			{
				Replica r = DataNodeTestUtils.FetchReplicaInfo(dn, bpid, lb[0].GetBlock().GetBlockId
					());
				NUnit.Framework.Assert.IsTrue("Replica on DN " + dn + " shouldn't be null", r != 
					null);
				NUnit.Framework.Assert.AreEqual("Should be RBW replica on " + dn + " after sequence of calls append()/write()/hflush()"
					, HdfsServerConstants.ReplicaState.Rbw, r.GetState());
			}
			ofs.Close();
		}

		/// <summary>These two test cases are already implemented by</summary>
		/// <link{TestReadWhileWriting>}</link{TestReadWhileWriting>
		public virtual void Pipeline_02_03()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		internal static byte[] WriteData(FSDataOutputStream @out, int length)
		{
			int bytesToWrite = length;
			byte[] ret = new byte[bytesToWrite];
			byte[] toWrite = new byte[1024];
			int written = 0;
			Random rb = new Random(rand.NextLong());
			while (bytesToWrite > 0)
			{
				rb.NextBytes(toWrite);
				int bytesToWriteNext = (1024 < bytesToWrite) ? 1024 : bytesToWrite;
				@out.Write(toWrite, 0, bytesToWriteNext);
				System.Array.Copy(toWrite, 0, ret, (ret.Length - bytesToWrite), bytesToWriteNext);
				written += bytesToWriteNext;
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Written: " + bytesToWriteNext + "; Total: " + written);
				}
				bytesToWrite -= bytesToWriteNext;
			}
			return ret;
		}

		private static void SetConfiguration()
		{
			conf = new Configuration();
			int customPerChecksumSize = 700;
			int customBlockSize = customPerChecksumSize * 3;
			conf.SetInt(DFSConfigKeys.DfsClientWritePacketSizeKey, 100);
			conf.SetInt(DFSConfigKeys.DfsBytesPerChecksumKey, customPerChecksumSize);
			conf.SetInt(DFSConfigKeys.DfsBlockSizeKey, customBlockSize);
			conf.SetInt(DFSConfigKeys.DfsClientWritePacketSizeKey, customBlockSize / 2);
			conf.SetInt(DFSConfigKeys.DfsClientSocketTimeoutKey, 0);
		}

		private static void InitLoggers()
		{
			DFSTestUtil.SetNameNodeLogLevel(Level.All);
			GenericTestUtils.SetLogLevel(DataNode.Log, Level.All);
			GenericTestUtils.SetLogLevel(DFSClient.Log, Level.All);
		}
	}
}
