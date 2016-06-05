/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
using NUnit.Framework;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FI;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>Test DataTransferProtocol with fault injection.</summary>
	public class TestFiDataTransferProtocol2
	{
		internal const short Replication = 3;

		internal const long Blocksize = 1L * (1L << 20);

		internal const int PacketSize = 1024;

		internal const int MinNPacket = 3;

		internal const int MaxNPacket = 10;

		internal const int MaxSleep = 1000;

		internal static readonly Configuration conf = new Configuration();

		static TestFiDataTransferProtocol2()
		{
			conf.SetInt(DFSConfigKeys.DfsDatanodeHandlerCountKey, 1);
			conf.SetInt(DFSConfigKeys.DfsReplicationKey, Replication);
			conf.SetInt(DFSConfigKeys.DfsClientWritePacketSizeKey, PacketSize);
			conf.SetInt(DFSConfigKeys.DfsClientSocketTimeoutKey, 5000);
		}

		internal static readonly byte[] bytes = new byte[MaxNPacket * PacketSize];

		internal static readonly byte[] toRead = new byte[MaxNPacket * PacketSize];

		/// <exception cref="System.IO.IOException"/>
		private static FSDataOutputStream CreateFile(FileSystem fs, Path p)
		{
			return fs.Create(p, true, fs.GetConf().GetInt(CommonConfigurationKeysPublic.IoFileBufferSizeKey
				, 4096), Replication, Blocksize);
		}

		/// <summary>1.</summary>
		/// <remarks>
		/// 1. create files with dfs
		/// 2. write MIN_N_PACKET to MAX_N_PACKET packets
		/// 3. close file
		/// 4. open the same file
		/// 5. read the bytes and compare results
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private static void WriteSeveralPackets(string methodName)
		{
			Random r = FiTestUtil.Random.Get();
			int nPackets = FiTestUtil.NextRandomInt(MinNPacket, MaxNPacket + 1);
			int lastPacketSize = FiTestUtil.NextRandomInt(1, PacketSize + 1);
			int size = (nPackets - 1) * PacketSize + lastPacketSize;
			FiTestUtil.Log.Info("size=" + size + ", nPackets=" + nPackets + ", lastPacketSize="
				 + lastPacketSize);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(Replication
				 + 2).Build();
			FileSystem dfs = cluster.GetFileSystem();
			try
			{
				Path p = new Path("/" + methodName + "/foo");
				FSDataOutputStream @out = CreateFile(dfs, p);
				long seed = r.NextLong();
				Random ran = new Random(seed);
				ran.NextBytes(bytes);
				@out.Write(bytes, 0, size);
				@out.Close();
				FSDataInputStream @in = dfs.Open(p);
				int totalRead = 0;
				int nRead = 0;
				while ((nRead = @in.Read(toRead, totalRead, size - totalRead)) > 0)
				{
					totalRead += nRead;
				}
				NUnit.Framework.Assert.AreEqual("Cannot read file.", size, totalRead);
				for (int i = 0; i < size; i++)
				{
					NUnit.Framework.Assert.IsTrue("File content differ.", bytes[i] == toRead[i]);
				}
			}
			finally
			{
				dfs.Close();
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void InitSlowDatanodeTest(DataTransferTestUtil.DataTransferTest t, 
			DataTransferTestUtil.SleepAction a)
		{
			t.fiCallReceivePacket.Set(a);
			t.fiReceiverOpWriteBlock.Set(a);
			t.fiStatusRead.Set(a);
		}

		/// <exception cref="System.IO.IOException"/>
		private void RunTest17_19(string methodName, int dnIndex)
		{
			FiTestUtil.Log.Info("Running " + methodName + " ...");
			DataTransferTestUtil.DataTransferTest t = (DataTransferTestUtil.DataTransferTest)
				DataTransferTestUtil.InitTest();
			InitSlowDatanodeTest(t, new DataTransferTestUtil.SleepAction(methodName, 0, 0, MaxSleep
				));
			InitSlowDatanodeTest(t, new DataTransferTestUtil.SleepAction(methodName, 1, 0, MaxSleep
				));
			InitSlowDatanodeTest(t, new DataTransferTestUtil.SleepAction(methodName, 2, 0, MaxSleep
				));
			t.fiCallWritePacketToDisk.Set(new DataTransferTestUtil.CountdownDoosAction(methodName
				, dnIndex, 3));
			t.fiPipelineErrorAfterInit.Set(new DataTransferTestUtil.VerificationAction(methodName
				, dnIndex));
			WriteSeveralPackets(methodName);
			NUnit.Framework.Assert.IsTrue(t.IsSuccess());
		}

		/// <exception cref="System.IO.IOException"/>
		private void RunTest29_30(string methodName, int dnIndex)
		{
			FiTestUtil.Log.Info("Running " + methodName + " ...");
			DataTransferTestUtil.DataTransferTest t = (DataTransferTestUtil.DataTransferTest)
				DataTransferTestUtil.InitTest();
			InitSlowDatanodeTest(t, new DataTransferTestUtil.SleepAction(methodName, 0, 0, MaxSleep
				));
			InitSlowDatanodeTest(t, new DataTransferTestUtil.SleepAction(methodName, 1, 0, MaxSleep
				));
			InitSlowDatanodeTest(t, new DataTransferTestUtil.SleepAction(methodName, 2, 0, MaxSleep
				));
			t.fiAfterDownstreamStatusRead.Set(new DataTransferTestUtil.CountdownOomAction(methodName
				, dnIndex, 3));
			t.fiPipelineErrorAfterInit.Set(new DataTransferTestUtil.VerificationAction(methodName
				, dnIndex));
			WriteSeveralPackets(methodName);
			NUnit.Framework.Assert.IsTrue(t.IsSuccess());
		}

		/// <exception cref="System.IO.IOException"/>
		private void RunTest34_35(string methodName, int dnIndex)
		{
			FiTestUtil.Log.Info("Running " + methodName + " ...");
			DataTransferTestUtil.DataTransferTest t = (DataTransferTestUtil.DataTransferTest)
				DataTransferTestUtil.InitTest();
			t.fiAfterDownstreamStatusRead.Set(new DataTransferTestUtil.CountdownSleepAction(methodName
				, dnIndex, 0, 3));
			t.fiPipelineErrorAfterInit.Set(new DataTransferTestUtil.VerificationAction(methodName
				, dnIndex));
			WriteSeveralPackets(methodName);
			NUnit.Framework.Assert.IsTrue(t.IsSuccess());
		}

		/// <summary>
		/// Streaming:
		/// Randomize datanode speed, write several packets,
		/// DN0 throws a DiskOutOfSpaceError when it writes the third packet to disk.
		/// </summary>
		/// <remarks>
		/// Streaming:
		/// Randomize datanode speed, write several packets,
		/// DN0 throws a DiskOutOfSpaceError when it writes the third packet to disk.
		/// Client gets an IOException and determines DN0 bad.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_17()
		{
			string methodName = FiTestUtil.GetMethodName();
			RunTest17_19(methodName, 0);
		}

		/// <summary>
		/// Streaming:
		/// Randomize datanode speed, write several packets,
		/// DN1 throws a DiskOutOfSpaceError when it writes the third packet to disk.
		/// </summary>
		/// <remarks>
		/// Streaming:
		/// Randomize datanode speed, write several packets,
		/// DN1 throws a DiskOutOfSpaceError when it writes the third packet to disk.
		/// Client gets an IOException and determines DN1 bad.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_18()
		{
			string methodName = FiTestUtil.GetMethodName();
			RunTest17_19(methodName, 1);
		}

		/// <summary>
		/// Streaming:
		/// Randomize datanode speed, write several packets,
		/// DN2 throws a DiskOutOfSpaceError when it writes the third packet to disk.
		/// </summary>
		/// <remarks>
		/// Streaming:
		/// Randomize datanode speed, write several packets,
		/// DN2 throws a DiskOutOfSpaceError when it writes the third packet to disk.
		/// Client gets an IOException and determines DN2 bad.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_19()
		{
			string methodName = FiTestUtil.GetMethodName();
			RunTest17_19(methodName, 2);
		}

		/// <summary>Streaming: Client writes several packets with DN0 very slow.</summary>
		/// <remarks>
		/// Streaming: Client writes several packets with DN0 very slow. Client
		/// finishes write successfully.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_20()
		{
			string methodName = FiTestUtil.GetMethodName();
			FiTestUtil.Log.Info("Running " + methodName + " ...");
			DataTransferTestUtil.DataTransferTest t = (DataTransferTestUtil.DataTransferTest)
				DataTransferTestUtil.InitTest();
			InitSlowDatanodeTest(t, new DataTransferTestUtil.SleepAction(methodName, 0, MaxSleep
				));
			WriteSeveralPackets(methodName);
		}

		/// <summary>Streaming: Client writes several packets with DN1 very slow.</summary>
		/// <remarks>
		/// Streaming: Client writes several packets with DN1 very slow. Client
		/// finishes write successfully.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_21()
		{
			string methodName = FiTestUtil.GetMethodName();
			FiTestUtil.Log.Info("Running " + methodName + " ...");
			DataTransferTestUtil.DataTransferTest t = (DataTransferTestUtil.DataTransferTest)
				DataTransferTestUtil.InitTest();
			InitSlowDatanodeTest(t, new DataTransferTestUtil.SleepAction(methodName, 1, MaxSleep
				));
			WriteSeveralPackets(methodName);
		}

		/// <summary>Streaming: Client writes several packets with DN2 very slow.</summary>
		/// <remarks>
		/// Streaming: Client writes several packets with DN2 very slow. Client
		/// finishes write successfully.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_22()
		{
			string methodName = FiTestUtil.GetMethodName();
			FiTestUtil.Log.Info("Running " + methodName + " ...");
			DataTransferTestUtil.DataTransferTest t = (DataTransferTestUtil.DataTransferTest)
				DataTransferTestUtil.InitTest();
			InitSlowDatanodeTest(t, new DataTransferTestUtil.SleepAction(methodName, 2, MaxSleep
				));
			WriteSeveralPackets(methodName);
		}

		/// <summary>
		/// Streaming: Randomize datanode speed, write several packets, DN1 throws a
		/// OutOfMemoryException when it receives the ack of the third packet from DN2.
		/// </summary>
		/// <remarks>
		/// Streaming: Randomize datanode speed, write several packets, DN1 throws a
		/// OutOfMemoryException when it receives the ack of the third packet from DN2.
		/// Client gets an IOException and determines DN1 bad.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_29()
		{
			string methodName = FiTestUtil.GetMethodName();
			RunTest29_30(methodName, 1);
		}

		/// <summary>
		/// Streaming: Randomize datanode speed, write several packets, DN0 throws a
		/// OutOfMemoryException when it receives the ack of the third packet from DN1.
		/// </summary>
		/// <remarks>
		/// Streaming: Randomize datanode speed, write several packets, DN0 throws a
		/// OutOfMemoryException when it receives the ack of the third packet from DN1.
		/// Client gets an IOException and determines DN0 bad.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_30()
		{
			string methodName = FiTestUtil.GetMethodName();
			RunTest29_30(methodName, 0);
		}

		/// <summary>
		/// Streaming: Write several packets, DN1 never responses when it receives the
		/// ack of the third packet from DN2.
		/// </summary>
		/// <remarks>
		/// Streaming: Write several packets, DN1 never responses when it receives the
		/// ack of the third packet from DN2. Client gets an IOException and determines
		/// DN1 bad.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_34()
		{
			string methodName = FiTestUtil.GetMethodName();
			RunTest34_35(methodName, 1);
		}

		/// <summary>
		/// Streaming: Write several packets, DN0 never responses when it receives the
		/// ack of the third packet from DN1.
		/// </summary>
		/// <remarks>
		/// Streaming: Write several packets, DN0 never responses when it receives the
		/// ack of the third packet from DN1. Client gets an IOException and determines
		/// DN0 bad.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_35()
		{
			string methodName = FiTestUtil.GetMethodName();
			RunTest34_35(methodName, 0);
		}
	}
}
