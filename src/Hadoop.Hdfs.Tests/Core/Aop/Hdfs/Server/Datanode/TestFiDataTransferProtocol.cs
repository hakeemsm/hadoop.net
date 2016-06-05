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
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FI;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>Test DataTransferProtocol with fault injection.</summary>
	public class TestFiDataTransferProtocol
	{
		internal const short Replication = 3;

		internal const long Blocksize = 1L * (1L << 20);

		internal static readonly Configuration conf = new HdfsConfiguration();

		static TestFiDataTransferProtocol()
		{
			conf.SetInt(DFSConfigKeys.DfsDatanodeHandlerCountKey, 1);
			conf.SetInt(DFSConfigKeys.DfsReplicationKey, Replication);
			conf.SetInt(DFSConfigKeys.DfsClientSocketTimeoutKey, 5000);
		}

		/// <exception cref="System.IO.IOException"/>
		private static FSDataOutputStream CreateFile(FileSystem fs, Path p)
		{
			return fs.Create(p, true, fs.GetConf().GetInt(CommonConfigurationKeys.IoFileBufferSizeKey
				, 4096), Replication, Blocksize);
		}

		/// <summary>1.</summary>
		/// <remarks>
		/// 1. create files with dfs
		/// 2. write 1 byte
		/// 3. close file
		/// 4. open the same file
		/// 5. read the 1 byte and compare results
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal static void Write1byte(string methodName)
		{
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(Replication
				 + 1).Build();
			FileSystem dfs = cluster.GetFileSystem();
			try
			{
				Path p = new Path("/" + methodName + "/foo");
				FSDataOutputStream @out = CreateFile(dfs, p);
				@out.Write(1);
				@out.Close();
				FSDataInputStream @in = dfs.Open(p);
				int b = @in.Read();
				@in.Close();
				NUnit.Framework.Assert.AreEqual(1, b);
			}
			finally
			{
				dfs.Close();
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void RunSlowDatanodeTest(string methodName, DataTransferTestUtil.SleepAction
			 a)
		{
			FiTestUtil.Log.Info("Running " + methodName + " ...");
			DataTransferTestUtil.DataTransferTest t = (DataTransferTestUtil.DataTransferTest)
				DataTransferTestUtil.InitTest();
			t.fiCallReceivePacket.Set(a);
			t.fiReceiverOpWriteBlock.Set(a);
			t.fiStatusRead.Set(a);
			Write1byte(methodName);
		}

		/// <exception cref="System.IO.IOException"/>
		private static void RunReceiverOpWriteBlockTest(string methodName, int errorIndex
			, FiTestUtil.Action<DatanodeID, IOException> a)
		{
			FiTestUtil.Log.Info("Running " + methodName + " ...");
			DataTransferTestUtil.DataTransferTest t = (DataTransferTestUtil.DataTransferTest)
				DataTransferTestUtil.InitTest();
			t.fiReceiverOpWriteBlock.Set(a);
			t.fiPipelineInitErrorNonAppend.Set(new DataTransferTestUtil.VerificationAction(methodName
				, errorIndex));
			Write1byte(methodName);
			NUnit.Framework.Assert.IsTrue(t.IsSuccess());
		}

		/// <exception cref="System.IO.IOException"/>
		private static void RunStatusReadTest(string methodName, int errorIndex, FiTestUtil.Action
			<DatanodeID, IOException> a)
		{
			FiTestUtil.Log.Info("Running " + methodName + " ...");
			DataTransferTestUtil.DataTransferTest t = (DataTransferTestUtil.DataTransferTest)
				DataTransferTestUtil.InitTest();
			t.fiStatusRead.Set(a);
			t.fiPipelineInitErrorNonAppend.Set(new DataTransferTestUtil.VerificationAction(methodName
				, errorIndex));
			Write1byte(methodName);
			NUnit.Framework.Assert.IsTrue(t.IsSuccess());
		}

		/// <exception cref="System.IO.IOException"/>
		private static void RunCallWritePacketToDisk(string methodName, int errorIndex, FiTestUtil.Action
			<DatanodeID, IOException> a)
		{
			FiTestUtil.Log.Info("Running " + methodName + " ...");
			DataTransferTestUtil.DataTransferTest t = (DataTransferTestUtil.DataTransferTest)
				DataTransferTestUtil.InitTest();
			t.fiCallWritePacketToDisk.Set(a);
			t.fiPipelineErrorAfterInit.Set(new DataTransferTestUtil.VerificationAction(methodName
				, errorIndex));
			Write1byte(methodName);
			NUnit.Framework.Assert.IsTrue(t.IsSuccess());
		}

		/// <summary>
		/// Pipeline setup:
		/// DN0 never responses after received setup request from client.
		/// </summary>
		/// <remarks>
		/// Pipeline setup:
		/// DN0 never responses after received setup request from client.
		/// Client gets an IOException and determine DN0 bad.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_01()
		{
			string methodName = FiTestUtil.GetMethodName();
			RunReceiverOpWriteBlockTest(methodName, 0, new DataTransferTestUtil.SleepAction(methodName
				, 0, 0));
		}

		/// <summary>
		/// Pipeline setup:
		/// DN1 never responses after received setup request from client.
		/// </summary>
		/// <remarks>
		/// Pipeline setup:
		/// DN1 never responses after received setup request from client.
		/// Client gets an IOException and determine DN1 bad.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_02()
		{
			string methodName = FiTestUtil.GetMethodName();
			RunReceiverOpWriteBlockTest(methodName, 1, new DataTransferTestUtil.SleepAction(methodName
				, 1, 0));
		}

		/// <summary>
		/// Pipeline setup:
		/// DN2 never responses after received setup request from client.
		/// </summary>
		/// <remarks>
		/// Pipeline setup:
		/// DN2 never responses after received setup request from client.
		/// Client gets an IOException and determine DN2 bad.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_03()
		{
			string methodName = FiTestUtil.GetMethodName();
			RunReceiverOpWriteBlockTest(methodName, 2, new DataTransferTestUtil.SleepAction(methodName
				, 2, 0));
		}

		/// <summary>Pipeline setup, DN1 never responses after received setup ack from DN2.</summary>
		/// <remarks>
		/// Pipeline setup, DN1 never responses after received setup ack from DN2.
		/// Client gets an IOException and determine DN1 bad.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_04()
		{
			string methodName = FiTestUtil.GetMethodName();
			RunStatusReadTest(methodName, 1, new DataTransferTestUtil.SleepAction(methodName, 
				1, 0));
		}

		/// <summary>Pipeline setup, DN0 never responses after received setup ack from DN1.</summary>
		/// <remarks>
		/// Pipeline setup, DN0 never responses after received setup ack from DN1.
		/// Client gets an IOException and determine DN0 bad.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_05()
		{
			string methodName = FiTestUtil.GetMethodName();
			RunStatusReadTest(methodName, 0, new DataTransferTestUtil.SleepAction(methodName, 
				0, 0));
		}

		/// <summary>Pipeline setup with DN0 very slow but it won't lead to timeout.</summary>
		/// <remarks>
		/// Pipeline setup with DN0 very slow but it won't lead to timeout.
		/// Client finishes setup successfully.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_06()
		{
			string methodName = FiTestUtil.GetMethodName();
			RunSlowDatanodeTest(methodName, new DataTransferTestUtil.SleepAction(methodName, 
				0, 3000));
		}

		/// <summary>Pipeline setup with DN1 very slow but it won't lead to timeout.</summary>
		/// <remarks>
		/// Pipeline setup with DN1 very slow but it won't lead to timeout.
		/// Client finishes setup successfully.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_07()
		{
			string methodName = FiTestUtil.GetMethodName();
			RunSlowDatanodeTest(methodName, new DataTransferTestUtil.SleepAction(methodName, 
				1, 3000));
		}

		/// <summary>Pipeline setup with DN2 very slow but it won't lead to timeout.</summary>
		/// <remarks>
		/// Pipeline setup with DN2 very slow but it won't lead to timeout.
		/// Client finishes setup successfully.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_08()
		{
			string methodName = FiTestUtil.GetMethodName();
			RunSlowDatanodeTest(methodName, new DataTransferTestUtil.SleepAction(methodName, 
				2, 3000));
		}

		/// <summary>
		/// Pipeline setup, DN0 throws an OutOfMemoryException right after it
		/// received a setup request from client.
		/// </summary>
		/// <remarks>
		/// Pipeline setup, DN0 throws an OutOfMemoryException right after it
		/// received a setup request from client.
		/// Client gets an IOException and determine DN0 bad.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_09()
		{
			string methodName = FiTestUtil.GetMethodName();
			RunReceiverOpWriteBlockTest(methodName, 0, new DataTransferTestUtil.OomAction(methodName
				, 0));
		}

		/// <summary>
		/// Pipeline setup, DN1 throws an OutOfMemoryException right after it
		/// received a setup request from DN0.
		/// </summary>
		/// <remarks>
		/// Pipeline setup, DN1 throws an OutOfMemoryException right after it
		/// received a setup request from DN0.
		/// Client gets an IOException and determine DN1 bad.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_10()
		{
			string methodName = FiTestUtil.GetMethodName();
			RunReceiverOpWriteBlockTest(methodName, 1, new DataTransferTestUtil.OomAction(methodName
				, 1));
		}

		/// <summary>
		/// Pipeline setup, DN2 throws an OutOfMemoryException right after it
		/// received a setup request from DN1.
		/// </summary>
		/// <remarks>
		/// Pipeline setup, DN2 throws an OutOfMemoryException right after it
		/// received a setup request from DN1.
		/// Client gets an IOException and determine DN2 bad.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_11()
		{
			string methodName = FiTestUtil.GetMethodName();
			RunReceiverOpWriteBlockTest(methodName, 2, new DataTransferTestUtil.OomAction(methodName
				, 2));
		}

		/// <summary>
		/// Pipeline setup, DN1 throws an OutOfMemoryException right after it
		/// received a setup ack from DN2.
		/// </summary>
		/// <remarks>
		/// Pipeline setup, DN1 throws an OutOfMemoryException right after it
		/// received a setup ack from DN2.
		/// Client gets an IOException and determine DN1 bad.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_12()
		{
			string methodName = FiTestUtil.GetMethodName();
			RunStatusReadTest(methodName, 1, new DataTransferTestUtil.OomAction(methodName, 1
				));
		}

		/// <summary>
		/// Pipeline setup, DN0 throws an OutOfMemoryException right after it
		/// received a setup ack from DN1.
		/// </summary>
		/// <remarks>
		/// Pipeline setup, DN0 throws an OutOfMemoryException right after it
		/// received a setup ack from DN1.
		/// Client gets an IOException and determine DN0 bad.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_13()
		{
			string methodName = FiTestUtil.GetMethodName();
			RunStatusReadTest(methodName, 0, new DataTransferTestUtil.OomAction(methodName, 0
				));
		}

		/// <summary>
		/// Streaming: Write a packet, DN0 throws a DiskOutOfSpaceError
		/// when it writes the data to disk.
		/// </summary>
		/// <remarks>
		/// Streaming: Write a packet, DN0 throws a DiskOutOfSpaceError
		/// when it writes the data to disk.
		/// Client gets an IOException and determine DN0 bad.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_14()
		{
			string methodName = FiTestUtil.GetMethodName();
			RunCallWritePacketToDisk(methodName, 0, new DataTransferTestUtil.DoosAction(methodName
				, 0));
		}

		/// <summary>
		/// Streaming: Write a packet, DN1 throws a DiskOutOfSpaceError
		/// when it writes the data to disk.
		/// </summary>
		/// <remarks>
		/// Streaming: Write a packet, DN1 throws a DiskOutOfSpaceError
		/// when it writes the data to disk.
		/// Client gets an IOException and determine DN1 bad.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_15()
		{
			string methodName = FiTestUtil.GetMethodName();
			RunCallWritePacketToDisk(methodName, 1, new DataTransferTestUtil.DoosAction(methodName
				, 1));
		}

		/// <summary>
		/// Streaming: Write a packet, DN2 throws a DiskOutOfSpaceError
		/// when it writes the data to disk.
		/// </summary>
		/// <remarks>
		/// Streaming: Write a packet, DN2 throws a DiskOutOfSpaceError
		/// when it writes the data to disk.
		/// Client gets an IOException and determine DN2 bad.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_16()
		{
			string methodName = FiTestUtil.GetMethodName();
			RunCallWritePacketToDisk(methodName, 2, new DataTransferTestUtil.DoosAction(methodName
				, 2));
		}
	}
}
