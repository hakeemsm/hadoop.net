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
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.FI;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class PipelinesTestUtil : DataTransferTestUtil
	{
		/// <summary><inheritDoc/></summary>
		public static PipelineTest InitTest()
		{
			return thepipelinetest = new PipelinesTestUtil.PipelinesTest();
		}

		/// <summary>Storing acknowleged bytes num.</summary>
		/// <remarks>Storing acknowleged bytes num. action for fault injection tests</remarks>
		public class ReceivedCheckAction : FiTestUtil.Action<PipelinesTestUtil.NodeBytes, 
			IOException>
		{
			internal string name;

			internal List<PipelinesTestUtil.NodeBytes> rcv = ((PipelinesTestUtil.PipelinesTest
				)GetPipelineTest()).received;

			internal List<PipelinesTestUtil.NodeBytes> ack = ((PipelinesTestUtil.PipelinesTest
				)GetPipelineTest()).acked;

			/// <param name="name">of the test</param>
			public ReceivedCheckAction(string name)
			{
				this.name = name;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Run(PipelinesTestUtil.NodeBytes nb)
			{
				lock (rcv)
				{
					rcv.AddItem(nb);
					foreach (PipelinesTestUtil.NodeBytes n in rcv)
					{
						long counterPartsBytes = -1;
						PipelinesTestUtil.NodeBytes counterPart = null;
						if (ack.Count > rcv.IndexOf(n))
						{
							counterPart = ack[rcv.IndexOf(n)];
							counterPartsBytes = counterPart.bytes;
						}
						NUnit.Framework.Assert.IsTrue("FI: Wrong receiving length", counterPartsBytes <= 
							n.bytes);
						if (FiTestUtil.Log.IsDebugEnabled())
						{
							FiTestUtil.Log.Debug("FI: before compare of Recv bytes. Expected " + n.bytes + ", got "
								 + counterPartsBytes);
						}
					}
				}
			}
		}

		/// <summary>Storing acknowleged bytes num.</summary>
		/// <remarks>Storing acknowleged bytes num. action for fault injection tests</remarks>
		public class AckedCheckAction : FiTestUtil.Action<PipelinesTestUtil.NodeBytes, IOException
			>
		{
			internal string name;

			internal List<PipelinesTestUtil.NodeBytes> rcv = ((PipelinesTestUtil.PipelinesTest
				)GetPipelineTest()).received;

			internal List<PipelinesTestUtil.NodeBytes> ack = ((PipelinesTestUtil.PipelinesTest
				)GetPipelineTest()).acked;

			/// <param name="name">of the test</param>
			public AckedCheckAction(string name)
			{
				this.name = name;
			}

			/// <summary><inheritDoc/></summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Run(PipelinesTestUtil.NodeBytes nb)
			{
				lock (ack)
				{
					ack.AddItem(nb);
					foreach (PipelinesTestUtil.NodeBytes n in ack)
					{
						PipelinesTestUtil.NodeBytes counterPart = null;
						long counterPartsBytes = -1;
						if (rcv.Count > ack.IndexOf(n))
						{
							counterPart = rcv[ack.IndexOf(n)];
							counterPartsBytes = counterPart.bytes;
						}
						NUnit.Framework.Assert.IsTrue("FI: Wrong acknowledged length", counterPartsBytes 
							== n.bytes);
						if (FiTestUtil.Log.IsDebugEnabled())
						{
							FiTestUtil.Log.Debug("FI: before compare of Acked bytes. Expected " + n.bytes + ", got "
								 + counterPartsBytes);
						}
					}
				}
			}
		}

		/// <summary>Class adds new types of action</summary>
		public class PipelinesTest : DataTransferTestUtil.DataTransferTest
		{
			internal List<PipelinesTestUtil.NodeBytes> received = new List<PipelinesTestUtil.NodeBytes
				>();

			internal List<PipelinesTestUtil.NodeBytes> acked = new List<PipelinesTestUtil.NodeBytes
				>();

			public readonly FiTestUtil.ActionContainer<PipelinesTestUtil.NodeBytes, IOException
				> fiCallSetNumBytes = new FiTestUtil.ActionContainer<PipelinesTestUtil.NodeBytes
				, IOException>();

			public readonly FiTestUtil.ActionContainer<PipelinesTestUtil.NodeBytes, IOException
				> fiCallSetBytesAcked = new FiTestUtil.ActionContainer<PipelinesTestUtil.NodeBytes
				, IOException>();

			private static bool suspend = false;

			private static long lastQueuedPacket = -1;

			public virtual void SetSuspend(bool flag)
			{
				suspend = flag;
			}

			public virtual bool GetSuspend()
			{
				return suspend;
			}

			public virtual void SetVerified(long packetNum)
			{
				PipelinesTestUtil.PipelinesTest.lastQueuedPacket = packetNum;
			}

			public virtual long GetLastQueued()
			{
				return lastQueuedPacket;
			}
		}

		public class NodeBytes
		{
			internal DatanodeID id;

			internal long bytes;

			public NodeBytes(DatanodeID id, long bytes)
			{
				this.id = id;
				this.bytes = bytes;
			}
		}
	}
}
