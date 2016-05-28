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
using Org.Apache.Hadoop.FI;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>Test DataTransferProtocol with fault injection.</summary>
	public class TestFiPipelineClose
	{
		/// <exception cref="System.IO.IOException"/>
		private static void RunPipelineCloseTest(string methodName, FiTestUtil.Action<DatanodeID
			, IOException> a)
		{
			FiTestUtil.Log.Info("Running " + methodName + " ...");
			DataTransferTestUtil.DataTransferTest t = (DataTransferTestUtil.DataTransferTest)
				DataTransferTestUtil.InitTest();
			t.fiPipelineClose.Set(a);
			TestFiDataTransferProtocol.Write1byte(methodName);
		}

		/// <summary>
		/// Pipeline close:
		/// DN0 never responses after received close request from client.
		/// </summary>
		/// <remarks>
		/// Pipeline close:
		/// DN0 never responses after received close request from client.
		/// Client gets an IOException and determine DN0 bad.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_36()
		{
			string methodName = FiTestUtil.GetMethodName();
			RunPipelineCloseTest(methodName, new DataTransferTestUtil.SleepAction(methodName, 
				0, 0));
		}

		/// <summary>
		/// Pipeline close:
		/// DN1 never responses after received close request from client.
		/// </summary>
		/// <remarks>
		/// Pipeline close:
		/// DN1 never responses after received close request from client.
		/// Client gets an IOException and determine DN1 bad.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_37()
		{
			string methodName = FiTestUtil.GetMethodName();
			RunPipelineCloseTest(methodName, new DataTransferTestUtil.SleepAction(methodName, 
				1, 0));
		}

		/// <summary>
		/// Pipeline close:
		/// DN2 never responses after received close request from client.
		/// </summary>
		/// <remarks>
		/// Pipeline close:
		/// DN2 never responses after received close request from client.
		/// Client gets an IOException and determine DN2 bad.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_38()
		{
			string methodName = FiTestUtil.GetMethodName();
			RunPipelineCloseTest(methodName, new DataTransferTestUtil.SleepAction(methodName, 
				2, 0));
		}

		/// <exception cref="System.IO.IOException"/>
		private static void Run41_43(string name, int i)
		{
			RunPipelineCloseTest(name, new DataTransferTestUtil.SleepAction(name, i, 3000));
		}

		/// <exception cref="System.IO.IOException"/>
		private static void RunPipelineCloseAck(string name, int i, DataTransferTestUtil.DataNodeAction
			 a)
		{
			FiTestUtil.Log.Info("Running " + name + " ...");
			DataTransferTestUtil.DataTransferTest t = (DataTransferTestUtil.DataTransferTest)
				DataTransferTestUtil.InitTest();
			FiTestUtil.MarkerConstraint marker = new FiTestUtil.MarkerConstraint(name);
			t.fiPipelineClose.Set(new DataTransferTestUtil.DatanodeMarkingAction(name, i, marker
				));
			t.fiPipelineAck.Set(new FiTestUtil.ConstraintSatisfactionAction<DatanodeID, IOException
				>(a, marker));
			TestFiDataTransferProtocol.Write1byte(name);
		}

		/// <exception cref="System.IO.IOException"/>
		private static void Run39_40(string name, int i)
		{
			RunPipelineCloseAck(name, i, new DataTransferTestUtil.SleepAction(name, i, 0));
		}

		/// <summary>
		/// Pipeline close:
		/// DN1 never responses after received close ack DN2.
		/// </summary>
		/// <remarks>
		/// Pipeline close:
		/// DN1 never responses after received close ack DN2.
		/// Client gets an IOException and determine DN1 bad.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_39()
		{
			Run39_40(FiTestUtil.GetMethodName(), 1);
		}

		/// <summary>
		/// Pipeline close:
		/// DN0 never responses after received close ack DN1.
		/// </summary>
		/// <remarks>
		/// Pipeline close:
		/// DN0 never responses after received close ack DN1.
		/// Client gets an IOException and determine DN0 bad.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_40()
		{
			Run39_40(FiTestUtil.GetMethodName(), 0);
		}

		/// <summary>Pipeline close with DN0 very slow but it won't lead to timeout.</summary>
		/// <remarks>
		/// Pipeline close with DN0 very slow but it won't lead to timeout.
		/// Client finishes close successfully.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_41()
		{
			Run41_43(FiTestUtil.GetMethodName(), 0);
		}

		/// <summary>Pipeline close with DN1 very slow but it won't lead to timeout.</summary>
		/// <remarks>
		/// Pipeline close with DN1 very slow but it won't lead to timeout.
		/// Client finishes close successfully.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_42()
		{
			Run41_43(FiTestUtil.GetMethodName(), 1);
		}

		/// <summary>Pipeline close with DN2 very slow but it won't lead to timeout.</summary>
		/// <remarks>
		/// Pipeline close with DN2 very slow but it won't lead to timeout.
		/// Client finishes close successfully.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_43()
		{
			Run41_43(FiTestUtil.GetMethodName(), 2);
		}

		/// <summary>
		/// Pipeline close:
		/// DN0 throws an OutOfMemoryException
		/// right after it received a close request from client.
		/// </summary>
		/// <remarks>
		/// Pipeline close:
		/// DN0 throws an OutOfMemoryException
		/// right after it received a close request from client.
		/// Client gets an IOException and determine DN0 bad.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_44()
		{
			string methodName = FiTestUtil.GetMethodName();
			RunPipelineCloseTest(methodName, new DataTransferTestUtil.OomAction(methodName, 0
				));
		}

		/// <summary>
		/// Pipeline close:
		/// DN1 throws an OutOfMemoryException
		/// right after it received a close request from client.
		/// </summary>
		/// <remarks>
		/// Pipeline close:
		/// DN1 throws an OutOfMemoryException
		/// right after it received a close request from client.
		/// Client gets an IOException and determine DN1 bad.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_45()
		{
			string methodName = FiTestUtil.GetMethodName();
			RunPipelineCloseTest(methodName, new DataTransferTestUtil.OomAction(methodName, 1
				));
		}

		/// <summary>
		/// Pipeline close:
		/// DN2 throws an OutOfMemoryException
		/// right after it received a close request from client.
		/// </summary>
		/// <remarks>
		/// Pipeline close:
		/// DN2 throws an OutOfMemoryException
		/// right after it received a close request from client.
		/// Client gets an IOException and determine DN2 bad.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_46()
		{
			string methodName = FiTestUtil.GetMethodName();
			RunPipelineCloseTest(methodName, new DataTransferTestUtil.OomAction(methodName, 2
				));
		}

		/// <exception cref="System.IO.IOException"/>
		private static void Run47_48(string name, int i)
		{
			RunPipelineCloseAck(name, i, new DataTransferTestUtil.OomAction(name, i));
		}

		/// <summary>
		/// Pipeline close:
		/// DN1 throws an OutOfMemoryException right after
		/// it received a close ack from DN2.
		/// </summary>
		/// <remarks>
		/// Pipeline close:
		/// DN1 throws an OutOfMemoryException right after
		/// it received a close ack from DN2.
		/// Client gets an IOException and determine DN1 bad.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_47()
		{
			Run47_48(FiTestUtil.GetMethodName(), 1);
		}

		/// <summary>
		/// Pipeline close:
		/// DN0 throws an OutOfMemoryException right after
		/// it received a close ack from DN1.
		/// </summary>
		/// <remarks>
		/// Pipeline close:
		/// DN0 throws an OutOfMemoryException right after
		/// it received a close ack from DN1.
		/// Client gets an IOException and determine DN0 bad.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_48()
		{
			Run47_48(FiTestUtil.GetMethodName(), 0);
		}

		/// <exception cref="System.IO.IOException"/>
		private static void RunBlockFileCloseTest(string methodName, FiTestUtil.Action<DatanodeID
			, IOException> a)
		{
			FiTestUtil.Log.Info("Running " + methodName + " ...");
			DataTransferTestUtil.DataTransferTest t = (DataTransferTestUtil.DataTransferTest)
				DataTransferTestUtil.InitTest();
			t.fiBlockFileClose.Set(a);
			TestFiDataTransferProtocol.Write1byte(methodName);
		}

		/// <exception cref="System.IO.IOException"/>
		private static void Run49_51(string name, int i)
		{
			RunBlockFileCloseTest(name, new DataTransferTestUtil.IoeAction(name, i, "DISK ERROR"
				));
		}

		/// <summary>
		/// Pipeline close:
		/// DN0 throws a disk error exception when it is closing the block file.
		/// </summary>
		/// <remarks>
		/// Pipeline close:
		/// DN0 throws a disk error exception when it is closing the block file.
		/// Client gets an IOException and determine DN0 bad.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_49()
		{
			Run49_51(FiTestUtil.GetMethodName(), 0);
		}

		/// <summary>
		/// Pipeline close:
		/// DN1 throws a disk error exception when it is closing the block file.
		/// </summary>
		/// <remarks>
		/// Pipeline close:
		/// DN1 throws a disk error exception when it is closing the block file.
		/// Client gets an IOException and determine DN1 bad.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_50()
		{
			Run49_51(FiTestUtil.GetMethodName(), 1);
		}

		/// <summary>
		/// Pipeline close:
		/// DN2 throws a disk error exception when it is closing the block file.
		/// </summary>
		/// <remarks>
		/// Pipeline close:
		/// DN2 throws a disk error exception when it is closing the block file.
		/// Client gets an IOException and determine DN2 bad.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Pipeline_Fi_51()
		{
			Run49_51(FiTestUtil.GetMethodName(), 2);
		}
	}
}
