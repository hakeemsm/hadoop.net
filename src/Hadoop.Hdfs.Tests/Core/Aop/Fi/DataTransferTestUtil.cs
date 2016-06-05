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
using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FI
{
	/// <summary>
	/// Utilities for DataTransferProtocol related tests,
	/// e.g.
	/// </summary>
	/// <remarks>
	/// Utilities for DataTransferProtocol related tests,
	/// e.g. TestFiDataTransferProtocol.
	/// </remarks>
	public class DataTransferTestUtil
	{
		protected internal static PipelineTest thepipelinetest;

		/// <summary>initialize pipeline test</summary>
		public static PipelineTest InitTest()
		{
			return thepipelinetest = new DataTransferTestUtil.DataTransferTest();
		}

		/// <summary>get the pipeline test object</summary>
		public static PipelineTest GetPipelineTest()
		{
			return thepipelinetest;
		}

		/// <summary>get the pipeline test object cast to DataTransferTest</summary>
		public static DataTransferTestUtil.DataTransferTest GetDataTransferTest()
		{
			return (DataTransferTestUtil.DataTransferTest)GetPipelineTest();
		}

		/// <summary>
		/// The DataTransferTest class includes a pipeline
		/// and some actions.
		/// </summary>
		public class DataTransferTest : PipelineTest
		{
			private readonly IList<Pipeline> pipelines = new AList<Pipeline>();

			private volatile bool isSuccess = false;

			/// <summary>Simulate action for the receiverOpWriteBlock pointcut</summary>
			public readonly FiTestUtil.ActionContainer<DatanodeID, IOException> fiReceiverOpWriteBlock
				 = new FiTestUtil.ActionContainer<DatanodeID, IOException>();

			/// <summary>Simulate action for the callReceivePacket pointcut</summary>
			public readonly FiTestUtil.ActionContainer<DatanodeID, IOException> fiCallReceivePacket
				 = new FiTestUtil.ActionContainer<DatanodeID, IOException>();

			/// <summary>Simulate action for the callWritePacketToDisk pointcut</summary>
			public readonly FiTestUtil.ActionContainer<DatanodeID, IOException> fiCallWritePacketToDisk
				 = new FiTestUtil.ActionContainer<DatanodeID, IOException>();

			/// <summary>Simulate action for the statusRead pointcut</summary>
			public readonly FiTestUtil.ActionContainer<DatanodeID, IOException> fiStatusRead = 
				new FiTestUtil.ActionContainer<DatanodeID, IOException>();

			/// <summary>Simulate action for the afterDownstreamStatusRead pointcut</summary>
			public readonly FiTestUtil.ActionContainer<DatanodeID, IOException> fiAfterDownstreamStatusRead
				 = new FiTestUtil.ActionContainer<DatanodeID, IOException>();

			/// <summary>Simulate action for the pipelineAck pointcut</summary>
			public readonly FiTestUtil.ActionContainer<DatanodeID, IOException> fiPipelineAck
				 = new FiTestUtil.ActionContainer<DatanodeID, IOException>();

			/// <summary>Simulate action for the pipelineClose pointcut</summary>
			public readonly FiTestUtil.ActionContainer<DatanodeID, IOException> fiPipelineClose
				 = new FiTestUtil.ActionContainer<DatanodeID, IOException>();

			/// <summary>Simulate action for the blockFileClose pointcut</summary>
			public readonly FiTestUtil.ActionContainer<DatanodeID, IOException> fiBlockFileClose
				 = new FiTestUtil.ActionContainer<DatanodeID, IOException>();

			/// <summary>Verification action for the pipelineInitNonAppend pointcut</summary>
			public readonly FiTestUtil.ActionContainer<int, RuntimeException> fiPipelineInitErrorNonAppend
				 = new FiTestUtil.ActionContainer<int, RuntimeException>();

			/// <summary>Verification action for the pipelineErrorAfterInit pointcut</summary>
			public readonly FiTestUtil.ActionContainer<int, RuntimeException> fiPipelineErrorAfterInit
				 = new FiTestUtil.ActionContainer<int, RuntimeException>();

			/// <summary>Get test status</summary>
			public virtual bool IsSuccess()
			{
				return this.isSuccess;
			}

			/// <summary>Set test status</summary>
			public virtual void MarkSuccess()
			{
				this.isSuccess = true;
			}

			/// <summary>Initialize the pipeline.</summary>
			public virtual Pipeline InitPipeline(LocatedBlock lb)
			{
				lock (this)
				{
					Pipeline pl = new Pipeline(lb);
					if (pipelines.Contains(pl))
					{
						throw new InvalidOperationException("thepipeline != null");
					}
					pipelines.AddItem(pl);
					return pl;
				}
			}

			/// <summary>Return the pipeline for the datanode.</summary>
			public virtual Pipeline GetPipelineForDatanode(DatanodeID id)
			{
				lock (this)
				{
					foreach (Pipeline p in pipelines)
					{
						if (p.Contains(id))
						{
							return p;
						}
					}
					FiTestUtil.Log.Info("FI: pipeline not found; id=" + id + ", pipelines=" + pipelines
						);
					return null;
				}
			}

			/// <summary>
			/// Is the test not yet success
			/// and the last pipeline contains the given datanode?
			/// </summary>
			private bool IsNotSuccessAndLastPipelineContains(int index, DatanodeID id)
			{
				lock (this)
				{
					if (IsSuccess())
					{
						return false;
					}
					int n = pipelines.Count;
					return n == 0 ? false : pipelines[n - 1].Contains(index, id);
				}
			}
		}

		/// <summary>Action for DataNode</summary>
		public abstract class DataNodeAction : FiTestUtil.Action<DatanodeID, IOException>
		{
			/// <summary>The name of the test</summary>
			internal readonly string currentTest;

			/// <summary>The index of the datanode</summary>
			internal readonly int index;

			/// <param name="currentTest">The name of the test</param>
			/// <param name="index">The index of the datanode</param>
			protected internal DataNodeAction(string currentTest, int index)
			{
				this.currentTest = currentTest;
				this.index = index;
			}

			/// <summary>
			/// <inheritDoc/>
			/// 
			/// </summary>
			public override string ToString()
			{
				return GetType().Name + ":" + currentTest + ", index=" + index;
			}

			/// <summary>return a String with this object and the datanodeID.</summary>
			internal virtual string ToString(DatanodeID datanodeID)
			{
				return "FI: " + this + ", datanode=";
			}

			public abstract void Run(DatanodeID arg1);
			//HM: code below part of above line but commented out since getName doesnt exit on DataNodeId
			//+ datanodeID.getName();
		}

		/// <summary>An action to set a marker if the DatanodeID is matched.</summary>
		public class DatanodeMarkingAction : DataTransferTestUtil.DataNodeAction
		{
			private readonly FiTestUtil.MarkerConstraint marker;

			/// <summary>Construct an object.</summary>
			public DatanodeMarkingAction(string currentTest, int index, FiTestUtil.MarkerConstraint
				 marker)
				: base(currentTest, index)
			{
				this.marker = marker;
			}

			/// <summary>Set the marker if the DatanodeID is matched.</summary>
			/// <exception cref="System.IO.IOException"/>
			public override void Run(DatanodeID datanodeid)
			{
				DataTransferTestUtil.DataTransferTest test = GetDataTransferTest();
				if (test.IsNotSuccessAndLastPipelineContains(index, datanodeid))
				{
					marker.Mark();
				}
			}

			/// <summary>
			/// <inheritDoc/>
			/// 
			/// </summary>
			public override string ToString()
			{
				return base.ToString() + ", " + marker;
			}
		}

		/// <summary>Throws OutOfMemoryError.</summary>
		public class OomAction : DataTransferTestUtil.DataNodeAction
		{
			/// <summary>Create an action for datanode i in the pipeline.</summary>
			public OomAction(string currentTest, int i)
				: base(currentTest, i)
			{
			}

			public override void Run(DatanodeID id)
			{
				DataTransferTestUtil.DataTransferTest test = GetDataTransferTest();
				if (test.IsNotSuccessAndLastPipelineContains(index, id))
				{
					string s = ToString(id);
					FiTestUtil.Log.Info(s);
					throw new OutOfMemoryException(s);
				}
			}
		}

		/// <summary>Throws OutOfMemoryError if the count is zero.</summary>
		public class CountdownOomAction : DataTransferTestUtil.OomAction
		{
			private readonly FiTestUtil.CountdownConstraint countdown;

			/// <summary>Create an action for datanode i in the pipeline with count down.</summary>
			public CountdownOomAction(string currentTest, int i, int count)
				: base(currentTest, i)
			{
				countdown = new FiTestUtil.CountdownConstraint(count);
			}

			public override void Run(DatanodeID id)
			{
				DataTransferTestUtil.DataTransferTest test = GetDataTransferTest();
				if (test.IsNotSuccessAndLastPipelineContains(index, id) && countdown.IsSatisfied(
					))
				{
					string s = ToString(id);
					FiTestUtil.Log.Info(s);
					throw new OutOfMemoryException(s);
				}
			}
		}

		/// <summary>Throws DiskOutOfSpaceException.</summary>
		public class DoosAction : DataTransferTestUtil.DataNodeAction
		{
			/// <summary>Create an action for datanode i in the pipeline.</summary>
			public DoosAction(string currentTest, int i)
				: base(currentTest, i)
			{
			}

			/// <exception cref="Org.Apache.Hadoop.Util.DiskChecker.DiskOutOfSpaceException"/>
			public override void Run(DatanodeID id)
			{
				DataTransferTestUtil.DataTransferTest test = GetDataTransferTest();
				if (test.IsNotSuccessAndLastPipelineContains(index, id))
				{
					string s = ToString(id);
					FiTestUtil.Log.Info(s);
					throw new DiskChecker.DiskOutOfSpaceException(s);
				}
			}
		}

		/// <summary>Throws an IOException.</summary>
		public class IoeAction : DataTransferTestUtil.DataNodeAction
		{
			private readonly string error;

			/// <summary>Create an action for datanode i in the pipeline.</summary>
			public IoeAction(string currentTest, int i, string error)
				: base(currentTest, i)
			{
				this.error = error;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Run(DatanodeID id)
			{
				DataTransferTestUtil.DataTransferTest test = GetDataTransferTest();
				if (test.IsNotSuccessAndLastPipelineContains(index, id))
				{
					string s = ToString(id);
					FiTestUtil.Log.Info(s);
					throw new IOException(s);
				}
			}

			public override string ToString()
			{
				return error + " " + base.ToString();
			}
		}

		/// <summary>Throws DiskOutOfSpaceException if the count is zero.</summary>
		public class CountdownDoosAction : DataTransferTestUtil.DoosAction
		{
			private readonly FiTestUtil.CountdownConstraint countdown;

			/// <summary>Create an action for datanode i in the pipeline with count down.</summary>
			public CountdownDoosAction(string currentTest, int i, int count)
				: base(currentTest, i)
			{
				countdown = new FiTestUtil.CountdownConstraint(count);
			}

			/// <exception cref="Org.Apache.Hadoop.Util.DiskChecker.DiskOutOfSpaceException"/>
			public override void Run(DatanodeID id)
			{
				DataTransferTestUtil.DataTransferTest test = GetDataTransferTest();
				if (test.IsNotSuccessAndLastPipelineContains(index, id) && countdown.IsSatisfied(
					))
				{
					string s = ToString(id);
					FiTestUtil.Log.Info(s);
					throw new DiskChecker.DiskOutOfSpaceException(s);
				}
			}
		}

		/// <summary>
		/// Sleep some period of time so that it slows down the datanode
		/// or sleep forever so that datanode becomes not responding.
		/// </summary>
		public class SleepAction : DataTransferTestUtil.DataNodeAction
		{
			/// <summary>
			/// In milliseconds;
			/// must have (0 &lt;= minDuration &lt; maxDuration) or (maxDuration &lt;= 0).
			/// </summary>
			internal readonly long minDuration;

			/// <summary>In milliseconds; maxDuration &lt;= 0 means sleeping forever.</summary>
			internal readonly long maxDuration;

			/// <summary>Create an action for datanode i in the pipeline.</summary>
			/// <param name="duration">In milliseconds, duration &lt;= 0 means sleeping forever.</param>
			public SleepAction(string currentTest, int i, long duration)
				: this(currentTest, i, duration, duration <= 0 ? duration : duration + 1)
			{
			}

			/// <summary>Create an action for datanode i in the pipeline.</summary>
			/// <param name="minDuration">minimum sleep time</param>
			/// <param name="maxDuration">maximum sleep time</param>
			public SleepAction(string currentTest, int i, long minDuration, long maxDuration)
				: base(currentTest, i)
			{
				if (maxDuration > 0)
				{
					if (minDuration < 0)
					{
						throw new ArgumentException("minDuration = " + minDuration + " < 0 but maxDuration = "
							 + maxDuration + " > 0");
					}
					if (minDuration >= maxDuration)
					{
						throw new ArgumentException(minDuration + " = minDuration >= maxDuration = " + maxDuration
							);
					}
				}
				this.minDuration = minDuration;
				this.maxDuration = maxDuration;
			}

			public override void Run(DatanodeID id)
			{
				DataTransferTestUtil.DataTransferTest test = GetDataTransferTest();
				if (test.IsNotSuccessAndLastPipelineContains(index, id))
				{
					FiTestUtil.Log.Info(ToString(id));
					if (maxDuration <= 0)
					{
						for (; FiTestUtil.Sleep(1000); )
						{
						}
					}
					else
					{
						//sleep forever until interrupt
						FiTestUtil.Sleep(minDuration, maxDuration);
					}
				}
			}

			/// <summary>
			/// <inheritDoc/>
			/// 
			/// </summary>
			public override string ToString()
			{
				return base.ToString() + ", duration=" + (maxDuration <= 0 ? "infinity" : "[" + minDuration
					 + ", " + maxDuration + ")");
			}
		}

		/// <summary>
		/// When the count is zero,
		/// sleep some period of time so that it slows down the datanode
		/// or sleep forever so that datanode becomes not responding.
		/// </summary>
		public class CountdownSleepAction : DataTransferTestUtil.SleepAction
		{
			private readonly FiTestUtil.CountdownConstraint countdown;

			/// <summary>Create an action for datanode i in the pipeline.</summary>
			/// <param name="duration">In milliseconds, duration &lt;= 0 means sleeping forever.</param>
			public CountdownSleepAction(string currentTest, int i, long duration, int count)
				: this(currentTest, i, duration, duration + 1, count)
			{
			}

			/// <summary>Create an action for datanode i in the pipeline with count down.</summary>
			public CountdownSleepAction(string currentTest, int i, long minDuration, long maxDuration
				, int count)
				: base(currentTest, i, minDuration, maxDuration)
			{
				countdown = new FiTestUtil.CountdownConstraint(count);
			}

			public override void Run(DatanodeID id)
			{
				DataTransferTestUtil.DataTransferTest test = GetDataTransferTest();
				if (test.IsNotSuccessAndLastPipelineContains(index, id) && countdown.IsSatisfied(
					))
				{
					string s = ToString(id) + ", duration = [" + minDuration + "," + maxDuration + ")";
					FiTestUtil.Log.Info(s);
					if (maxDuration <= 1)
					{
						for (; FiTestUtil.Sleep(1000); )
						{
						}
					}
					else
					{
						//sleep forever until interrupt
						FiTestUtil.Sleep(minDuration, maxDuration);
					}
				}
			}
		}

		/// <summary>Action for pipeline error verification</summary>
		public class VerificationAction : FiTestUtil.Action<int, RuntimeException>
		{
			/// <summary>The name of the test</summary>
			internal readonly string currentTest;

			/// <summary>The error index of the datanode</summary>
			internal readonly int errorIndex;

			/// <summary>Create a verification action for errors at datanode i in the pipeline.</summary>
			/// <param name="currentTest">The name of the test</param>
			/// <param name="i">The error index of the datanode</param>
			public VerificationAction(string currentTest, int i)
			{
				this.currentTest = currentTest;
				this.errorIndex = i;
			}

			/// <summary>
			/// <inheritDoc/>
			/// 
			/// </summary>
			public override string ToString()
			{
				return currentTest + ", errorIndex=" + errorIndex;
			}

			public virtual void Run(int i)
			{
				if (i == errorIndex)
				{
					FiTestUtil.Log.Info(this + ", successfully verified.");
					GetDataTransferTest().MarkSuccess();
				}
			}
		}

		/// <summary>
		/// Create a OomAction with a CountdownConstraint
		/// so that it throws OutOfMemoryError if the count is zero.
		/// </summary>
		public static FiTestUtil.ConstraintSatisfactionAction<DatanodeID, IOException> CreateCountdownOomAction
			(string currentTest, int i, int count)
		{
			return new FiTestUtil.ConstraintSatisfactionAction<DatanodeID, IOException>(new DataTransferTestUtil.OomAction
				(currentTest, i), new FiTestUtil.CountdownConstraint(count));
		}

		/// <summary>
		/// Create a DoosAction with a CountdownConstraint
		/// so that it throws DiskOutOfSpaceException if the count is zero.
		/// </summary>
		public static FiTestUtil.ConstraintSatisfactionAction<DatanodeID, IOException> CreateCountdownDoosAction
			(string currentTest, int i, int count)
		{
			return new FiTestUtil.ConstraintSatisfactionAction<DatanodeID, IOException>(new DataTransferTestUtil.DoosAction
				(currentTest, i), new FiTestUtil.CountdownConstraint(count));
		}

		/// <summary>
		/// Create a SleepAction with a CountdownConstraint
		/// for datanode i in the pipeline.
		/// </summary>
		/// <remarks>
		/// Create a SleepAction with a CountdownConstraint
		/// for datanode i in the pipeline.
		/// When the count is zero,
		/// sleep some period of time so that it slows down the datanode
		/// or sleep forever so the that datanode becomes not responding.
		/// </remarks>
		public static FiTestUtil.ConstraintSatisfactionAction<DatanodeID, IOException> CreateCountdownSleepAction
			(string currentTest, int i, long minDuration, long maxDuration, int count)
		{
			return new FiTestUtil.ConstraintSatisfactionAction<DatanodeID, IOException>(new DataTransferTestUtil.SleepAction
				(currentTest, i, minDuration, maxDuration), new FiTestUtil.CountdownConstraint(count
				));
		}

		/// <summary>
		/// Same as
		/// createCountdownSleepAction(currentTest, i, duration, duration+1, count).
		/// </summary>
		public static FiTestUtil.ConstraintSatisfactionAction<DatanodeID, IOException> CreateCountdownSleepAction
			(string currentTest, int i, long duration, int count)
		{
			return CreateCountdownSleepAction(currentTest, i, duration, duration + 1, count);
		}
	}
}
