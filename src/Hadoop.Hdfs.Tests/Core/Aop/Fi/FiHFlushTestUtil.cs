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
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FI
{
	/// <summary>Helper methods and actions for hflush() fault injection tests</summary>
	public class FiHFlushTestUtil : DataTransferTestUtil
	{
		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public static PipelineTest InitTest()
		{
			return thepipelinetest = new FiHFlushTestUtil.HFlushTest();
		}

		/// <summary>Disk error action for fault injection tests</summary>
		public class DerrAction : DataTransferTestUtil.DataNodeAction
		{
			/// <param name="currentTest">The name of the test</param>
			/// <param name="index">The index of the datanode</param>
			public DerrAction(string currentTest, int index)
				: base(currentTest, index)
			{
			}

			/// <summary>
			/// <inheritDoc/>
			/// 
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			public override void Run(DatanodeID id)
			{
				Pipeline p = GetPipelineTest().GetPipelineForDatanode(id);
				if (p == null)
				{
					return;
				}
				if (p.Contains(index, id))
				{
					string s = base.ToString(id);
					FiTestUtil.Log.Info(s);
					throw new DiskChecker.DiskErrorException(s);
				}
			}
		}

		/// <summary>Class adds new type of action</summary>
		public class HFlushTest : DataTransferTestUtil.DataTransferTest
		{
			public readonly FiTestUtil.ActionContainer<DatanodeID, IOException> fiCallHFlush = 
				new FiTestUtil.ActionContainer<DatanodeID, IOException>();

			public readonly FiTestUtil.ActionContainer<int, RuntimeException> fiErrorOnCallHFlush
				 = new FiTestUtil.ActionContainer<int, RuntimeException>();
		}
	}
}
