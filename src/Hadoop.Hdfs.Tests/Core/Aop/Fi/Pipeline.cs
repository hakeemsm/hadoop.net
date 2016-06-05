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
using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.FI
{
	public class Pipeline
	{
		private readonly IList<string> datanodes = new AList<string>();

		internal Pipeline(LocatedBlock lb)
		{
			foreach (DatanodeInfo d in lb.GetLocations())
			{
				datanodes.AddItem(d.GetName());
			}
		}

		/// <summary>Does the pipeline contains d?</summary>
		public virtual bool Contains(DatanodeID d)
		{
			//return datanodes.contains(d.getName());
			//HM: code above commented out since getName doesnt exit on DataNodeId
			return false;
		}

		/// <summary>Does the pipeline contains d at the n th position?</summary>
		public virtual bool Contains(int n, DatanodeID d)
		{
			//return d.getName().equals(datanodes.get(n));
			//HM: code above commented out since getName doesnt exit on DataNodeId
			return false;
		}

		public override string ToString()
		{
			return GetType().Name + datanodes;
		}
	}
}
