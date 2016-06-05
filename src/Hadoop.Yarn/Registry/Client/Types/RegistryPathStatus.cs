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
using System.Text;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Client.Types
{
	/// <summary>Output of a <code>RegistryOperations.stat()</code> call</summary>
	public sealed class RegistryPathStatus
	{
		/// <summary>Short path in the registry to this entry</summary>
		public readonly string path;

		/// <summary>Timestamp</summary>
		public readonly long time;

		/// <summary>Entry size in bytes, as returned by the storage infrastructure.</summary>
		/// <remarks>
		/// Entry size in bytes, as returned by the storage infrastructure.
		/// In zookeeper, even "empty" nodes have a non-zero size.
		/// </remarks>
		public readonly long size;

		/// <summary>Number of child nodes</summary>
		public readonly int children;

		/// <summary>Construct an instance</summary>
		/// <param name="path">full path</param>
		/// <param name="time">time</param>
		/// <param name="size">entry size</param>
		/// <param name="children">number of children</param>
		public RegistryPathStatus(string path, long time, long size, int children)
		{
			this.path = path;
			this.time = time;
			this.size = size;
			this.children = children;
		}

		/// <summary>Equality operator checks size, time and path of the entries.</summary>
		/// <remarks>
		/// Equality operator checks size, time and path of the entries.
		/// It does <i>not</i> check
		/// <see cref="children"/>
		/// .
		/// </remarks>
		/// <param name="other">the other entry</param>
		/// <returns>true if the entries are considered equal.</returns>
		public override bool Equals(object other)
		{
			if (this == other)
			{
				return true;
			}
			if (other == null || GetType() != other.GetType())
			{
				return false;
			}
			Org.Apache.Hadoop.Registry.Client.Types.RegistryPathStatus status = (Org.Apache.Hadoop.Registry.Client.Types.RegistryPathStatus
				)other;
			if (size != status.size)
			{
				return false;
			}
			if (time != status.time)
			{
				return false;
			}
			if (path != null ? !path.Equals(status.path) : status.path != null)
			{
				return false;
			}
			return true;
		}

		/// <summary>The hash code is derived from the path.</summary>
		/// <returns>hash code for storing the path in maps.</returns>
		public override int GetHashCode()
		{
			return path != null ? path.GetHashCode() : 0;
		}

		public override string ToString()
		{
			StringBuilder sb = new StringBuilder("RegistryPathStatus{");
			sb.Append("path='").Append(path).Append('\'');
			sb.Append(", time=").Append(time);
			sb.Append(", size=").Append(size);
			sb.Append(", children=").Append(children);
			sb.Append('}');
			return sb.ToString();
		}
	}
}
