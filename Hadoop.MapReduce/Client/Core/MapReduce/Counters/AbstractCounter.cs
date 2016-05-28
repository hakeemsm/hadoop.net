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
using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Counters
{
	/// <summary>
	/// An abstract counter class to provide common implementation of
	/// the counter interface in both mapred and mapreduce packages.
	/// </summary>
	public abstract class AbstractCounter : Counter
	{
		[Obsolete]
		public virtual void SetDisplayName(string name)
		{
		}

		public override bool Equals(object genericRight)
		{
			lock (this)
			{
				if (genericRight is Counter)
				{
					lock (genericRight)
					{
						Counter right = (Counter)genericRight;
						return GetName().Equals(right.GetName()) && GetDisplayName().Equals(right.GetDisplayName
							()) && GetValue() == right.GetValue();
					}
				}
				return false;
			}
		}

		public override int GetHashCode()
		{
			lock (this)
			{
				return Objects.HashCode(GetName(), GetDisplayName(), GetValue());
			}
		}

		public abstract void ReadFields(DataInput arg1);

		public abstract void Write(DataOutput arg1);

		public abstract string GetDisplayName();

		public abstract string GetName();

		public abstract Counter GetUnderlyingCounter();

		public abstract long GetValue();

		public abstract void Increment(long arg1);

		public abstract void SetValue(long arg1);
	}
}
