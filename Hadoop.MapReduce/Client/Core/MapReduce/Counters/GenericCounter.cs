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
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Counters
{
	/// <summary>A generic counter implementation</summary>
	public class GenericCounter : AbstractCounter
	{
		private string name;

		private string displayName;

		private long value = 0;

		public GenericCounter()
		{
		}

		public GenericCounter(string name, string displayName)
		{
			// mostly for readFields
			this.name = name;
			this.displayName = displayName;
		}

		public GenericCounter(string name, string displayName, long value)
		{
			this.name = name;
			this.displayName = displayName;
			this.value = value;
		}

		[Obsolete]
		public override void SetDisplayName(string displayName)
		{
			lock (this)
			{
				this.displayName = displayName;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ReadFields(DataInput @in)
		{
			lock (this)
			{
				name = StringInterner.WeakIntern(Text.ReadString(@in));
				displayName = @in.ReadBoolean() ? StringInterner.WeakIntern(Text.ReadString(@in))
					 : name;
				value = WritableUtils.ReadVLong(@in);
			}
		}

		/// <summary>GenericCounter ::= keyName isDistinctDisplayName [displayName] value</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void Write(DataOutput @out)
		{
			lock (this)
			{
				Text.WriteString(@out, name);
				bool distinctDisplayName = !name.Equals(displayName);
				@out.WriteBoolean(distinctDisplayName);
				if (distinctDisplayName)
				{
					Text.WriteString(@out, displayName);
				}
				WritableUtils.WriteVLong(@out, value);
			}
		}

		public override string GetName()
		{
			lock (this)
			{
				return name;
			}
		}

		public override string GetDisplayName()
		{
			lock (this)
			{
				return displayName;
			}
		}

		public override long GetValue()
		{
			lock (this)
			{
				return value;
			}
		}

		public override void SetValue(long value)
		{
			lock (this)
			{
				this.value = value;
			}
		}

		public override void Increment(long incr)
		{
			lock (this)
			{
				value += incr;
			}
		}

		public override Counter GetUnderlyingCounter()
		{
			return this;
		}
	}
}
