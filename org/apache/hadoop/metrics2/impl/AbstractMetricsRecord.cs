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
using Sharpen;

namespace org.apache.hadoop.metrics2.impl
{
	internal abstract class AbstractMetricsRecord : org.apache.hadoop.metrics2.MetricsRecord
	{
		public override bool Equals(object obj)
		{
			if (obj is org.apache.hadoop.metrics2.MetricsRecord)
			{
				org.apache.hadoop.metrics2.MetricsRecord other = (org.apache.hadoop.metrics2.MetricsRecord
					)obj;
				return com.google.common.@base.Objects.equal(timestamp(), other.timestamp()) && com.google.common.@base.Objects
					.equal(name(), other.name()) && com.google.common.@base.Objects.equal(description
					(), other.description()) && com.google.common.@base.Objects.equal(tags(), other.
					tags()) && com.google.common.collect.Iterables.elementsEqual(metrics(), other.metrics
					());
			}
			return false;
		}

		// Should make sense most of the time when the record is used as a key
		public override int GetHashCode()
		{
			return com.google.common.@base.Objects.hashCode(name(), description(), tags());
		}

		public override string ToString()
		{
			return com.google.common.@base.Objects.toStringHelper(this).add("timestamp", timestamp
				()).add("name", name()).add("description", description()).add("tags", tags()).add
				("metrics", com.google.common.collect.Iterables.toString(metrics())).ToString();
		}

		public abstract string context();

		public abstract string description();

		public abstract System.Collections.Generic.IEnumerable<org.apache.hadoop.metrics2.AbstractMetric
			> metrics();

		public abstract string name();

		public abstract System.Collections.Generic.ICollection<org.apache.hadoop.metrics2.MetricsTag
			> tags();

		public abstract long timestamp();
	}
}
