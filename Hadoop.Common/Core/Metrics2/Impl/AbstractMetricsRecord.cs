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
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Metrics2;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Impl
{
	internal abstract class AbstractMetricsRecord : MetricsRecord
	{
		public override bool Equals(object obj)
		{
			if (obj is MetricsRecord)
			{
				MetricsRecord other = (MetricsRecord)obj;
				return Objects.Equal(Timestamp(), other.Timestamp()) && Objects.Equal(Name(), other
					.Name()) && Objects.Equal(Description(), other.Description()) && Objects.Equal(Tags
					(), other.Tags()) && Iterables.ElementsEqual(Metrics(), other.Metrics());
			}
			return false;
		}

		// Should make sense most of the time when the record is used as a key
		public override int GetHashCode()
		{
			return Objects.HashCode(Name(), Description(), Tags());
		}

		public override string ToString()
		{
			return Objects.ToStringHelper(this).Add("timestamp", Timestamp()).Add("name", Name
				()).Add("description", Description()).Add("tags", Tags()).Add("metrics", Iterables
				.ToString(Metrics())).ToString();
		}

		public abstract string Context();

		public abstract string Description();

		public abstract IEnumerable<AbstractMetric> Metrics();

		public abstract string Name();

		public abstract ICollection<MetricsTag> Tags();

		public abstract long Timestamp();
	}
}
