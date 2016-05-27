/*
* MetricValue.java
*
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

namespace Org.Apache.Hadoop.Metrics.Spi
{
	/// <summary>A Number that is either an absolute or an incremental amount.</summary>
	public class MetricValue
	{
		public const bool Absolute = false;

		public const bool Increment = true;

		private bool isIncrement;

		private Number number;

		/// <summary>Creates a new instance of MetricValue</summary>
		public MetricValue(Number number, bool isIncrement)
		{
			this.number = number;
			this.isIncrement = isIncrement;
		}

		public virtual bool IsIncrement()
		{
			return isIncrement;
		}

		public virtual bool IsAbsolute()
		{
			return !isIncrement;
		}

		public virtual Number GetNumber()
		{
			return number;
		}
	}
}
