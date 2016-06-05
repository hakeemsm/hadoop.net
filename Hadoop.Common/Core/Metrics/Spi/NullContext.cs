/*
* NullContext.java
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
using Org.Apache.Hadoop.Classification;


namespace Org.Apache.Hadoop.Metrics.Spi
{
	/// <summary>Null metrics context: a metrics context which does nothing.</summary>
	/// <remarks>
	/// Null metrics context: a metrics context which does nothing.  Used as the
	/// default context, so that no performance data is emitted if no configuration
	/// data is found.
	/// </remarks>
	public class NullContext : AbstractMetricsContext
	{
		/// <summary>Creates a new instance of NullContext</summary>
		[InterfaceAudience.Private]
		public NullContext()
		{
		}

		/// <summary>Do-nothing version of startMonitoring</summary>
		[InterfaceAudience.Private]
		public override void StartMonitoring()
		{
		}

		/// <summary>Do-nothing version of emitRecord</summary>
		[InterfaceAudience.Private]
		protected internal override void EmitRecord(string contextName, string recordName
			, OutputRecord outRec)
		{
		}

		/// <summary>Do-nothing version of update</summary>
		[InterfaceAudience.Private]
		protected internal override void Update(MetricsRecordImpl record)
		{
		}

		/// <summary>Do-nothing version of remove</summary>
		[InterfaceAudience.Private]
		protected internal override void Remove(MetricsRecordImpl record)
		{
		}
	}
}
