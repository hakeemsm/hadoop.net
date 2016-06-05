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
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Metrics;


namespace Org.Apache.Hadoop.Metrics.Spi
{
	/// <summary>
	/// A null context which has a thread calling
	/// periodically when monitoring is started.
	/// </summary>
	/// <remarks>
	/// A null context which has a thread calling
	/// periodically when monitoring is started. This keeps the data sampled
	/// correctly.
	/// In all other respects, this is like the NULL context: No data is emitted.
	/// This is suitable for Monitoring systems like JMX which reads the metrics
	/// when someone reads the data from JMX.
	/// The default impl of start and stop monitoring:
	/// is the AbstractMetricsContext is good enough.
	/// </remarks>
	public class NullContextWithUpdateThread : AbstractMetricsContext
	{
		private const string PeriodProperty = "period";

		/// <summary>Creates a new instance of NullContextWithUpdateThread</summary>
		[InterfaceAudience.Private]
		public NullContextWithUpdateThread()
		{
		}

		[InterfaceAudience.Private]
		public override void Init(string contextName, ContextFactory factory)
		{
			base.Init(contextName, factory);
			ParseAndSetPeriod(PeriodProperty);
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
