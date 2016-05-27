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
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Impl;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Source
{
	public class TestJvmMetrics
	{
		[NUnit.Framework.Test]
		public virtual void TestPresence()
		{
			JvmPauseMonitor pauseMonitor = new JvmPauseMonitor(new Configuration());
			JvmMetrics jvmMetrics = new JvmMetrics("test", "test");
			jvmMetrics.SetPauseMonitor(pauseMonitor);
			MetricsRecordBuilder rb = MetricsAsserts.GetMetrics(jvmMetrics);
			MetricsCollector mc = rb.Parent();
			Org.Mockito.Mockito.Verify(mc).AddRecord(JvmMetricsInfo.JvmMetrics);
			Org.Mockito.Mockito.Verify(rb).Tag(MsInfo.ProcessName, "test");
			Org.Mockito.Mockito.Verify(rb).Tag(MsInfo.SessionId, "test");
			foreach (JvmMetricsInfo info in JvmMetricsInfo.Values())
			{
				if (info.ToString().StartsWith("Mem"))
				{
					Org.Mockito.Mockito.Verify(rb).AddGauge(Eq(info), AnyFloat());
				}
				else
				{
					if (info.ToString().StartsWith("Gc"))
					{
						Org.Mockito.Mockito.Verify(rb).AddCounter(Eq(info), AnyLong());
					}
					else
					{
						if (info.ToString().StartsWith("Threads"))
						{
							Org.Mockito.Mockito.Verify(rb).AddGauge(Eq(info), AnyInt());
						}
						else
						{
							if (info.ToString().StartsWith("Log"))
							{
								Org.Mockito.Mockito.Verify(rb).AddCounter(Eq(info), AnyLong());
							}
						}
					}
				}
			}
		}
	}
}
