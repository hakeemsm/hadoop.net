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
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Annotation;
using Org.Apache.Hadoop.Metrics2.Impl;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Lib
{
	public class TestMetricsAnnotations
	{
		internal class MyMetrics
		{
			[Metric]
			internal MutableCounterInt c1;

			internal MutableCounterLong c2;

			[Metric]
			internal MutableGaugeInt g1;

			[Metric]
			internal MutableGaugeInt g2;

			internal MutableGaugeLong g3;

			[Metric]
			internal MutableRate r1;

			[Metric]
			internal MutableStat s1;

			[Metric]
			internal MutableRates rs1;
		}

		[Fact]
		public virtual void TestFields()
		{
			TestMetricsAnnotations.MyMetrics metrics = new TestMetricsAnnotations.MyMetrics();
			MetricsSource source = MetricsAnnotations.MakeSource(metrics);
			metrics.c1.Incr();
			metrics.c2.Incr();
			metrics.g1.Incr();
			metrics.g2.Incr();
			metrics.g3.Incr();
			metrics.r1.Add(1);
			metrics.s1.Add(1);
			metrics.rs1.Add("rs1", 1);
			MetricsRecordBuilder rb = MetricsAsserts.GetMetrics(source);
			Org.Mockito.Mockito.Verify(rb).AddCounter(Interns.Info("C1", "C1"), 1);
			Org.Mockito.Mockito.Verify(rb).AddCounter(Interns.Info("Counter2", "Counter2 desc"
				), 1L);
			Org.Mockito.Mockito.Verify(rb).AddGauge(Interns.Info("G1", "G1"), 1);
			Org.Mockito.Mockito.Verify(rb).AddGauge(Interns.Info("G2", "G2"), 1);
			Org.Mockito.Mockito.Verify(rb).AddGauge(Interns.Info("G3", "g3 desc"), 1L);
			Org.Mockito.Mockito.Verify(rb).AddCounter(Interns.Info("R1NumOps", "Number of ops for r1"
				), 1L);
			Org.Mockito.Mockito.Verify(rb).AddGauge(Interns.Info("R1AvgTime", "Average time for r1"
				), 1.0);
			Org.Mockito.Mockito.Verify(rb).AddCounter(Interns.Info("S1NumOps", "Number of ops for s1"
				), 1L);
			Org.Mockito.Mockito.Verify(rb).AddGauge(Interns.Info("S1AvgTime", "Average time for s1"
				), 1.0);
			Org.Mockito.Mockito.Verify(rb).AddCounter(Interns.Info("Rs1NumOps", "Number of ops for rs1"
				), 1L);
			Org.Mockito.Mockito.Verify(rb).AddGauge(Interns.Info("Rs1AvgTime", "Average time for rs1"
				), 1.0);
		}

		internal class BadMetrics
		{
			[Metric]
			internal int i0;
		}

		public virtual void TestBadFields()
		{
			MetricsAnnotations.MakeSource(new TestMetricsAnnotations.BadMetrics());
		}

		internal class MyMetrics2
		{
			[Metric]
			internal virtual int GetG1()
			{
				return 1;
			}

			[Metric]
			internal virtual long GetG2()
			{
				return 2;
			}

			[Metric]
			internal virtual float GetG3()
			{
				return 3;
			}

			[Metric]
			internal virtual double GetG4()
			{
				return 4;
			}

			internal virtual int GetC1()
			{
				return 1;
			}

			internal virtual long GetC2()
			{
				return 2;
			}

			internal virtual string GetT1()
			{
				return "t1";
			}
		}

		[Fact]
		public virtual void TestMethods()
		{
			TestMetricsAnnotations.MyMetrics2 metrics = new TestMetricsAnnotations.MyMetrics2
				();
			MetricsSource source = MetricsAnnotations.MakeSource(metrics);
			MetricsRecordBuilder rb = MetricsAsserts.GetMetrics(source);
			Org.Mockito.Mockito.Verify(rb).AddGauge(Interns.Info("G1", "G1"), 1);
			Org.Mockito.Mockito.Verify(rb).AddGauge(Interns.Info("G2", "G2"), 2L);
			Org.Mockito.Mockito.Verify(rb).AddGauge(Interns.Info("G3", "G3"), 3.0f);
			Org.Mockito.Mockito.Verify(rb).AddGauge(Interns.Info("G4", "G4"), 4.0);
			Org.Mockito.Mockito.Verify(rb).AddCounter(Interns.Info("C1", "C1"), 1);
			Org.Mockito.Mockito.Verify(rb).AddCounter(Interns.Info("C2", "C2"), 2L);
			Org.Mockito.Mockito.Verify(rb).Tag(Interns.Info("T1", "T1"), "t1");
		}

		internal class BadMetrics2
		{
			[Metric]
			internal virtual int Foo(int i)
			{
				return i;
			}
		}

		public virtual void TestBadMethodWithArgs()
		{
			MetricsAnnotations.MakeSource(new TestMetricsAnnotations.BadMetrics2());
		}

		internal class BadMetrics3
		{
			[Metric]
			internal virtual bool Foo()
			{
				return true;
			}
		}

		public virtual void TestBadMethodReturnType()
		{
			MetricsAnnotations.MakeSource(new TestMetricsAnnotations.BadMetrics3());
		}

		internal class MyMetrics3
		{
			[Metric]
			internal virtual int GetG1()
			{
				return 1;
			}
		}

		[Fact]
		public virtual void TestClasses()
		{
			MetricsRecordBuilder rb = MetricsAsserts.GetMetrics(MetricsAnnotations.MakeSource
				(new TestMetricsAnnotations.MyMetrics3()));
			MetricsCollector collector = rb.Parent();
			Org.Mockito.Mockito.Verify(collector).AddRecord(Interns.Info("MyMetrics3", "My metrics"
				));
			Org.Mockito.Mockito.Verify(rb).Add(Interns.Tag(MsInfo.Context, "foo"));
		}

		internal class HybridMetrics : MetricsSource
		{
			internal readonly MetricsRegistry registry = new MetricsRegistry("HybridMetrics")
				.SetContext("hybrid");

			internal MutableCounterInt C0;

			[Metric]
			internal virtual int GetG0()
			{
				return 0;
			}

			public virtual void GetMetrics(MetricsCollector collector, bool all)
			{
				collector.AddRecord("foo").SetContext("foocontext").AddCounter(Interns.Info("C1", 
					"C1 desc"), 1).EndRecord().AddRecord("bar").SetContext("barcontext").AddGauge(Interns.Info
					("G1", "G1 desc"), 1);
				registry.Snapshot(collector.AddRecord(registry.Info()), all);
			}
		}

		[Fact]
		public virtual void TestHybrid()
		{
			TestMetricsAnnotations.HybridMetrics metrics = new TestMetricsAnnotations.HybridMetrics
				();
			MetricsSource source = MetricsAnnotations.MakeSource(metrics);
			NUnit.Framework.Assert.AreSame(metrics, source);
			metrics.C0.Incr();
			MetricsRecordBuilder rb = MetricsAsserts.GetMetrics(source);
			MetricsCollector collector = rb.Parent();
			Org.Mockito.Mockito.Verify(collector).AddRecord("foo");
			Org.Mockito.Mockito.Verify(collector).AddRecord("bar");
			Org.Mockito.Mockito.Verify(collector).AddRecord(Interns.Info("HybridMetrics", "HybridMetrics"
				));
			Org.Mockito.Mockito.Verify(rb).SetContext("foocontext");
			Org.Mockito.Mockito.Verify(rb).AddCounter(Interns.Info("C1", "C1 desc"), 1);
			Org.Mockito.Mockito.Verify(rb).SetContext("barcontext");
			Org.Mockito.Mockito.Verify(rb).AddGauge(Interns.Info("G1", "G1 desc"), 1);
			Org.Mockito.Mockito.Verify(rb).Add(Interns.Tag(MsInfo.Context, "hybrid"));
			Org.Mockito.Mockito.Verify(rb).AddCounter(Interns.Info("C0", "C0 desc"), 1);
			Org.Mockito.Mockito.Verify(rb).AddGauge(Interns.Info("G0", "G0"), 0);
		}

		internal class BadHybridMetrics : MetricsSource
		{
			[Metric]
			internal MutableCounterInt c1;

			public virtual void GetMetrics(MetricsCollector collector, bool all)
			{
				collector.AddRecord("foo");
			}
		}

		public virtual void TestBadHybrid()
		{
			MetricsAnnotations.MakeSource(new TestMetricsAnnotations.BadHybridMetrics());
		}

		internal class EmptyMetrics
		{
			internal int foo;
		}

		public virtual void TestEmptyMetrics()
		{
			MetricsAnnotations.MakeSource(new TestMetricsAnnotations.EmptyMetrics());
		}
	}
}
