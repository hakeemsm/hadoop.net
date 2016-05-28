using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Lib.Service;
using Org.Apache.Hadoop.Lib.Service.Scheduler;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Json.Simple;
using Org.Json.Simple.Parser;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Service.Instrumentation
{
	public class TestInstrumentationService : HTestCase
	{
		protected internal override float GetWaitForRatio()
		{
			return 1;
		}

		[NUnit.Framework.Test]
		public virtual void Cron()
		{
			InstrumentationService.Cron cron = new InstrumentationService.Cron();
			NUnit.Framework.Assert.AreEqual(cron.start, 0);
			NUnit.Framework.Assert.AreEqual(cron.lapStart, 0);
			NUnit.Framework.Assert.AreEqual(cron.own, 0);
			NUnit.Framework.Assert.AreEqual(cron.total, 0);
			long begin = Time.Now();
			NUnit.Framework.Assert.AreEqual(cron.Start(), cron);
			NUnit.Framework.Assert.AreEqual(cron.Start(), cron);
			NUnit.Framework.Assert.AreEqual(cron.start, begin, 20);
			NUnit.Framework.Assert.AreEqual(cron.start, cron.lapStart);
			Sleep(100);
			NUnit.Framework.Assert.AreEqual(cron.Stop(), cron);
			long end = Time.Now();
			long delta = end - begin;
			NUnit.Framework.Assert.AreEqual(cron.own, delta, 20);
			NUnit.Framework.Assert.AreEqual(cron.total, 0);
			NUnit.Framework.Assert.AreEqual(cron.lapStart, 0);
			Sleep(100);
			long reStart = Time.Now();
			cron.Start();
			NUnit.Framework.Assert.AreEqual(cron.start, begin, 20);
			NUnit.Framework.Assert.AreEqual(cron.lapStart, reStart, 20);
			Sleep(100);
			cron.Stop();
			long reEnd = Time.Now();
			delta += reEnd - reStart;
			NUnit.Framework.Assert.AreEqual(cron.own, delta, 20);
			NUnit.Framework.Assert.AreEqual(cron.total, 0);
			NUnit.Framework.Assert.AreEqual(cron.lapStart, 0);
			cron.End();
			NUnit.Framework.Assert.AreEqual(cron.total, reEnd - begin, 20);
			try
			{
				cron.Start();
				NUnit.Framework.Assert.Fail();
			}
			catch (InvalidOperationException)
			{
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail();
			}
			try
			{
				cron.Stop();
				NUnit.Framework.Assert.Fail();
			}
			catch (InvalidOperationException)
			{
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void Timer()
		{
			InstrumentationService.Timer timer = new InstrumentationService.Timer(2);
			InstrumentationService.Cron cron = new InstrumentationService.Cron();
			long ownStart;
			long ownEnd;
			long totalStart;
			long totalEnd;
			long ownDelta;
			long totalDelta;
			long avgTotal;
			long avgOwn;
			cron.Start();
			ownStart = Time.Now();
			totalStart = ownStart;
			ownDelta = 0;
			Sleep(100);
			cron.Stop();
			ownEnd = Time.Now();
			ownDelta += ownEnd - ownStart;
			Sleep(100);
			cron.Start();
			ownStart = Time.Now();
			Sleep(100);
			cron.Stop();
			ownEnd = Time.Now();
			ownDelta += ownEnd - ownStart;
			totalEnd = ownEnd;
			totalDelta = totalEnd - totalStart;
			avgTotal = totalDelta;
			avgOwn = ownDelta;
			timer.AddCron(cron);
			long[] values = timer.GetValues();
			NUnit.Framework.Assert.AreEqual(values[InstrumentationService.Timer.LastTotal], totalDelta
				, 20);
			NUnit.Framework.Assert.AreEqual(values[InstrumentationService.Timer.LastOwn], ownDelta
				, 20);
			NUnit.Framework.Assert.AreEqual(values[InstrumentationService.Timer.AvgTotal], avgTotal
				, 20);
			NUnit.Framework.Assert.AreEqual(values[InstrumentationService.Timer.AvgOwn], avgOwn
				, 20);
			cron = new InstrumentationService.Cron();
			cron.Start();
			ownStart = Time.Now();
			totalStart = ownStart;
			ownDelta = 0;
			Sleep(200);
			cron.Stop();
			ownEnd = Time.Now();
			ownDelta += ownEnd - ownStart;
			Sleep(200);
			cron.Start();
			ownStart = Time.Now();
			Sleep(200);
			cron.Stop();
			ownEnd = Time.Now();
			ownDelta += ownEnd - ownStart;
			totalEnd = ownEnd;
			totalDelta = totalEnd - totalStart;
			avgTotal = (avgTotal * 1 + totalDelta) / 2;
			avgOwn = (avgOwn * 1 + ownDelta) / 2;
			timer.AddCron(cron);
			values = timer.GetValues();
			NUnit.Framework.Assert.AreEqual(values[InstrumentationService.Timer.LastTotal], totalDelta
				, 20);
			NUnit.Framework.Assert.AreEqual(values[InstrumentationService.Timer.LastOwn], ownDelta
				, 20);
			NUnit.Framework.Assert.AreEqual(values[InstrumentationService.Timer.AvgTotal], avgTotal
				, 20);
			NUnit.Framework.Assert.AreEqual(values[InstrumentationService.Timer.AvgOwn], avgOwn
				, 20);
			avgTotal = totalDelta;
			avgOwn = ownDelta;
			cron = new InstrumentationService.Cron();
			cron.Start();
			ownStart = Time.Now();
			totalStart = ownStart;
			ownDelta = 0;
			Sleep(300);
			cron.Stop();
			ownEnd = Time.Now();
			ownDelta += ownEnd - ownStart;
			Sleep(300);
			cron.Start();
			ownStart = Time.Now();
			Sleep(300);
			cron.Stop();
			ownEnd = Time.Now();
			ownDelta += ownEnd - ownStart;
			totalEnd = ownEnd;
			totalDelta = totalEnd - totalStart;
			avgTotal = (avgTotal * 1 + totalDelta) / 2;
			avgOwn = (avgOwn * 1 + ownDelta) / 2;
			cron.Stop();
			timer.AddCron(cron);
			values = timer.GetValues();
			NUnit.Framework.Assert.AreEqual(values[InstrumentationService.Timer.LastTotal], totalDelta
				, 20);
			NUnit.Framework.Assert.AreEqual(values[InstrumentationService.Timer.LastOwn], ownDelta
				, 20);
			NUnit.Framework.Assert.AreEqual(values[InstrumentationService.Timer.AvgTotal], avgTotal
				, 20);
			NUnit.Framework.Assert.AreEqual(values[InstrumentationService.Timer.AvgOwn], avgOwn
				, 20);
			JSONObject json = (JSONObject)new JSONParser().Parse(timer.ToJSONString());
			NUnit.Framework.Assert.AreEqual(json.Count, 4);
			NUnit.Framework.Assert.AreEqual(json["lastTotal"], values[InstrumentationService.Timer
				.LastTotal]);
			NUnit.Framework.Assert.AreEqual(json["lastOwn"], values[InstrumentationService.Timer
				.LastOwn]);
			NUnit.Framework.Assert.AreEqual(json["avgTotal"], values[InstrumentationService.Timer
				.AvgTotal]);
			NUnit.Framework.Assert.AreEqual(json["avgOwn"], values[InstrumentationService.Timer
				.AvgOwn]);
			StringWriter writer = new StringWriter();
			timer.WriteJSONString(writer);
			writer.Close();
			json = (JSONObject)new JSONParser().Parse(writer.ToString());
			NUnit.Framework.Assert.AreEqual(json.Count, 4);
			NUnit.Framework.Assert.AreEqual(json["lastTotal"], values[InstrumentationService.Timer
				.LastTotal]);
			NUnit.Framework.Assert.AreEqual(json["lastOwn"], values[InstrumentationService.Timer
				.LastOwn]);
			NUnit.Framework.Assert.AreEqual(json["avgTotal"], values[InstrumentationService.Timer
				.AvgTotal]);
			NUnit.Framework.Assert.AreEqual(json["avgOwn"], values[InstrumentationService.Timer
				.AvgOwn]);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void Sampler()
		{
			long[] value = new long[1];
			Instrumentation.Variable<long> var = new _Variable_238(value);
			InstrumentationService.Sampler sampler = new InstrumentationService.Sampler();
			sampler.Init(4, var);
			NUnit.Framework.Assert.AreEqual(sampler.GetRate(), 0f, 0.0001);
			sampler.Sample();
			NUnit.Framework.Assert.AreEqual(sampler.GetRate(), 0f, 0.0001);
			value[0] = 1;
			sampler.Sample();
			NUnit.Framework.Assert.AreEqual(sampler.GetRate(), (0d + 1) / 2, 0.0001);
			value[0] = 2;
			sampler.Sample();
			NUnit.Framework.Assert.AreEqual(sampler.GetRate(), (0d + 1 + 2) / 3, 0.0001);
			value[0] = 3;
			sampler.Sample();
			NUnit.Framework.Assert.AreEqual(sampler.GetRate(), (0d + 1 + 2 + 3) / 4, 0.0001);
			value[0] = 4;
			sampler.Sample();
			NUnit.Framework.Assert.AreEqual(sampler.GetRate(), (4d + 1 + 2 + 3) / 4, 0.0001);
			JSONObject json = (JSONObject)new JSONParser().Parse(sampler.ToJSONString());
			NUnit.Framework.Assert.AreEqual(json.Count, 2);
			NUnit.Framework.Assert.AreEqual(json["sampler"], sampler.GetRate());
			NUnit.Framework.Assert.AreEqual(json["size"], 4L);
			StringWriter writer = new StringWriter();
			sampler.WriteJSONString(writer);
			writer.Close();
			json = (JSONObject)new JSONParser().Parse(writer.ToString());
			NUnit.Framework.Assert.AreEqual(json.Count, 2);
			NUnit.Framework.Assert.AreEqual(json["sampler"], sampler.GetRate());
			NUnit.Framework.Assert.AreEqual(json["size"], 4L);
		}

		private sealed class _Variable_238 : Instrumentation.Variable<long>
		{
			public _Variable_238(long[] value)
			{
				this.value = value;
			}

			public long GetValue()
			{
				return value[0];
			}

			private readonly long[] value;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void VariableHolder()
		{
			InstrumentationService.VariableHolder<string> variableHolder = new InstrumentationService.VariableHolder
				<string>();
			variableHolder.var = new _Variable_282();
			JSONObject json = (JSONObject)new JSONParser().Parse(variableHolder.ToJSONString(
				));
			NUnit.Framework.Assert.AreEqual(json.Count, 1);
			NUnit.Framework.Assert.AreEqual(json["value"], "foo");
			StringWriter writer = new StringWriter();
			variableHolder.WriteJSONString(writer);
			writer.Close();
			json = (JSONObject)new JSONParser().Parse(writer.ToString());
			NUnit.Framework.Assert.AreEqual(json.Count, 1);
			NUnit.Framework.Assert.AreEqual(json["value"], "foo");
		}

		private sealed class _Variable_282 : Instrumentation.Variable<string>
		{
			public _Variable_282()
			{
			}

			public string GetValue()
			{
				return "foo";
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void Service()
		{
			string dir = TestDirHelper.GetTestDir().GetAbsolutePath();
			string services = StringUtils.Join(",", Arrays.AsList(typeof(InstrumentationService
				).FullName));
			Configuration conf = new Configuration(false);
			conf.Set("server.services", services);
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", dir, dir, dir, dir, conf);
			server.Init();
			Org.Apache.Hadoop.Lib.Service.Instrumentation instrumentation = server.Get<Org.Apache.Hadoop.Lib.Service.Instrumentation
				>();
			NUnit.Framework.Assert.IsNotNull(instrumentation);
			instrumentation.Incr("g", "c", 1);
			instrumentation.Incr("g", "c", 2);
			instrumentation.Incr("g", "c1", 2);
			Instrumentation.Cron cron = instrumentation.CreateCron();
			cron.Start();
			Sleep(100);
			cron.Stop();
			instrumentation.AddCron("g", "t", cron);
			cron = instrumentation.CreateCron();
			cron.Start();
			Sleep(200);
			cron.Stop();
			instrumentation.AddCron("g", "t", cron);
			Instrumentation.Variable<string> var = new _Variable_329();
			instrumentation.AddVariable("g", "v", var);
			Instrumentation.Variable<long> varToSample = new _Variable_337();
			instrumentation.AddSampler("g", "s", 10, varToSample);
			IDictionary<string, object> snapshot = instrumentation.GetSnapshot();
			NUnit.Framework.Assert.IsNotNull(snapshot["os-env"]);
			NUnit.Framework.Assert.IsNotNull(snapshot["sys-props"]);
			NUnit.Framework.Assert.IsNotNull(snapshot["jvm"]);
			NUnit.Framework.Assert.IsNotNull(snapshot["counters"]);
			NUnit.Framework.Assert.IsNotNull(snapshot["timers"]);
			NUnit.Framework.Assert.IsNotNull(snapshot["variables"]);
			NUnit.Framework.Assert.IsNotNull(snapshot["samplers"]);
			NUnit.Framework.Assert.IsNotNull(((IDictionary<string, string>)snapshot["os-env"]
				)["PATH"]);
			NUnit.Framework.Assert.IsNotNull(((IDictionary<string, string>)snapshot["sys-props"
				])["java.version"]);
			NUnit.Framework.Assert.IsNotNull(((IDictionary<string, object>)snapshot["jvm"])["free.memory"
				]);
			NUnit.Framework.Assert.IsNotNull(((IDictionary<string, object>)snapshot["jvm"])["max.memory"
				]);
			NUnit.Framework.Assert.IsNotNull(((IDictionary<string, object>)snapshot["jvm"])["total.memory"
				]);
			NUnit.Framework.Assert.IsNotNull(((IDictionary<string, IDictionary<string, object
				>>)snapshot["counters"])["g"]);
			NUnit.Framework.Assert.IsNotNull(((IDictionary<string, IDictionary<string, object
				>>)snapshot["timers"])["g"]);
			NUnit.Framework.Assert.IsNotNull(((IDictionary<string, IDictionary<string, object
				>>)snapshot["variables"])["g"]);
			NUnit.Framework.Assert.IsNotNull(((IDictionary<string, IDictionary<string, object
				>>)snapshot["samplers"])["g"]);
			NUnit.Framework.Assert.IsNotNull(((IDictionary<string, IDictionary<string, object
				>>)snapshot["counters"])["g"]["c"]);
			NUnit.Framework.Assert.IsNotNull(((IDictionary<string, IDictionary<string, object
				>>)snapshot["counters"])["g"]["c1"]);
			NUnit.Framework.Assert.IsNotNull(((IDictionary<string, IDictionary<string, object
				>>)snapshot["timers"])["g"]["t"]);
			NUnit.Framework.Assert.IsNotNull(((IDictionary<string, IDictionary<string, object
				>>)snapshot["variables"])["g"]["v"]);
			NUnit.Framework.Assert.IsNotNull(((IDictionary<string, IDictionary<string, object
				>>)snapshot["samplers"])["g"]["s"]);
			StringWriter writer = new StringWriter();
			JSONObject.WriteJSONString(snapshot, writer);
			writer.Close();
			server.Destroy();
		}

		private sealed class _Variable_329 : Instrumentation.Variable<string>
		{
			public _Variable_329()
			{
			}

			public string GetValue()
			{
				return "foo";
			}
		}

		private sealed class _Variable_337 : Instrumentation.Variable<long>
		{
			public _Variable_337()
			{
			}

			public long GetValue()
			{
				return 1L;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void Sampling()
		{
			string dir = TestDirHelper.GetTestDir().GetAbsolutePath();
			string services = StringUtils.Join(",", Arrays.AsList(typeof(InstrumentationService
				).FullName, typeof(SchedulerService).FullName));
			Configuration conf = new Configuration(false);
			conf.Set("server.services", services);
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", dir, dir, dir, dir, conf);
			server.Init();
			Org.Apache.Hadoop.Lib.Service.Instrumentation instrumentation = server.Get<Org.Apache.Hadoop.Lib.Service.Instrumentation
				>();
			AtomicInteger count = new AtomicInteger();
			Instrumentation.Variable<long> varToSample = new _Variable_389(count);
			instrumentation.AddSampler("g", "s", 10, varToSample);
			Sleep(2000);
			int i = count.Get();
			NUnit.Framework.Assert.IsTrue(i > 0);
			IDictionary<string, IDictionary<string, object>> snapshot = instrumentation.GetSnapshot
				();
			IDictionary<string, IDictionary<string, object>> samplers = (IDictionary<string, 
				IDictionary<string, object>>)snapshot["samplers"];
			InstrumentationService.Sampler sampler = (InstrumentationService.Sampler)samplers
				["g"]["s"];
			NUnit.Framework.Assert.IsTrue(sampler.GetRate() > 0);
			server.Destroy();
		}

		private sealed class _Variable_389 : Instrumentation.Variable<long>
		{
			public _Variable_389(AtomicInteger count)
			{
				this.count = count;
			}

			public long GetValue()
			{
				return (long)count.IncrementAndGet();
			}

			private readonly AtomicInteger count;
		}
	}
}
