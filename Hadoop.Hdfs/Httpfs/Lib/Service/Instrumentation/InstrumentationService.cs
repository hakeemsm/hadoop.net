using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Lib.Server;
using Org.Apache.Hadoop.Lib.Service;
using Org.Apache.Hadoop.Util;
using Org.Json.Simple;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Service.Instrumentation
{
	public class InstrumentationService : BaseService, Org.Apache.Hadoop.Lib.Service.Instrumentation
	{
		public const string Prefix = "instrumentation";

		public const string ConfTimersSize = "timers.size";

		private int timersSize;

		private Lock counterLock;

		private Lock timerLock;

		private Lock variableLock;

		private Lock samplerLock;

		private IDictionary<string, IDictionary<string, AtomicLong>> counters;

		private IDictionary<string, IDictionary<string, InstrumentationService.Timer>> timers;

		private IDictionary<string, IDictionary<string, InstrumentationService.VariableHolder
			>> variables;

		private IDictionary<string, IDictionary<string, InstrumentationService.Sampler>> 
			samplers;

		private IList<InstrumentationService.Sampler> samplersList;

		private IDictionary<string, IDictionary<string, object>> all;

		public InstrumentationService()
			: base(Prefix)
		{
		}

		/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServiceException"/>
		protected internal override void Init()
		{
			timersSize = GetServiceConfig().GetInt(ConfTimersSize, 10);
			counterLock = new ReentrantLock();
			timerLock = new ReentrantLock();
			variableLock = new ReentrantLock();
			samplerLock = new ReentrantLock();
			IDictionary<string, InstrumentationService.VariableHolder> jvmVariables = new ConcurrentHashMap
				<string, InstrumentationService.VariableHolder>();
			counters = new ConcurrentHashMap<string, IDictionary<string, AtomicLong>>();
			timers = new ConcurrentHashMap<string, IDictionary<string, InstrumentationService.Timer
				>>();
			variables = new ConcurrentHashMap<string, IDictionary<string, InstrumentationService.VariableHolder
				>>();
			samplers = new ConcurrentHashMap<string, IDictionary<string, InstrumentationService.Sampler
				>>();
			samplersList = new AList<InstrumentationService.Sampler>();
			all = new LinkedHashMap<string, IDictionary<string, object>>();
			all["os-env"] = Sharpen.Runtime.GetEnv();
			all["sys-props"] = (IDictionary<string, object>)(IDictionary)Runtime.GetProperties
				();
			all["jvm"] = jvmVariables;
			all["counters"] = (IDictionary)counters;
			all["timers"] = (IDictionary)timers;
			all["variables"] = (IDictionary)variables;
			all["samplers"] = (IDictionary)samplers;
			jvmVariables["free.memory"] = new InstrumentationService.VariableHolder<long>(new 
				_Variable_87());
			jvmVariables["max.memory"] = new InstrumentationService.VariableHolder<long>(new 
				_Variable_93());
			jvmVariables["total.memory"] = new InstrumentationService.VariableHolder<long>(new 
				_Variable_99());
		}

		private sealed class _Variable_87 : Instrumentation.Variable<long>
		{
			public _Variable_87()
			{
			}

			public long GetValue()
			{
				return Runtime.GetRuntime().FreeMemory();
			}
		}

		private sealed class _Variable_93 : Instrumentation.Variable<long>
		{
			public _Variable_93()
			{
			}

			public long GetValue()
			{
				return Runtime.GetRuntime().MaxMemory();
			}
		}

		private sealed class _Variable_99 : Instrumentation.Variable<long>
		{
			public _Variable_99()
			{
			}

			public long GetValue()
			{
				return Runtime.GetRuntime().TotalMemory();
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServiceException"/>
		public override void PostInit()
		{
			Scheduler scheduler = GetServer().Get<Scheduler>();
			if (scheduler != null)
			{
				scheduler.Schedule(new InstrumentationService.SamplersRunnable(this), 0, 1, TimeUnit
					.Seconds);
			}
		}

		public override Type GetInterface()
		{
			return typeof(Org.Apache.Hadoop.Lib.Service.Instrumentation);
		}

		private T GetToAdd<T>(string group, string name, Lock Lock, IDictionary<string, IDictionary
			<string, T>> map)
		{
			System.Type klass = typeof(T);
			bool locked = false;
			try
			{
				IDictionary<string, T> groupMap = map[group];
				if (groupMap == null)
				{
					Lock.Lock();
					locked = true;
					groupMap = map[group];
					if (groupMap == null)
					{
						groupMap = new ConcurrentHashMap<string, T>();
						map[group] = groupMap;
					}
				}
				T element = groupMap[name];
				if (element == null)
				{
					if (!locked)
					{
						Lock.Lock();
						locked = true;
					}
					element = groupMap[name];
					if (element == null)
					{
						try
						{
							if (klass == typeof(InstrumentationService.Timer))
							{
								element = (T)new InstrumentationService.Timer(timersSize);
							}
							else
							{
								element = System.Activator.CreateInstance(klass);
							}
						}
						catch (Exception ex)
						{
							throw new RuntimeException(ex);
						}
						groupMap[name] = element;
					}
				}
				return element;
			}
			finally
			{
				if (locked)
				{
					Lock.Unlock();
				}
			}
		}

		internal class Cron : Instrumentation.Cron
		{
			internal long start;

			internal long lapStart;

			internal long own;

			internal long total;

			public virtual InstrumentationService.Cron Start()
			{
				if (total != 0)
				{
					throw new InvalidOperationException("Cron already used");
				}
				if (start == 0)
				{
					start = Time.Now();
					lapStart = start;
				}
				else
				{
					if (lapStart == 0)
					{
						lapStart = Time.Now();
					}
				}
				return this;
			}

			public virtual InstrumentationService.Cron Stop()
			{
				if (total != 0)
				{
					throw new InvalidOperationException("Cron already used");
				}
				if (lapStart > 0)
				{
					own += Time.Now() - lapStart;
					lapStart = 0;
				}
				return this;
			}

			internal virtual void End()
			{
				Stop();
				total = Time.Now() - start;
			}
		}

		internal class Timer : JSONAware, JSONStreamAware
		{
			internal const int LastTotal = 0;

			internal const int LastOwn = 1;

			internal const int AvgTotal = 2;

			internal const int AvgOwn = 3;

			internal Lock Lock = new ReentrantLock();

			private long[] own;

			private long[] total;

			private int last;

			private bool full;

			private int size;

			public Timer(int size)
			{
				this.size = size;
				own = new long[size];
				total = new long[size];
				for (int i = 0; i < size; i++)
				{
					own[i] = -1;
					total[i] = -1;
				}
				last = -1;
			}

			internal virtual long[] GetValues()
			{
				Lock.Lock();
				try
				{
					long[] values = new long[4];
					values[LastTotal] = total[last];
					values[LastOwn] = own[last];
					int limit = (full) ? size : (last + 1);
					for (int i = 0; i < limit; i++)
					{
						values[AvgTotal] += total[i];
						values[AvgOwn] += own[i];
					}
					values[AvgTotal] = values[AvgTotal] / limit;
					values[AvgOwn] = values[AvgOwn] / limit;
					return values;
				}
				finally
				{
					Lock.Unlock();
				}
			}

			internal virtual void AddCron(InstrumentationService.Cron cron)
			{
				cron.End();
				Lock.Lock();
				try
				{
					last = (last + 1) % size;
					full = full || last == (size - 1);
					total[last] = cron.total;
					own[last] = cron.own;
				}
				finally
				{
					Lock.Unlock();
				}
			}

			private JSONObject GetJSON()
			{
				long[] values = GetValues();
				JSONObject json = new JSONObject();
				json["lastTotal"] = values[0];
				json["lastOwn"] = values[1];
				json["avgTotal"] = values[2];
				json["avgOwn"] = values[3];
				return json;
			}

			public virtual string ToJSONString()
			{
				return GetJSON().ToJSONString();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void WriteJSONString(TextWriter @out)
			{
				GetJSON().WriteJSONString(@out);
			}
		}

		public override Instrumentation.Cron CreateCron()
		{
			return new InstrumentationService.Cron();
		}

		public override void Incr(string group, string name, long count)
		{
			AtomicLong counter = GetToAdd<AtomicLong>(group, name, counterLock, counters);
			counter.AddAndGet(count);
		}

		public override void AddCron(string group, string name, Instrumentation.Cron cron
			)
		{
			InstrumentationService.Timer timer = GetToAdd<InstrumentationService.Timer>(group
				, name, timerLock, timers);
			timer.AddCron((InstrumentationService.Cron)cron);
		}

		internal class VariableHolder<E> : JSONAware, JSONStreamAware
		{
			internal Instrumentation.Variable<E> var;

			public VariableHolder()
			{
			}

			public VariableHolder(Instrumentation.Variable<E> var)
			{
				this.var = var;
			}

			private JSONObject GetJSON()
			{
				JSONObject json = new JSONObject();
				json["value"] = var.GetValue();
				return json;
			}

			public virtual string ToJSONString()
			{
				return GetJSON().ToJSONString();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void WriteJSONString(TextWriter @out)
			{
				@out.Write(ToJSONString());
			}
		}

		public override void AddVariable<_T0>(string group, string name, Instrumentation.Variable
			<_T0> variable)
		{
			InstrumentationService.VariableHolder holder = GetToAdd<InstrumentationService.VariableHolder
				>(group, name, variableLock, variables);
			holder.var = variable;
		}

		internal class Sampler : JSONAware, JSONStreamAware
		{
			internal Instrumentation.Variable<long> variable;

			internal long[] values;

			private AtomicLong sum;

			private int last;

			private bool full;

			internal virtual void Init(int size, Instrumentation.Variable<long> variable)
			{
				this.variable = variable;
				values = new long[size];
				sum = new AtomicLong();
				last = 0;
			}

			internal virtual void Sample()
			{
				int index = last;
				long valueGoingOut = values[last];
				full = full || last == (values.Length - 1);
				last = (last + 1) % values.Length;
				values[index] = variable.GetValue();
				sum.AddAndGet(-valueGoingOut + values[index]);
			}

			internal virtual double GetRate()
			{
				return ((double)sum.Get()) / ((full) ? values.Length : ((last == 0) ? 1 : last));
			}

			private JSONObject GetJSON()
			{
				JSONObject json = new JSONObject();
				json["sampler"] = GetRate();
				json["size"] = (full) ? values.Length : last;
				return json;
			}

			public virtual string ToJSONString()
			{
				return GetJSON().ToJSONString();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void WriteJSONString(TextWriter @out)
			{
				@out.Write(ToJSONString());
			}
		}

		public override void AddSampler(string group, string name, int samplingSize, Instrumentation.Variable
			<long> variable)
		{
			InstrumentationService.Sampler sampler = GetToAdd<InstrumentationService.Sampler>
				(group, name, samplerLock, samplers);
			samplerLock.Lock();
			try
			{
				sampler.Init(samplingSize, variable);
				samplersList.AddItem(sampler);
			}
			finally
			{
				samplerLock.Unlock();
			}
		}

		internal class SamplersRunnable : Runnable
		{
			public virtual void Run()
			{
				this._enclosing.samplerLock.Lock();
				try
				{
					foreach (InstrumentationService.Sampler sampler in this._enclosing.samplersList)
					{
						sampler.Sample();
					}
				}
				finally
				{
					this._enclosing.samplerLock.Unlock();
				}
			}

			internal SamplersRunnable(InstrumentationService _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly InstrumentationService _enclosing;
		}

		public override IDictionary<string, IDictionary<string, object>> GetSnapshot()
		{
			return all;
		}
	}
}
