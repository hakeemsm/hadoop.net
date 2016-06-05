using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>Collects the statistics in time windows.</summary>
	internal class StatisticsCollector
	{
		private const int DefaultPeriod = 5;

		internal static readonly StatisticsCollector.TimeWindow SinceStart = new StatisticsCollector.TimeWindow
			("Since Start", -1, -1);

		internal static readonly StatisticsCollector.TimeWindow LastWeek = new StatisticsCollector.TimeWindow
			("Last Week", 7 * 24 * 60 * 60, 60 * 60);

		internal static readonly StatisticsCollector.TimeWindow LastDay = new StatisticsCollector.TimeWindow
			("Last Day", 24 * 60 * 60, 60 * 60);

		internal static readonly StatisticsCollector.TimeWindow LastHour = new StatisticsCollector.TimeWindow
			("Last Hour", 60 * 60, 60);

		internal static readonly StatisticsCollector.TimeWindow LastMinute = new StatisticsCollector.TimeWindow
			("Last Minute", 60, 10);

		internal static readonly StatisticsCollector.TimeWindow[] DefaultCollectWindows = 
			new StatisticsCollector.TimeWindow[] { Org.Apache.Hadoop.Mapred.StatisticsCollector
			.SinceStart, Org.Apache.Hadoop.Mapred.StatisticsCollector.LastDay, Org.Apache.Hadoop.Mapred.StatisticsCollector
			.LastHour };

		private readonly int period;

		private bool started;

		private readonly IDictionary<StatisticsCollector.TimeWindow, StatisticsCollector.StatUpdater
			> updaters = new LinkedHashMap<StatisticsCollector.TimeWindow, StatisticsCollector.StatUpdater
			>();

		private readonly IDictionary<string, StatisticsCollector.Stat> statistics = new Dictionary
			<string, StatisticsCollector.Stat>();

		internal StatisticsCollector()
			: this(DefaultPeriod)
		{
		}

		internal StatisticsCollector(int period)
		{
			this.period = period;
		}

		internal virtual void Start()
		{
			lock (this)
			{
				if (started)
				{
					return;
				}
				Timer timer = new Timer("Timer thread for monitoring ", true);
				TimerTask task = new _TimerTask_78(this);
				long millis = period * 1000;
				timer.ScheduleAtFixedRate(task, millis, millis);
				started = true;
			}
		}

		private sealed class _TimerTask_78 : TimerTask
		{
			public _TimerTask_78(StatisticsCollector _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override void Run()
			{
				this._enclosing.Update();
			}

			private readonly StatisticsCollector _enclosing;
		}

		protected internal virtual void Update()
		{
			lock (this)
			{
				foreach (StatisticsCollector.StatUpdater c in updaters.Values)
				{
					c.Update();
				}
			}
		}

		internal virtual IDictionary<StatisticsCollector.TimeWindow, StatisticsCollector.StatUpdater
			> GetUpdaters()
		{
			return Sharpen.Collections.UnmodifiableMap(updaters);
		}

		internal virtual IDictionary<string, StatisticsCollector.Stat> GetStatistics()
		{
			return Sharpen.Collections.UnmodifiableMap(statistics);
		}

		internal virtual StatisticsCollector.Stat CreateStat(string name)
		{
			lock (this)
			{
				return CreateStat(name, DefaultCollectWindows);
			}
		}

		internal virtual StatisticsCollector.Stat CreateStat(string name, StatisticsCollector.TimeWindow
			[] windows)
		{
			lock (this)
			{
				if (statistics[name] != null)
				{
					throw new RuntimeException("Stat with name " + name + " is already defined");
				}
				IDictionary<StatisticsCollector.TimeWindow, StatisticsCollector.Stat.TimeStat> timeStats
					 = new LinkedHashMap<StatisticsCollector.TimeWindow, StatisticsCollector.Stat.TimeStat
					>();
				foreach (StatisticsCollector.TimeWindow window in windows)
				{
					StatisticsCollector.StatUpdater collector = updaters[window];
					if (collector == null)
					{
						if (SinceStart.Equals(window))
						{
							collector = new StatisticsCollector.StatUpdater();
						}
						else
						{
							collector = new StatisticsCollector.TimeWindowStatUpdater(window, period);
						}
						updaters[window] = collector;
					}
					StatisticsCollector.Stat.TimeStat timeStat = new StatisticsCollector.Stat.TimeStat
						();
					collector.AddTimeStat(name, timeStat);
					timeStats[window] = timeStat;
				}
				StatisticsCollector.Stat stat = new StatisticsCollector.Stat(name, timeStats);
				statistics[name] = stat;
				return stat;
			}
		}

		internal virtual StatisticsCollector.Stat RemoveStat(string name)
		{
			lock (this)
			{
				StatisticsCollector.Stat stat = Sharpen.Collections.Remove(statistics, name);
				if (stat != null)
				{
					foreach (StatisticsCollector.StatUpdater collector in updaters.Values)
					{
						collector.RemoveTimeStat(name);
					}
				}
				return stat;
			}
		}

		internal class TimeWindow
		{
			internal readonly string name;

			internal readonly int windowSize;

			internal readonly int updateGranularity;

			internal TimeWindow(string name, int windowSize, int updateGranularity)
			{
				if (updateGranularity > windowSize)
				{
					throw new RuntimeException("Invalid TimeWindow: updateGranularity > windowSize");
				}
				this.name = name;
				this.windowSize = windowSize;
				this.updateGranularity = updateGranularity;
			}

			public override int GetHashCode()
			{
				return name.GetHashCode() + updateGranularity + windowSize;
			}

			public override bool Equals(object obj)
			{
				if (this == obj)
				{
					return true;
				}
				if (obj == null)
				{
					return false;
				}
				if (GetType() != obj.GetType())
				{
					return false;
				}
				StatisticsCollector.TimeWindow other = (StatisticsCollector.TimeWindow)obj;
				if (name == null)
				{
					if (other.name != null)
					{
						return false;
					}
				}
				else
				{
					if (!name.Equals(other.name))
					{
						return false;
					}
				}
				if (updateGranularity != other.updateGranularity)
				{
					return false;
				}
				if (windowSize != other.windowSize)
				{
					return false;
				}
				return true;
			}
		}

		internal class Stat
		{
			internal readonly string name;

			private IDictionary<StatisticsCollector.TimeWindow, StatisticsCollector.Stat.TimeStat
				> timeStats;

			private Stat(string name, IDictionary<StatisticsCollector.TimeWindow, StatisticsCollector.Stat.TimeStat
				> timeStats)
			{
				this.name = name;
				this.timeStats = timeStats;
			}

			public virtual void Inc(int incr)
			{
				lock (this)
				{
					foreach (StatisticsCollector.Stat.TimeStat ts in timeStats.Values)
					{
						ts.Inc(incr);
					}
				}
			}

			public virtual void Inc()
			{
				lock (this)
				{
					Inc(1);
				}
			}

			public virtual IDictionary<StatisticsCollector.TimeWindow, StatisticsCollector.Stat.TimeStat
				> GetValues()
			{
				lock (this)
				{
					return Sharpen.Collections.UnmodifiableMap(timeStats);
				}
			}

			internal class TimeStat
			{
				private readonly List<int> buckets = new List<int>();

				private int value;

				private int currentValue;

				public virtual int GetValue()
				{
					lock (this)
					{
						return value;
					}
				}

				private void Inc(int i)
				{
					lock (this)
					{
						currentValue += i;
					}
				}

				private void AddBucket()
				{
					lock (this)
					{
						buckets.AddLast(currentValue);
						SetValueToCurrent();
					}
				}

				private void SetValueToCurrent()
				{
					lock (this)
					{
						value += currentValue;
						currentValue = 0;
					}
				}

				private void RemoveBucket()
				{
					lock (this)
					{
						int removed = buckets.RemoveFirst();
						value -= removed;
					}
				}
			}
		}

		private class StatUpdater
		{
			protected internal readonly IDictionary<string, StatisticsCollector.Stat.TimeStat
				> statToCollect = new Dictionary<string, StatisticsCollector.Stat.TimeStat>();

			internal virtual void AddTimeStat(string name, StatisticsCollector.Stat.TimeStat 
				s)
			{
				lock (this)
				{
					statToCollect[name] = s;
				}
			}

			internal virtual StatisticsCollector.Stat.TimeStat RemoveTimeStat(string name)
			{
				lock (this)
				{
					return Sharpen.Collections.Remove(statToCollect, name);
				}
			}

			internal virtual void Update()
			{
				lock (this)
				{
					foreach (StatisticsCollector.Stat.TimeStat stat in statToCollect.Values)
					{
						stat.SetValueToCurrent();
					}
				}
			}
		}

		/// <summary>Updates TimeWindow statistics in buckets.</summary>
		private class TimeWindowStatUpdater : StatisticsCollector.StatUpdater
		{
			internal readonly int collectBuckets;

			internal readonly int updatesPerBucket;

			private int updates;

			private int buckets;

			internal TimeWindowStatUpdater(StatisticsCollector.TimeWindow w, int updatePeriod
				)
			{
				if (updatePeriod > w.updateGranularity)
				{
					throw new RuntimeException("Invalid conf: updatePeriod > updateGranularity");
				}
				collectBuckets = w.windowSize / w.updateGranularity;
				updatesPerBucket = w.updateGranularity / updatePeriod;
			}

			internal override void Update()
			{
				lock (this)
				{
					updates++;
					if (updates == updatesPerBucket)
					{
						foreach (StatisticsCollector.Stat.TimeStat stat in statToCollect.Values)
						{
							stat.AddBucket();
						}
						updates = 0;
						buckets++;
						if (buckets > collectBuckets)
						{
							foreach (StatisticsCollector.Stat.TimeStat stat_1 in statToCollect.Values)
							{
								stat_1.RemoveBucket();
							}
							buckets--;
						}
					}
				}
			}
		}
	}
}
