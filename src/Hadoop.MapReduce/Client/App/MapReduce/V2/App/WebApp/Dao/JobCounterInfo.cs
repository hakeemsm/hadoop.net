using System.Collections.Generic;
using Javax.Xml.Bind.Annotation;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.Dao
{
	public class JobCounterInfo
	{
		[XmlTransient]
		protected internal Counters total = null;

		[XmlTransient]
		protected internal Counters map = null;

		[XmlTransient]
		protected internal Counters reduce = null;

		protected internal string id;

		protected internal AList<CounterGroupInfo> counterGroup;

		public JobCounterInfo()
		{
		}

		public JobCounterInfo(AppContext ctx, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job 
			job)
		{
			GetCounters(ctx, job);
			counterGroup = new AList<CounterGroupInfo>();
			this.id = MRApps.ToString(job.GetID());
			if (total != null)
			{
				foreach (CounterGroup g in total)
				{
					if (g != null)
					{
						CounterGroup mg = map == null ? null : map.GetGroup(g.GetName());
						CounterGroup rg = reduce == null ? null : reduce.GetGroup(g.GetName());
						CounterGroupInfo cginfo = new CounterGroupInfo(g.GetName(), g, mg, rg);
						counterGroup.AddItem(cginfo);
					}
				}
			}
		}

		private void GetCounters(AppContext ctx, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
			 job)
		{
			if (job == null)
			{
				return;
			}
			total = job.GetAllCounters();
			bool needTotalCounters = false;
			if (total == null)
			{
				total = new Counters();
				needTotalCounters = true;
			}
			map = new Counters();
			reduce = new Counters();
			// Get all types of counters
			IDictionary<TaskId, Task> tasks = job.GetTasks();
			foreach (Task t in tasks.Values)
			{
				Counters counters = t.GetCounters();
				if (counters == null)
				{
					continue;
				}
				switch (t.GetType())
				{
					case TaskType.Map:
					{
						map.IncrAllCounters(counters);
						break;
					}

					case TaskType.Reduce:
					{
						reduce.IncrAllCounters(counters);
						break;
					}
				}
				if (needTotalCounters)
				{
					total.IncrAllCounters(counters);
				}
			}
		}
	}
}
