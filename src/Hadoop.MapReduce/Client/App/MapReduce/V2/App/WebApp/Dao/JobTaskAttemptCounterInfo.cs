using Javax.Xml.Bind.Annotation;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.Dao
{
	public class JobTaskAttemptCounterInfo
	{
		[XmlTransient]
		protected internal Counters total = null;

		protected internal string id;

		protected internal AList<TaskCounterGroupInfo> taskAttemptCounterGroup;

		public JobTaskAttemptCounterInfo()
		{
		}

		public JobTaskAttemptCounterInfo(TaskAttempt taskattempt)
		{
			this.id = MRApps.ToString(taskattempt.GetID());
			total = taskattempt.GetCounters();
			taskAttemptCounterGroup = new AList<TaskCounterGroupInfo>();
			if (total != null)
			{
				foreach (CounterGroup g in total)
				{
					if (g != null)
					{
						TaskCounterGroupInfo cginfo = new TaskCounterGroupInfo(g.GetName(), g);
						if (cginfo != null)
						{
							taskAttemptCounterGroup.AddItem(cginfo);
						}
					}
				}
			}
		}
	}
}
