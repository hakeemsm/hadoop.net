using Javax.Xml.Bind.Annotation;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.Dao
{
	public class JobTaskCounterInfo
	{
		[XmlTransient]
		protected internal Counters total = null;

		protected internal string id;

		protected internal AList<TaskCounterGroupInfo> taskCounterGroup;

		public JobTaskCounterInfo()
		{
		}

		public JobTaskCounterInfo(Task task)
		{
			total = task.GetCounters();
			this.id = MRApps.ToString(task.GetID());
			taskCounterGroup = new AList<TaskCounterGroupInfo>();
			if (total != null)
			{
				foreach (CounterGroup g in total)
				{
					if (g != null)
					{
						TaskCounterGroupInfo cginfo = new TaskCounterGroupInfo(g.GetName(), g);
						taskCounterGroup.AddItem(cginfo);
					}
				}
			}
		}
	}
}
