using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.Dao
{
	public class TaskCounterGroupInfo
	{
		protected internal string counterGroupName;

		protected internal AList<TaskCounterInfo> counter;

		public TaskCounterGroupInfo()
		{
		}

		public TaskCounterGroupInfo(string name, CounterGroup group)
		{
			this.counterGroupName = name;
			this.counter = new AList<TaskCounterInfo>();
			foreach (Counter c in group)
			{
				TaskCounterInfo cinfo = new TaskCounterInfo(c.GetName(), c.GetValue());
				this.counter.AddItem(cinfo);
			}
		}
	}
}
