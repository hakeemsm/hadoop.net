using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.Dao
{
	public class CounterGroupInfo
	{
		protected internal string counterGroupName;

		protected internal AList<CounterInfo> counter;

		public CounterGroupInfo()
		{
		}

		public CounterGroupInfo(string name, CounterGroup group, CounterGroup mg, CounterGroup
			 rg)
		{
			this.counterGroupName = name;
			this.counter = new AList<CounterInfo>();
			foreach (Counter c in group)
			{
				Counter mc = mg == null ? null : mg.FindCounter(c.GetName());
				Counter rc = rg == null ? null : rg.FindCounter(c.GetName());
				CounterInfo cinfo = new CounterInfo(c, mc, rc);
				this.counter.AddItem(cinfo);
			}
		}
	}
}
