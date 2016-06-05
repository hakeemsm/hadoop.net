using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.Dao
{
	public class CounterInfo
	{
		protected internal string name;

		protected internal long totalCounterValue;

		protected internal long mapCounterValue;

		protected internal long reduceCounterValue;

		public CounterInfo()
		{
		}

		public CounterInfo(Counter c, Counter mc, Counter rc)
		{
			this.name = c.GetName();
			this.totalCounterValue = c.GetValue();
			this.mapCounterValue = mc == null ? 0 : mc.GetValue();
			this.reduceCounterValue = rc == null ? 0 : rc.GetValue();
		}
	}
}
