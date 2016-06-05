using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.Dao
{
	public class TaskCounterInfo
	{
		protected internal string name;

		protected internal long value;

		public TaskCounterInfo()
		{
		}

		public TaskCounterInfo(string name, long value)
		{
			this.name = name;
			this.value = value;
		}

		public virtual string GetName()
		{
			return name;
		}

		public virtual long GetValue()
		{
			return value;
		}
	}
}
