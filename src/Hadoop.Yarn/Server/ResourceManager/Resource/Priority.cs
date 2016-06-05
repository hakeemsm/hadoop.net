using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Resource
{
	public class Priority
	{
		public static Priority Create(int prio)
		{
			Priority priority = RecordFactoryProvider.GetRecordFactory(null).NewRecordInstance
				<Priority>();
			priority.SetPriority(prio);
			return priority;
		}

		public class Comparator : IComparer<Priority>
		{
			public virtual int Compare(Priority o1, Priority o2)
			{
				return o1.GetPriority() - o2.GetPriority();
			}
		}
	}
}
