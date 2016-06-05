using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao
{
	public class StatisticsItemInfo
	{
		protected internal YarnApplicationState state;

		protected internal string type;

		protected internal long count;

		public StatisticsItemInfo()
		{
		}

		public StatisticsItemInfo(YarnApplicationState state, string type, long count)
		{
			// JAXB needs this
			this.state = state;
			this.type = type;
			this.count = count;
		}

		public virtual YarnApplicationState GetState()
		{
			return state;
		}

		public virtual string GetType()
		{
			return type;
		}

		public virtual long GetCount()
		{
			return count;
		}
	}
}
