using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao
{
	public class ApplicationStatisticsInfo
	{
		protected internal AList<StatisticsItemInfo> statItem = new AList<StatisticsItemInfo
			>();

		public ApplicationStatisticsInfo()
		{
		}

		// JAXB needs this
		public virtual void Add(StatisticsItemInfo statItem)
		{
			this.statItem.AddItem(statItem);
		}

		public virtual AList<StatisticsItemInfo> GetStatItems()
		{
			return statItem;
		}
	}
}
