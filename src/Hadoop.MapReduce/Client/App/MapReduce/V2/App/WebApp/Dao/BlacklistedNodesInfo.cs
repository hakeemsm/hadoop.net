using System.Collections.Generic;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.Dao
{
	public class BlacklistedNodesInfo
	{
		private ICollection<string> blacklistedNodes;

		public BlacklistedNodesInfo()
		{
		}

		public BlacklistedNodesInfo(AppContext appContext)
		{
			blacklistedNodes = appContext.GetBlacklistedNodes();
		}

		public virtual ICollection<string> GetBlacklistedNodes()
		{
			return blacklistedNodes;
		}
	}
}
