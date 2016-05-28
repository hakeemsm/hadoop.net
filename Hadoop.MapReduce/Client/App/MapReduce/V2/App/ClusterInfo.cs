using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App
{
	public class ClusterInfo
	{
		private Resource maxContainerCapability;

		public ClusterInfo()
		{
			this.maxContainerCapability = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<Resource
				>();
		}

		public ClusterInfo(Resource maxCapability)
		{
			this.maxContainerCapability = maxCapability;
		}

		public virtual Resource GetMaxContainerCapability()
		{
			return maxContainerCapability;
		}

		public virtual void SetMaxContainerCapability(Resource maxContainerCapability)
		{
			this.maxContainerCapability = maxContainerCapability;
		}
	}
}
