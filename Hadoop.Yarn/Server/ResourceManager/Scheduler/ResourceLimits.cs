using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler
{
	/// <summary>
	/// Resource limits for queues/applications, this means max overall (please note
	/// that, it's not "extra") resource you can get.
	/// </summary>
	public class ResourceLimits
	{
		internal volatile Resource limit;

		private volatile Resource amountNeededUnreserve;

		public ResourceLimits(Resource limit)
		{
			// This is special limit that goes with the RESERVE_CONT_LOOK_ALL_NODES
			// config. This limit indicates how much we need to unreserve to allocate
			// another container.
			this.amountNeededUnreserve = Resources.None();
			this.limit = limit;
		}

		public ResourceLimits(Org.Apache.Hadoop.Yarn.Api.Records.Resource limit, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 amountNeededUnreserve)
		{
			this.amountNeededUnreserve = amountNeededUnreserve;
			this.limit = limit;
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetLimit()
		{
			return limit;
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetAmountNeededUnreserve
			()
		{
			return amountNeededUnreserve;
		}

		public virtual void SetLimit(Org.Apache.Hadoop.Yarn.Api.Records.Resource limit)
		{
			this.limit = limit;
		}

		public virtual void SetAmountNeededUnreserve(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 amountNeededUnreserve)
		{
			this.amountNeededUnreserve = amountNeededUnreserve;
		}
	}
}
