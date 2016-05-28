using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	public class BlockPlacementStatusDefault : BlockPlacementStatus
	{
		private int requiredRacks = 0;

		private int currentRacks = 0;

		public BlockPlacementStatusDefault(int currentRacks, int requiredRacks)
		{
			this.requiredRacks = requiredRacks;
			this.currentRacks = currentRacks;
		}

		public virtual bool IsPlacementPolicySatisfied()
		{
			return requiredRacks <= currentRacks;
		}

		public virtual string GetErrorDescription()
		{
			if (IsPlacementPolicySatisfied())
			{
				return null;
			}
			return "Block should be additionally replicated on " + (requiredRacks - currentRacks
				) + " more rack(s).";
		}
	}
}
