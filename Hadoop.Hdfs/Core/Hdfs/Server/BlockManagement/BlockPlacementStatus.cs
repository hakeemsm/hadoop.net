using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	public interface BlockPlacementStatus
	{
		/// <summary>
		/// Boolean value to identify if replicas of this block satisfy requirement of
		/// placement policy
		/// </summary>
		/// <returns>if replicas satisfy placement policy's requirement</returns>
		bool IsPlacementPolicySatisfied();

		/// <summary>
		/// Get description info for log or printed in case replicas are failed to meet
		/// requirement of placement policy
		/// </summary>
		/// <returns>
		/// description in case replicas are failed to meet requirement of
		/// placement policy
		/// </returns>
		string GetErrorDescription();
	}
}
