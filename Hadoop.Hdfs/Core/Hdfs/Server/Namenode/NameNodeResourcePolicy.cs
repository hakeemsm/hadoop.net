using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// Given a set of checkable resources, this class is capable of determining
	/// whether sufficient resources are available for the NN to continue operating.
	/// </summary>
	internal sealed class NameNodeResourcePolicy
	{
		/// <summary>
		/// Return true if and only if there are sufficient NN
		/// resources to continue logging edits.
		/// </summary>
		/// <param name="resources">the collection of resources to check.</param>
		/// <param name="minimumRedundantResources">
		/// the minimum number of redundant resources
		/// required to continue operation.
		/// </param>
		/// <returns>
		/// true if and only if there are sufficient NN resources to
		/// continue logging edits.
		/// </returns>
		internal static bool AreResourcesAvailable<_T0>(ICollection<_T0> resources, int minimumRedundantResources
			)
			where _T0 : CheckableNameNodeResource
		{
			// TODO: workaround:
			// - during startup, if there are no edits dirs on disk, then there is
			// a call to areResourcesAvailable() with no dirs at all, which was
			// previously causing the NN to enter safemode
			if (resources.IsEmpty())
			{
				return true;
			}
			int requiredResourceCount = 0;
			int redundantResourceCount = 0;
			int disabledRedundantResourceCount = 0;
			foreach (CheckableNameNodeResource resource in resources)
			{
				if (!resource.IsRequired())
				{
					redundantResourceCount++;
					if (!resource.IsResourceAvailable())
					{
						disabledRedundantResourceCount++;
					}
				}
				else
				{
					requiredResourceCount++;
					if (!resource.IsResourceAvailable())
					{
						// Short circuit - a required resource is not available.
						return false;
					}
				}
			}
			if (redundantResourceCount == 0)
			{
				// If there are no redundant resources, return true if there are any
				// required resources available.
				return requiredResourceCount > 0;
			}
			else
			{
				return redundantResourceCount - disabledRedundantResourceCount >= minimumRedundantResources;
			}
		}
	}
}
