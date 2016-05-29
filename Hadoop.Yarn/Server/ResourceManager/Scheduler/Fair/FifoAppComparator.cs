using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	/// <summary>
	/// Order
	/// <see cref="FSAppAttempt"/>
	/// objects by priority and then by submit time, as
	/// in the default scheduler in Hadoop.
	/// </summary>
	[System.Serializable]
	public class FifoAppComparator : IComparer<FSAppAttempt>
	{
		private const long serialVersionUID = 3428835083489547918L;

		public virtual int Compare(FSAppAttempt a1, FSAppAttempt a2)
		{
			int res = a1.GetPriority().CompareTo(a2.GetPriority());
			if (res == 0)
			{
				if (a1.GetStartTime() < a2.GetStartTime())
				{
					res = -1;
				}
				else
				{
					res = (a1.GetStartTime() == a2.GetStartTime() ? 0 : 1);
				}
			}
			if (res == 0)
			{
				// If there is a tie, break it by app ID to get a deterministic order
				res = a1.GetApplicationId().CompareTo(a2.GetApplicationId());
			}
			return res;
		}
	}
}
