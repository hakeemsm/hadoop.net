using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler
{
	[System.Serializable]
	public class SchedulerDynamicEditException : YarnException
	{
		private const long serialVersionUID = 7100374511387193257L;

		public SchedulerDynamicEditException(string @string)
			: base(@string)
		{
		}
	}
}
