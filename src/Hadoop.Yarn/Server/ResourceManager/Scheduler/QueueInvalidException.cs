using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler
{
	[System.Serializable]
	public class QueueInvalidException : YarnRuntimeException
	{
		private const long serialVersionUID = 187239430L;

		public QueueInvalidException(string message)
			: base(message)
		{
		}
	}
}
