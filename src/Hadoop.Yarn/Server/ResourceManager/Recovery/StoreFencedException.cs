using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery
{
	[System.Serializable]
	public class StoreFencedException : YarnException
	{
		private const long serialVersionUID = 1L;

		public StoreFencedException()
			: base("RMStateStore has been fenced")
		{
		}
	}
}
