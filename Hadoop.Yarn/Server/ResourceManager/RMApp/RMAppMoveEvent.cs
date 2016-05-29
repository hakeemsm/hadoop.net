using Com.Google.Common.Util.Concurrent;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp
{
	public class RMAppMoveEvent : RMAppEvent
	{
		private string targetQueue;

		private SettableFuture<object> result;

		public RMAppMoveEvent(ApplicationId id, string newQueue, SettableFuture<object> resultFuture
			)
			: base(id, RMAppEventType.Move)
		{
			this.targetQueue = newQueue;
			this.result = resultFuture;
		}

		public virtual string GetTargetQueue()
		{
			return targetQueue;
		}

		public virtual SettableFuture<object> GetResult()
		{
			return result;
		}
	}
}
