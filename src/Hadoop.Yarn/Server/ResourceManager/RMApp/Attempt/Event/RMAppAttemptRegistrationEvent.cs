using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt.Event
{
	public class RMAppAttemptRegistrationEvent : RMAppAttemptEvent
	{
		private readonly ApplicationAttemptId appAttemptId;

		private readonly string host;

		private int rpcport;

		private string trackingurl;

		public RMAppAttemptRegistrationEvent(ApplicationAttemptId appAttemptId, string host
			, int rpcPort, string trackingUrl)
			: base(appAttemptId, RMAppAttemptEventType.Registered)
		{
			this.appAttemptId = appAttemptId;
			this.host = host;
			this.rpcport = rpcPort;
			this.trackingurl = trackingUrl;
		}

		public virtual string GetHost()
		{
			return this.host;
		}

		public virtual int GetRpcport()
		{
			return this.rpcport;
		}

		public virtual string GetTrackingurl()
		{
			return this.trackingurl;
		}
	}
}
