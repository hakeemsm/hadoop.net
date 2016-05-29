using System;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class RMFatalEvent : AbstractEvent<RMFatalEventType>
	{
		private string cause;

		public RMFatalEvent(RMFatalEventType rmFatalEventType, string cause)
			: base(rmFatalEventType)
		{
			this.cause = cause;
		}

		public RMFatalEvent(RMFatalEventType rmFatalEventType, Exception cause)
			: base(rmFatalEventType)
		{
			this.cause = StringUtils.StringifyException(cause);
		}

		public virtual string GetCause()
		{
			return this.cause;
		}
	}
}
