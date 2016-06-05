using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api
{
	public interface LocalizationProtocol
	{
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		LocalizerHeartbeatResponse Heartbeat(LocalizerStatus status);
	}
}
