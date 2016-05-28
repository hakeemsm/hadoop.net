using Com.Google.Protobuf;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Proto
{
	/// <summary>
	/// Fake protocol to differentiate the blocking interfaces in the
	/// security info class loaders.
	/// </summary>
	public abstract class HSClientProtocol
	{
		public abstract class HSClientProtocolService
		{
			public interface BlockingInterface : MRClientProtocolPB
			{
			}

			public static BlockingService NewReflectiveBlockingService(HSClientProtocol.HSClientProtocolService.BlockingInterface
				 impl)
			{
				// The cast is safe
				return MRClientProtocol.MRClientProtocolService.NewReflectiveBlockingService((MRClientProtocol.MRClientProtocolService.BlockingInterface
					)impl);
			}
		}
	}

	public static class HSClientProtocolConstants
	{
	}
}
