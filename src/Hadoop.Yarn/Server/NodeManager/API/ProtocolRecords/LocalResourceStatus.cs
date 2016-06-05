using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Protocolrecords
{
	public interface LocalResourceStatus
	{
		LocalResource GetResource();

		ResourceStatusType GetStatus();

		URL GetLocalPath();

		long GetLocalSize();

		SerializedException GetException();

		void SetResource(LocalResource resource);

		void SetStatus(ResourceStatusType status);

		void SetLocalPath(URL localPath);

		void SetLocalSize(long size);

		void SetException(SerializedException exception);
	}
}
