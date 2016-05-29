using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Records
{
	public interface MasterKey
	{
		int GetKeyId();

		void SetKeyId(int keyId);

		ByteBuffer GetBytes();

		void SetBytes(ByteBuffer bytes);
	}
}
