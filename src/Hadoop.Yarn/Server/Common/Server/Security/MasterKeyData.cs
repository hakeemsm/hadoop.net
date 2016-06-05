using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Security
{
	public class MasterKeyData
	{
		private readonly MasterKey masterKeyRecord;

		private readonly SecretKey generatedSecretKey;

		public MasterKeyData(int serialNo, SecretKey secretKey)
		{
			// Underlying secret-key also stored to avoid repetitive encoding and
			// decoding the masterKeyRecord bytes.
			this.masterKeyRecord = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<MasterKey>();
			this.masterKeyRecord.SetKeyId(serialNo);
			this.generatedSecretKey = secretKey;
			this.masterKeyRecord.SetBytes(ByteBuffer.Wrap(generatedSecretKey.GetEncoded()));
		}

		public MasterKeyData(MasterKey masterKeyRecord, SecretKey secretKey)
		{
			this.masterKeyRecord = masterKeyRecord;
			this.generatedSecretKey = secretKey;
		}

		public virtual MasterKey GetMasterKey()
		{
			return this.masterKeyRecord;
		}

		public virtual SecretKey GetSecretKey()
		{
			return this.generatedSecretKey;
		}
	}
}
