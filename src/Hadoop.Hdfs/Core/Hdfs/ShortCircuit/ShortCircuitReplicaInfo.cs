using System.Text;
using Org.Apache.Hadoop.Security.Token;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Shortcircuit
{
	public sealed class ShortCircuitReplicaInfo
	{
		private readonly ShortCircuitReplica replica;

		private readonly SecretManager.InvalidToken exc;

		public ShortCircuitReplicaInfo()
		{
			this.replica = null;
			this.exc = null;
		}

		public ShortCircuitReplicaInfo(ShortCircuitReplica replica)
		{
			this.replica = replica;
			this.exc = null;
		}

		public ShortCircuitReplicaInfo(SecretManager.InvalidToken exc)
		{
			this.replica = null;
			this.exc = exc;
		}

		public ShortCircuitReplica GetReplica()
		{
			return replica;
		}

		public SecretManager.InvalidToken GetInvalidTokenException()
		{
			return exc;
		}

		public override string ToString()
		{
			StringBuilder builder = new StringBuilder();
			string prefix = string.Empty;
			builder.Append("ShortCircuitReplicaInfo{");
			if (replica != null)
			{
				builder.Append(prefix).Append(replica);
				prefix = ", ";
			}
			if (exc != null)
			{
				builder.Append(prefix).Append(exc);
				prefix = ", ";
			}
			builder.Append("}");
			return builder.ToString();
		}
	}
}
