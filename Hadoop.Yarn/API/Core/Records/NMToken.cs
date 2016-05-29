using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>
	/// <p>The NMToken is used for authenticating communication with
	/// <code>NodeManager</code></p>
	/// <p>It is issued by <code>ResourceMananger</code> when <code>ApplicationMaster</code>
	/// negotiates resource with <code>ResourceManager</code> and
	/// validated on <code>NodeManager</code> side.</p>
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.AllocateResponse.GetNMTokens()
	/// 	"/>
	public abstract class NMToken
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static NMToken NewInstance(NodeId nodeId, Token token)
		{
			NMToken nmToken = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<NMToken>();
			nmToken.SetNodeId(nodeId);
			nmToken.SetToken(token);
			return nmToken;
		}

		/// <summary>
		/// Get the
		/// <see cref="NodeId"/>
		/// of the <code>NodeManager</code> for which the NMToken
		/// is used to authenticate.
		/// </summary>
		/// <returns>
		/// the
		/// <see cref="NodeId"/>
		/// of the <code>NodeManager</code> for which the
		/// NMToken is used to authenticate.
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract NodeId GetNodeId();

		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetNodeId(NodeId nodeId);

		/// <summary>
		/// Get the
		/// <see cref="Token"/>
		/// used for authenticating with <code>NodeManager</code>
		/// </summary>
		/// <returns>
		/// the
		/// <see cref="Token"/>
		/// used for authenticating with <code>NodeManager</code>
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract Token GetToken();

		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetToken(Token token);

		public override int GetHashCode()
		{
			int prime = 31;
			int result = 1;
			result = prime * result + ((GetNodeId() == null) ? 0 : GetNodeId().GetHashCode());
			result = prime * result + ((GetToken() == null) ? 0 : GetToken().GetHashCode());
			return result;
		}

		public override bool Equals(object obj)
		{
			if (this == obj)
			{
				return true;
			}
			if (obj == null)
			{
				return false;
			}
			if (GetType() != obj.GetType())
			{
				return false;
			}
			NMToken other = (NMToken)obj;
			if (GetNodeId() == null)
			{
				if (other.GetNodeId() != null)
				{
					return false;
				}
			}
			else
			{
				if (!GetNodeId().Equals(other.GetNodeId()))
				{
					return false;
				}
			}
			if (GetToken() == null)
			{
				if (other.GetToken() != null)
				{
					return false;
				}
			}
			else
			{
				if (!GetToken().Equals(other.GetToken()))
				{
					return false;
				}
			}
			return true;
		}
	}
}
