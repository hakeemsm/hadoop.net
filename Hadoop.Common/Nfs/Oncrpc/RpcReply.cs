using Com.Google.Common.Base;
using Org.Apache.Hadoop.Oncrpc.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Oncrpc
{
	/// <summary>Represents an RPC message of type RPC reply as defined in RFC 1831</summary>
	public abstract class RpcReply : RpcMessage
	{
		/// <summary>RPC reply_stat as defined in RFC 1831</summary>
		[System.Serializable]
		public sealed class ReplyState
		{
			public static readonly RpcReply.ReplyState MsgAccepted = new RpcReply.ReplyState(
				);

			public static readonly RpcReply.ReplyState MsgDenied = new RpcReply.ReplyState();

			// the order of the values below are significant.
			internal int GetValue()
			{
				return Ordinal();
			}

			public static RpcReply.ReplyState FromValue(int value)
			{
				return Values()[value];
			}
		}

		protected internal readonly RpcReply.ReplyState replyState;

		protected internal readonly Verifier verifier;

		internal RpcReply(int xid, RpcReply.ReplyState state, Verifier verifier)
			: base(xid, RpcMessage.Type.RpcReply)
		{
			this.replyState = state;
			this.verifier = verifier;
		}

		public virtual RpcAuthInfo GetVerifier()
		{
			return verifier;
		}

		public static RpcReply Read(XDR xdr)
		{
			int xid = xdr.ReadInt();
			RpcMessage.Type messageType = RpcMessage.Type.FromValue(xdr.ReadInt());
			Preconditions.CheckState(messageType == RpcMessage.Type.RpcReply);
			RpcReply.ReplyState stat = RpcReply.ReplyState.FromValue(xdr.ReadInt());
			switch (stat)
			{
				case RpcReply.ReplyState.MsgAccepted:
				{
					return RpcAcceptedReply.Read(xid, stat, xdr);
				}

				case RpcReply.ReplyState.MsgDenied:
				{
					return RpcDeniedReply.Read(xid, stat, xdr);
				}
			}
			return null;
		}

		public virtual RpcReply.ReplyState GetState()
		{
			return replyState;
		}
	}
}
