using System.Text;
using Org.Apache.Hadoop.Oncrpc.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Oncrpc
{
	/// <summary>Represents RPC message MSG_DENIED reply body.</summary>
	/// <remarks>
	/// Represents RPC message MSG_DENIED reply body. See RFC 1831 for details.
	/// This response is sent to a request to indicate failure of the request.
	/// </remarks>
	public class RpcDeniedReply : RpcReply
	{
		[System.Serializable]
		public sealed class RejectState
		{
			public static readonly RpcDeniedReply.RejectState RpcMismatch = new RpcDeniedReply.RejectState
				();

			public static readonly RpcDeniedReply.RejectState AuthError = new RpcDeniedReply.RejectState
				();

			// the order of the values below are significant.
			internal int GetValue()
			{
				return Ordinal();
			}

			internal static RpcDeniedReply.RejectState FromValue(int value)
			{
				return Values()[value];
			}
		}

		private readonly RpcDeniedReply.RejectState rejectState;

		public RpcDeniedReply(int xid, RpcReply.ReplyState replyState, RpcDeniedReply.RejectState
			 rejectState, Verifier verifier)
			: base(xid, replyState, verifier)
		{
			this.rejectState = rejectState;
		}

		public static RpcDeniedReply Read(int xid, RpcReply.ReplyState replyState, XDR xdr
			)
		{
			Verifier verifier = Verifier.ReadFlavorAndVerifier(xdr);
			RpcDeniedReply.RejectState rejectState = RpcDeniedReply.RejectState.FromValue(xdr
				.ReadInt());
			return new RpcDeniedReply(xid, replyState, rejectState, verifier);
		}

		public virtual RpcDeniedReply.RejectState GetRejectState()
		{
			return rejectState;
		}

		public override string ToString()
		{
			return new StringBuilder().Append("xid:").Append(xid).Append(",messageType:").Append
				(messageType).Append("verifier_flavor:").Append(verifier.GetFlavor()).Append("rejectState:"
				).Append(rejectState).ToString();
		}

		public override XDR Write(XDR xdr)
		{
			xdr.WriteInt(xid);
			xdr.WriteInt(messageType.GetValue());
			xdr.WriteInt(replyState.GetValue());
			Verifier.WriteFlavorAndVerifier(verifier, xdr);
			xdr.WriteInt(rejectState.GetValue());
			return xdr;
		}
	}
}
