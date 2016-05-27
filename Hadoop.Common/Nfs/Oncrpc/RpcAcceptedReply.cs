using Org.Apache.Hadoop.Oncrpc.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Oncrpc
{
	/// <summary>Represents RPC message MSG_ACCEPTED reply body.</summary>
	/// <remarks>
	/// Represents RPC message MSG_ACCEPTED reply body. See RFC 1831 for details.
	/// This response is sent to a request to indicate success of the request.
	/// </remarks>
	public class RpcAcceptedReply : RpcReply
	{
		[System.Serializable]
		public sealed class AcceptState
		{
			public static readonly RpcAcceptedReply.AcceptState Success = new RpcAcceptedReply.AcceptState
				();

			public static readonly RpcAcceptedReply.AcceptState ProgUnavail = new RpcAcceptedReply.AcceptState
				();

			public static readonly RpcAcceptedReply.AcceptState ProgMismatch = new RpcAcceptedReply.AcceptState
				();

			public static readonly RpcAcceptedReply.AcceptState ProcUnavail = new RpcAcceptedReply.AcceptState
				();

			public static readonly RpcAcceptedReply.AcceptState GarbageArgs = new RpcAcceptedReply.AcceptState
				();

			public static readonly RpcAcceptedReply.AcceptState SystemErr = new RpcAcceptedReply.AcceptState
				();

			// the order of the values below are significant.
			/* RPC executed successfully */
			/* remote hasn't exported program */
			/* remote can't support version # */
			/* program can't support procedure */
			/* procedure can't decode params */
			/* e.g. memory allocation failure */
			public static RpcAcceptedReply.AcceptState FromValue(int value)
			{
				return Values()[value];
			}

			public int GetValue()
			{
				return Ordinal();
			}
		}

		public static RpcAcceptedReply GetAcceptInstance(int xid, Verifier verifier)
		{
			return GetInstance(xid, RpcAcceptedReply.AcceptState.Success, verifier);
		}

		public static RpcAcceptedReply GetInstance(int xid, RpcAcceptedReply.AcceptState 
			state, Verifier verifier)
		{
			return new RpcAcceptedReply(xid, RpcReply.ReplyState.MsgAccepted, verifier, state
				);
		}

		private readonly RpcAcceptedReply.AcceptState acceptState;

		internal RpcAcceptedReply(int xid, RpcReply.ReplyState state, Verifier verifier, 
			RpcAcceptedReply.AcceptState acceptState)
			: base(xid, state, verifier)
		{
			this.acceptState = acceptState;
		}

		public static RpcAcceptedReply Read(int xid, RpcReply.ReplyState replyState, XDR 
			xdr)
		{
			Verifier verifier = Verifier.ReadFlavorAndVerifier(xdr);
			RpcAcceptedReply.AcceptState acceptState = RpcAcceptedReply.AcceptState.FromValue
				(xdr.ReadInt());
			return new RpcAcceptedReply(xid, replyState, verifier, acceptState);
		}

		public virtual RpcAcceptedReply.AcceptState GetAcceptState()
		{
			return acceptState;
		}

		public override XDR Write(XDR xdr)
		{
			xdr.WriteInt(xid);
			xdr.WriteInt(messageType.GetValue());
			xdr.WriteInt(replyState.GetValue());
			Verifier.WriteFlavorAndVerifier(verifier, xdr);
			xdr.WriteInt(acceptState.GetValue());
			return xdr;
		}
	}
}
