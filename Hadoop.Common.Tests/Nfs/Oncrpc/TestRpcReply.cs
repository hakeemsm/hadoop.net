using NUnit.Framework;
using Org.Apache.Hadoop.Oncrpc.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Oncrpc
{
	/// <summary>
	/// Test for
	/// <see cref="RpcReply"/>
	/// </summary>
	public class TestRpcReply
	{
		[NUnit.Framework.Test]
		public virtual void TestReplyStateFromValue()
		{
			NUnit.Framework.Assert.AreEqual(RpcReply.ReplyState.MsgAccepted, RpcReply.ReplyState
				.FromValue(0));
			NUnit.Framework.Assert.AreEqual(RpcReply.ReplyState.MsgDenied, RpcReply.ReplyState
				.FromValue(1));
		}

		public virtual void TestReplyStateFromInvalidValue1()
		{
			RpcReply.ReplyState.FromValue(2);
		}

		[NUnit.Framework.Test]
		public virtual void TestRpcReply()
		{
			RpcReply reply = new _RpcReply_44(0, RpcReply.ReplyState.MsgAccepted, new VerifierNone
				());
			NUnit.Framework.Assert.AreEqual(0, reply.GetXid());
			NUnit.Framework.Assert.AreEqual(RpcMessage.Type.RpcReply, reply.GetMessageType());
			NUnit.Framework.Assert.AreEqual(RpcReply.ReplyState.MsgAccepted, reply.GetState()
				);
		}

		private sealed class _RpcReply_44 : RpcReply
		{
			public _RpcReply_44(int baseArg1, RpcReply.ReplyState baseArg2, Verifier baseArg3
				)
				: base(baseArg1, baseArg2, baseArg3)
			{
			}

			public override XDR Write(XDR xdr)
			{
				return null;
			}
		}
	}
}
