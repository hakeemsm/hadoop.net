using NUnit.Framework;
using Org.Apache.Hadoop.Oncrpc.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Oncrpc
{
	/// <summary>
	/// Test for
	/// <see cref="RpcDeniedReply"/>
	/// </summary>
	public class TestRpcDeniedReply
	{
		[NUnit.Framework.Test]
		public virtual void TestRejectStateFromValue()
		{
			NUnit.Framework.Assert.AreEqual(RpcDeniedReply.RejectState.RpcMismatch, RpcDeniedReply.RejectState
				.FromValue(0));
			NUnit.Framework.Assert.AreEqual(RpcDeniedReply.RejectState.AuthError, RpcDeniedReply.RejectState
				.FromValue(1));
		}

		public virtual void TestRejectStateFromInvalidValue1()
		{
			RpcDeniedReply.RejectState.FromValue(2);
		}

		[NUnit.Framework.Test]
		public virtual void TestConstructor()
		{
			RpcDeniedReply reply = new RpcDeniedReply(0, RpcReply.ReplyState.MsgAccepted, RpcDeniedReply.RejectState
				.AuthError, new VerifierNone());
			NUnit.Framework.Assert.AreEqual(0, reply.GetXid());
			NUnit.Framework.Assert.AreEqual(RpcMessage.Type.RpcReply, reply.GetMessageType());
			NUnit.Framework.Assert.AreEqual(RpcReply.ReplyState.MsgAccepted, reply.GetState()
				);
			NUnit.Framework.Assert.AreEqual(RpcDeniedReply.RejectState.AuthError, reply.GetRejectState
				());
		}
	}
}
