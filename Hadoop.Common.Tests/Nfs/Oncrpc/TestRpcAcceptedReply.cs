using Org.Apache.Hadoop.Oncrpc.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Oncrpc
{
	/// <summary>
	/// Test for
	/// <see cref="RpcAcceptedReply"/>
	/// </summary>
	public class TestRpcAcceptedReply
	{
		[NUnit.Framework.Test]
		public virtual void TestAcceptState()
		{
			NUnit.Framework.Assert.AreEqual(RpcAcceptedReply.AcceptState.Success, RpcAcceptedReply.AcceptState
				.FromValue(0));
			NUnit.Framework.Assert.AreEqual(RpcAcceptedReply.AcceptState.ProgUnavail, RpcAcceptedReply.AcceptState
				.FromValue(1));
			NUnit.Framework.Assert.AreEqual(RpcAcceptedReply.AcceptState.ProgMismatch, RpcAcceptedReply.AcceptState
				.FromValue(2));
			NUnit.Framework.Assert.AreEqual(RpcAcceptedReply.AcceptState.ProcUnavail, RpcAcceptedReply.AcceptState
				.FromValue(3));
			NUnit.Framework.Assert.AreEqual(RpcAcceptedReply.AcceptState.GarbageArgs, RpcAcceptedReply.AcceptState
				.FromValue(4));
			NUnit.Framework.Assert.AreEqual(RpcAcceptedReply.AcceptState.SystemErr, RpcAcceptedReply.AcceptState
				.FromValue(5));
		}

		public virtual void TestAcceptStateFromInvalidValue()
		{
			RpcAcceptedReply.AcceptState.FromValue(6);
		}

		[NUnit.Framework.Test]
		public virtual void TestConstructor()
		{
			Verifier verifier = new VerifierNone();
			RpcAcceptedReply reply = new RpcAcceptedReply(0, RpcReply.ReplyState.MsgAccepted, 
				verifier, RpcAcceptedReply.AcceptState.Success);
			NUnit.Framework.Assert.AreEqual(0, reply.GetXid());
			NUnit.Framework.Assert.AreEqual(RpcMessage.Type.RpcReply, reply.GetMessageType());
			NUnit.Framework.Assert.AreEqual(RpcReply.ReplyState.MsgAccepted, reply.GetState()
				);
			NUnit.Framework.Assert.AreEqual(verifier, reply.GetVerifier());
			NUnit.Framework.Assert.AreEqual(RpcAcceptedReply.AcceptState.Success, reply.GetAcceptState
				());
		}
	}
}
