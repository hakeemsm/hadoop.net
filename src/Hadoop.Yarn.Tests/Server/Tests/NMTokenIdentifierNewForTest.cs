using System.IO;
using Com.Google.Protobuf;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server
{
	public class NMTokenIdentifierNewForTest : NMTokenIdentifier
	{
		private static Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.NMTokenIdentifierNewForTest
			));

		public static readonly Text Kind = new Text("NMToken");

		private YarnSecurityTestTokenProtos.NMTokenIdentifierNewProto proto;

		private YarnSecurityTestTokenProtos.NMTokenIdentifierNewProto.Builder builder;

		public NMTokenIdentifierNewForTest()
		{
			builder = YarnSecurityTestTokenProtos.NMTokenIdentifierNewProto.NewBuilder();
		}

		public NMTokenIdentifierNewForTest(YarnSecurityTestTokenProtos.NMTokenIdentifierNewProto
			 proto)
		{
			this.proto = proto;
		}

		public NMTokenIdentifierNewForTest(NMTokenIdentifier tokenIdentifier, string message
			)
		{
			builder = YarnSecurityTestTokenProtos.NMTokenIdentifierNewProto.NewBuilder();
			builder.SetAppAttemptId(tokenIdentifier.GetProto().GetAppAttemptId());
			builder.SetNodeId(tokenIdentifier.GetProto().GetNodeId());
			builder.SetAppSubmitter(tokenIdentifier.GetApplicationSubmitter());
			builder.SetKeyId(tokenIdentifier.GetKeyId());
			builder.SetMessage(message);
			proto = ((YarnSecurityTestTokenProtos.NMTokenIdentifierNewProto)builder.Build());
			builder = null;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(DataOutput @out)
		{
			Log.Debug("Writing NMTokenIdentifierNewForTest to RPC layer: " + this);
			@out.Write(proto.ToByteArray());
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ReadFields(DataInput @in)
		{
			DataInputStream dis = (DataInputStream)@in;
			byte[] buffer = IOUtils.ToByteArray(dis);
			proto = YarnSecurityTestTokenProtos.NMTokenIdentifierNewProto.ParseFrom(buffer);
		}

		public override Text GetKind()
		{
			return Kind;
		}

		public override UserGroupInformation GetUser()
		{
			return null;
		}

		public virtual string GetMessage()
		{
			return proto.GetMessage();
		}

		public virtual void SetMessage(string message)
		{
			builder.SetMessage(message);
		}

		public virtual YarnSecurityTestTokenProtos.NMTokenIdentifierNewProto GetNewProto(
			)
		{
			return proto;
		}

		public virtual void Build()
		{
			proto = ((YarnSecurityTestTokenProtos.NMTokenIdentifierNewProto)builder.Build());
			builder = null;
		}

		public override ApplicationAttemptId GetApplicationAttemptId()
		{
			return new ApplicationAttemptIdPBImpl(proto.GetAppAttemptId());
		}

		public override NodeId GetNodeId()
		{
			return new NodeIdPBImpl(proto.GetNodeId());
		}

		public override string GetApplicationSubmitter()
		{
			return proto.GetAppSubmitter();
		}

		public override int GetKeyId()
		{
			return proto.GetKeyId();
		}

		public override int GetHashCode()
		{
			return this.proto.GetHashCode();
		}

		public override bool Equals(object other)
		{
			if (other == null)
			{
				return false;
			}
			if (other.GetType().IsAssignableFrom(this.GetType()))
			{
				return this.GetNewProto().Equals(this.GetType().Cast(other).GetNewProto());
			}
			return false;
		}

		public override string ToString()
		{
			return TextFormat.ShortDebugString(this.proto);
		}
	}
}
