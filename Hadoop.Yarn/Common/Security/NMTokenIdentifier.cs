using System.IO;
using Com.Google.Protobuf;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Security
{
	public class NMTokenIdentifier : TokenIdentifier
	{
		private static Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Security.NMTokenIdentifier
			));

		public static readonly Text Kind = new Text("NMToken");

		private YarnSecurityTokenProtos.NMTokenIdentifierProto proto;

		public NMTokenIdentifier(ApplicationAttemptId appAttemptId, NodeId nodeId, string
			 applicationSubmitter, int masterKeyId)
		{
			YarnSecurityTokenProtos.NMTokenIdentifierProto.Builder builder = YarnSecurityTokenProtos.NMTokenIdentifierProto
				.NewBuilder();
			if (appAttemptId != null)
			{
				builder.SetAppAttemptId(((ApplicationAttemptIdPBImpl)appAttemptId).GetProto());
			}
			if (nodeId != null)
			{
				builder.SetNodeId(((NodeIdPBImpl)nodeId).GetProto());
			}
			builder.SetAppSubmitter(applicationSubmitter);
			builder.SetKeyId(masterKeyId);
			proto = ((YarnSecurityTokenProtos.NMTokenIdentifierProto)builder.Build());
		}

		/// <summary>Default constructor needed by RPC/Secret manager</summary>
		public NMTokenIdentifier()
		{
		}

		public virtual ApplicationAttemptId GetApplicationAttemptId()
		{
			if (!proto.HasAppAttemptId())
			{
				return null;
			}
			return new ApplicationAttemptIdPBImpl(proto.GetAppAttemptId());
		}

		public virtual NodeId GetNodeId()
		{
			if (!proto.HasNodeId())
			{
				return null;
			}
			return new NodeIdPBImpl(proto.GetNodeId());
		}

		public virtual string GetApplicationSubmitter()
		{
			return proto.GetAppSubmitter();
		}

		public virtual int GetKeyId()
		{
			return proto.GetKeyId();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(DataOutput @out)
		{
			Log.Debug("Writing NMTokenIdentifier to RPC layer: " + this);
			@out.Write(proto.ToByteArray());
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ReadFields(DataInput @in)
		{
			proto = YarnSecurityTokenProtos.NMTokenIdentifierProto.ParseFrom((DataInputStream
				)@in);
		}

		public override Text GetKind()
		{
			return Kind;
		}

		public override UserGroupInformation GetUser()
		{
			string appAttemptId = null;
			if (proto.HasAppAttemptId())
			{
				appAttemptId = new ApplicationAttemptIdPBImpl(proto.GetAppAttemptId()).ToString();
			}
			return UserGroupInformation.CreateRemoteUser(appAttemptId);
		}

		public virtual YarnSecurityTokenProtos.NMTokenIdentifierProto GetProto()
		{
			return proto;
		}

		public override int GetHashCode()
		{
			return GetProto().GetHashCode();
		}

		public override bool Equals(object other)
		{
			if (other == null)
			{
				return false;
			}
			if (other.GetType().IsAssignableFrom(this.GetType()))
			{
				return this.GetProto().Equals(this.GetType().Cast(other).GetProto());
			}
			return false;
		}

		public override string ToString()
		{
			return TextFormat.ShortDebugString(GetProto());
		}
	}
}
