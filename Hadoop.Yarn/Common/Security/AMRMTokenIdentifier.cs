using System.IO;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Security
{
	/// <summary>
	/// AMRMTokenIdentifier is the TokenIdentifier to be used by
	/// ApplicationMasters to authenticate to the ResourceManager.
	/// </summary>
	public class AMRMTokenIdentifier : TokenIdentifier
	{
		public static readonly Text KindName = new Text("YARN_AM_RM_TOKEN");

		private YarnSecurityTokenProtos.AMRMTokenIdentifierProto proto;

		public AMRMTokenIdentifier()
		{
		}

		public AMRMTokenIdentifier(ApplicationAttemptId appAttemptId, int masterKeyId)
		{
			YarnSecurityTokenProtos.AMRMTokenIdentifierProto.Builder builder = YarnSecurityTokenProtos.AMRMTokenIdentifierProto
				.NewBuilder();
			if (appAttemptId != null)
			{
				builder.SetAppAttemptId(((ApplicationAttemptIdPBImpl)appAttemptId).GetProto());
			}
			builder.SetKeyId(masterKeyId);
			proto = ((YarnSecurityTokenProtos.AMRMTokenIdentifierProto)builder.Build());
		}

		[InterfaceAudience.Private]
		public virtual ApplicationAttemptId GetApplicationAttemptId()
		{
			if (!proto.HasAppAttemptId())
			{
				return null;
			}
			return new ApplicationAttemptIdPBImpl(proto.GetAppAttemptId());
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(DataOutput @out)
		{
			@out.Write(proto.ToByteArray());
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ReadFields(DataInput @in)
		{
			proto = YarnSecurityTokenProtos.AMRMTokenIdentifierProto.ParseFrom((DataInputStream
				)@in);
		}

		public override Text GetKind()
		{
			return KindName;
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

		public virtual int GetKeyId()
		{
			return proto.GetKeyId();
		}

		public virtual YarnSecurityTokenProtos.AMRMTokenIdentifierProto GetProto()
		{
			return this.proto;
		}

		public class Renewer : Token.TrivialRenewer
		{
			// TODO: Needed?
			protected override Text GetKind()
			{
				return KindName;
			}
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
