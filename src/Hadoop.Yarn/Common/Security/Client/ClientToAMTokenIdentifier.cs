using System.IO;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Security.Client
{
	public class ClientToAMTokenIdentifier : TokenIdentifier
	{
		public static readonly Text KindName = new Text("YARN_CLIENT_TOKEN");

		private YarnSecurityTokenProtos.ClientToAMTokenIdentifierProto proto;

		public ClientToAMTokenIdentifier()
		{
		}

		public ClientToAMTokenIdentifier(ApplicationAttemptId id, string client)
		{
			// TODO: Add more information in the tokenID such that it is not
			// transferrable, more secure etc.
			YarnSecurityTokenProtos.ClientToAMTokenIdentifierProto.Builder builder = YarnSecurityTokenProtos.ClientToAMTokenIdentifierProto
				.NewBuilder();
			if (id != null)
			{
				builder.SetAppAttemptId(((ApplicationAttemptIdPBImpl)id).GetProto());
			}
			if (client != null)
			{
				builder.SetClientName(client);
			}
			proto = ((YarnSecurityTokenProtos.ClientToAMTokenIdentifierProto)builder.Build());
		}

		public virtual ApplicationAttemptId GetApplicationAttemptID()
		{
			if (!proto.HasAppAttemptId())
			{
				return null;
			}
			return new ApplicationAttemptIdPBImpl(proto.GetAppAttemptId());
		}

		public virtual string GetClientName()
		{
			return proto.GetClientName();
		}

		public virtual YarnSecurityTokenProtos.ClientToAMTokenIdentifierProto GetProto()
		{
			return proto;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(DataOutput @out)
		{
			@out.Write(proto.ToByteArray());
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ReadFields(DataInput @in)
		{
			proto = YarnSecurityTokenProtos.ClientToAMTokenIdentifierProto.ParseFrom((DataInputStream
				)@in);
		}

		public override Text GetKind()
		{
			return KindName;
		}

		public override UserGroupInformation GetUser()
		{
			string clientName = GetClientName();
			if (clientName == null)
			{
				return null;
			}
			return UserGroupInformation.CreateRemoteUser(clientName);
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
