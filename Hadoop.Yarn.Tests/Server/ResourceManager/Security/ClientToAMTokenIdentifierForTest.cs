using System.IO;
using Com.Google.Protobuf;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security
{
	public class ClientToAMTokenIdentifierForTest : ClientToAMTokenIdentifier
	{
		private YarnSecurityTestClientAMTokenProtos.ClientToAMTokenIdentifierForTestProto
			 proto;

		public ClientToAMTokenIdentifierForTest()
		{
		}

		public ClientToAMTokenIdentifierForTest(ClientToAMTokenIdentifier tokenIdentifier
			, string message)
		{
			YarnSecurityTestClientAMTokenProtos.ClientToAMTokenIdentifierForTestProto.Builder
				 builder = YarnSecurityTestClientAMTokenProtos.ClientToAMTokenIdentifierForTestProto
				.NewBuilder();
			builder.SetAppAttemptId(tokenIdentifier.GetProto().GetAppAttemptId());
			builder.SetClientName(tokenIdentifier.GetProto().GetClientName());
			builder.SetMessage(message);
			proto = ((YarnSecurityTestClientAMTokenProtos.ClientToAMTokenIdentifierForTestProto
				)builder.Build());
		}

		public override ApplicationAttemptId GetApplicationAttemptID()
		{
			if (!proto.HasAppAttemptId())
			{
				return null;
			}
			return new ApplicationAttemptIdPBImpl(proto.GetAppAttemptId());
		}

		public override string GetClientName()
		{
			return proto.GetClientName();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(DataOutput @out)
		{
			@out.Write(proto.ToByteArray());
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ReadFields(DataInput @in)
		{
			DataInputStream dis = (DataInputStream)@in;
			byte[] buffer = IOUtils.ToByteArray(dis);
			proto = YarnSecurityTestClientAMTokenProtos.ClientToAMTokenIdentifierForTestProto
				.ParseFrom(buffer);
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
			return GetNewProto().GetHashCode();
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

		public virtual YarnSecurityTestClientAMTokenProtos.ClientToAMTokenIdentifierForTestProto
			 GetNewProto()
		{
			return proto;
		}

		public override string ToString()
		{
			return TextFormat.ShortDebugString(GetNewProto());
		}
	}
}
