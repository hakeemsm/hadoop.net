using System.IO;
using Com.Google.Protobuf;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Api.Impl
{
	public class AMRMTokenIdentifierForTest : AMRMTokenIdentifier
	{
		private static Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Client.Api.Impl.AMRMTokenIdentifierForTest
			));

		public static readonly Text Kind = new Text("YARN_AM_RM_TOKEN");

		private YarnSecurityTestAMRMTokenProtos.AMRMTokenIdentifierForTestProto proto;

		private YarnSecurityTestAMRMTokenProtos.AMRMTokenIdentifierForTestProto.Builder builder;

		public AMRMTokenIdentifierForTest()
		{
			builder = YarnSecurityTestAMRMTokenProtos.AMRMTokenIdentifierForTestProto.NewBuilder
				();
		}

		public AMRMTokenIdentifierForTest(YarnSecurityTestAMRMTokenProtos.AMRMTokenIdentifierForTestProto
			 proto)
		{
			this.proto = proto;
		}

		public AMRMTokenIdentifierForTest(AMRMTokenIdentifier tokenIdentifier, string message
			)
		{
			builder = YarnSecurityTestAMRMTokenProtos.AMRMTokenIdentifierForTestProto.NewBuilder
				();
			builder.SetAppAttemptId(tokenIdentifier.GetProto().GetAppAttemptId());
			builder.SetKeyId(tokenIdentifier.GetKeyId());
			builder.SetMessage(message);
			proto = ((YarnSecurityTestAMRMTokenProtos.AMRMTokenIdentifierForTestProto)builder
				.Build());
			builder = null;
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
			proto = YarnSecurityTestAMRMTokenProtos.AMRMTokenIdentifierForTestProto.ParseFrom
				(buffer);
		}

		public override Text GetKind()
		{
			return Kind;
		}

		public virtual string GetMessage()
		{
			return proto.GetMessage();
		}

		public virtual void SetMessage(string message)
		{
			builder.SetMessage(message);
		}

		public virtual void Build()
		{
			proto = ((YarnSecurityTestAMRMTokenProtos.AMRMTokenIdentifierForTestProto)builder
				.Build());
			builder = null;
		}

		public override ApplicationAttemptId GetApplicationAttemptId()
		{
			return new ApplicationAttemptIdPBImpl(proto.GetAppAttemptId());
		}

		public override int GetKeyId()
		{
			return proto.GetKeyId();
		}

		public virtual YarnSecurityTestAMRMTokenProtos.AMRMTokenIdentifierForTestProto GetNewProto
			()
		{
			return this.proto;
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
