using System.IO;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class RMDelegationTokenIdentifierForTest : RMDelegationTokenIdentifier
	{
		private YarnSecurityTestClientAMTokenProtos.RMDelegationTokenIdentifierForTestProto.Builder
			 builder = YarnSecurityTestClientAMTokenProtos.RMDelegationTokenIdentifierForTestProto
			.NewBuilder();

		public RMDelegationTokenIdentifierForTest()
		{
		}

		public RMDelegationTokenIdentifierForTest(RMDelegationTokenIdentifier token, string
			 message)
		{
			if (token.GetOwner() != null)
			{
				SetOwner(new Text(token.GetOwner()));
			}
			if (token.GetRenewer() != null)
			{
				SetRenewer(new Text(token.GetRenewer()));
			}
			if (token.GetRealUser() != null)
			{
				SetRealUser(new Text(token.GetRealUser()));
			}
			SetIssueDate(token.GetIssueDate());
			SetMaxDate(token.GetMaxDate());
			SetSequenceNumber(token.GetSequenceNumber());
			SetMasterKeyId(token.GetMasterKeyId());
			builder.SetMessage(message);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(DataOutput @out)
		{
			builder.SetOwner(GetOwner().ToString());
			builder.SetRenewer(GetRenewer().ToString());
			builder.SetRealUser(GetRealUser().ToString());
			builder.SetIssueDate(GetIssueDate());
			builder.SetMaxDate(GetMaxDate());
			builder.SetSequenceNumber(GetSequenceNumber());
			builder.SetMasterKeyId(GetMasterKeyId());
			builder.SetMessage(GetMessage());
			((YarnSecurityTestClientAMTokenProtos.RMDelegationTokenIdentifierForTestProto)builder
				.Build()).WriteTo((DataOutputStream)@out);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ReadFields(DataInput @in)
		{
			builder.MergeFrom((DataInputStream)@in);
			if (builder.GetOwner() != null)
			{
				SetOwner(new Text(builder.GetOwner()));
			}
			if (builder.GetRenewer() != null)
			{
				SetRenewer(new Text(builder.GetRenewer()));
			}
			if (builder.GetRealUser() != null)
			{
				SetRealUser(new Text(builder.GetRealUser()));
			}
			SetIssueDate(builder.GetIssueDate());
			SetMaxDate(builder.GetMaxDate());
			SetSequenceNumber(builder.GetSequenceNumber());
			SetMasterKeyId(builder.GetMasterKeyId());
		}

		public virtual string GetMessage()
		{
			return builder.GetMessage();
		}

		public override bool Equals(object obj)
		{
			if (obj == this)
			{
				return true;
			}
			if (obj is Org.Apache.Hadoop.Yarn.Server.Resourcemanager.RMDelegationTokenIdentifierForTest)
			{
				Org.Apache.Hadoop.Yarn.Server.Resourcemanager.RMDelegationTokenIdentifierForTest 
					that = (Org.Apache.Hadoop.Yarn.Server.Resourcemanager.RMDelegationTokenIdentifierForTest
					)obj;
				return this.GetSequenceNumber() == that.GetSequenceNumber() && this.GetIssueDate(
					) == that.GetIssueDate() && this.GetMaxDate() == that.GetMaxDate() && this.GetMasterKeyId
					() == that.GetMasterKeyId() && IsEqual(this.GetOwner(), that.GetOwner()) && IsEqual
					(this.GetRenewer(), that.GetRenewer()) && IsEqual(this.GetRealUser(), that.GetRealUser
					()) && IsEqual(this.GetMessage(), that.GetMessage());
			}
			return false;
		}
	}
}
