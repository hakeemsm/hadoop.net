using System.IO;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Security.Client
{
	public abstract class YARNDelegationTokenIdentifier : AbstractDelegationTokenIdentifier
	{
		internal YarnSecurityTokenProtos.YARNDelegationTokenIdentifierProto.Builder builder
			 = YarnSecurityTokenProtos.YARNDelegationTokenIdentifierProto.NewBuilder();

		public YARNDelegationTokenIdentifier()
		{
		}

		public YARNDelegationTokenIdentifier(Text owner, Text renewer, Text realUser)
			: base(owner, renewer, realUser)
		{
		}

		public YARNDelegationTokenIdentifier(YarnSecurityTokenProtos.YARNDelegationTokenIdentifierProto.Builder
			 builder)
		{
			this.builder = builder;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ReadFields(DataInput @in)
		{
			lock (this)
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
		}

		private void SetBuilderFields()
		{
			if (builder.GetOwner() != null && !builder.GetOwner().Equals(GetOwner().ToString(
				)))
			{
				builder.SetOwner(GetOwner().ToString());
			}
			if (builder.GetRenewer() != null && !builder.GetRenewer().Equals(GetRenewer().ToString
				()))
			{
				builder.SetRenewer(GetRenewer().ToString());
			}
			if (builder.GetRealUser() != null && !builder.GetRealUser().Equals(GetRealUser().
				ToString()))
			{
				builder.SetRealUser(GetRealUser().ToString());
			}
			if (builder.GetIssueDate() != GetIssueDate())
			{
				builder.SetIssueDate(GetIssueDate());
			}
			if (builder.GetMaxDate() != GetMaxDate())
			{
				builder.SetMaxDate(GetMaxDate());
			}
			if (builder.GetSequenceNumber() != GetSequenceNumber())
			{
				builder.SetSequenceNumber(GetSequenceNumber());
			}
			if (builder.GetMasterKeyId() != GetMasterKeyId())
			{
				builder.SetMasterKeyId(GetMasterKeyId());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(DataOutput @out)
		{
			lock (this)
			{
				SetBuilderFields();
				((YarnSecurityTokenProtos.YARNDelegationTokenIdentifierProto)builder.Build()).WriteTo
					((DataOutputStream)@out);
			}
		}

		public virtual YarnSecurityTokenProtos.YARNDelegationTokenIdentifierProto GetProto
			()
		{
			SetBuilderFields();
			return ((YarnSecurityTokenProtos.YARNDelegationTokenIdentifierProto)builder.Build
				());
		}
	}
}
