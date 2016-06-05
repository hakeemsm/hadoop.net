using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Hadoop.Common.Core.IO;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Token.Delegation
{
	public abstract class AbstractDelegationTokenIdentifier : TokenIdentifier
	{
		private const byte Version = 0;

		private Text owner;

		private Text renewer;

		private Text realUser;

		private long issueDate;

		private long maxDate;

		private int sequenceNumber;

		private int masterKeyId = 0;

		public AbstractDelegationTokenIdentifier()
			: this(new Text(), new Text(), new Text())
		{
		}

		public AbstractDelegationTokenIdentifier(Text owner, Text renewer, Text realUser)
		{
			SetOwner(owner);
			SetRenewer(renewer);
			SetRealUser(realUser);
			issueDate = 0;
			maxDate = 0;
		}

		public abstract override Text GetKind();

		/// <summary>Get the username encoded in the token identifier</summary>
		/// <returns>the username or owner</returns>
		public override UserGroupInformation GetUser()
		{
			if ((owner == null) || (owner.ToString().IsEmpty()))
			{
				return null;
			}
			UserGroupInformation realUgi;
			UserGroupInformation ugi;
			if ((realUser == null) || (realUser.ToString().IsEmpty()) || realUser.Equals(owner
				))
			{
				ugi = realUgi = UserGroupInformation.CreateRemoteUser(owner.ToString());
			}
			else
			{
				realUgi = UserGroupInformation.CreateRemoteUser(realUser.ToString());
				ugi = UserGroupInformation.CreateProxyUser(owner.ToString(), realUgi);
			}
			realUgi.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Token);
			return ugi;
		}

		public virtual Text GetOwner()
		{
			return owner;
		}

		public virtual void SetOwner(Text owner)
		{
			if (owner == null)
			{
				this.owner = new Text();
			}
			else
			{
				this.owner = owner;
			}
		}

		public virtual Text GetRenewer()
		{
			return renewer;
		}

		public virtual void SetRenewer(Text renewer)
		{
			if (renewer == null)
			{
				this.renewer = new Text();
			}
			else
			{
				HadoopKerberosName renewerKrbName = new HadoopKerberosName(renewer.ToString());
				try
				{
					this.renewer = new Text(renewerKrbName.GetShortName());
				}
				catch (IOException e)
				{
					throw new RuntimeException(e);
				}
			}
		}

		public virtual Text GetRealUser()
		{
			return realUser;
		}

		public virtual void SetRealUser(Text realUser)
		{
			if (realUser == null)
			{
				this.realUser = new Text();
			}
			else
			{
				this.realUser = realUser;
			}
		}

		public virtual void SetIssueDate(long issueDate)
		{
			this.issueDate = issueDate;
		}

		public virtual long GetIssueDate()
		{
			return issueDate;
		}

		public virtual void SetMaxDate(long maxDate)
		{
			this.maxDate = maxDate;
		}

		public virtual long GetMaxDate()
		{
			return maxDate;
		}

		public virtual void SetSequenceNumber(int seqNum)
		{
			this.sequenceNumber = seqNum;
		}

		public virtual int GetSequenceNumber()
		{
			return sequenceNumber;
		}

		public virtual void SetMasterKeyId(int newId)
		{
			masterKeyId = newId;
		}

		public virtual int GetMasterKeyId()
		{
			return masterKeyId;
		}

		protected internal static bool IsEqual(object a, object b)
		{
			return a == null ? b == null : a.Equals(b);
		}

		public override bool Equals(object obj)
		{
			if (obj == this)
			{
				return true;
			}
			if (obj is Org.Apache.Hadoop.Security.Token.Delegation.AbstractDelegationTokenIdentifier)
			{
				Org.Apache.Hadoop.Security.Token.Delegation.AbstractDelegationTokenIdentifier that
					 = (Org.Apache.Hadoop.Security.Token.Delegation.AbstractDelegationTokenIdentifier
					)obj;
				return this.sequenceNumber == that.sequenceNumber && this.issueDate == that.issueDate
					 && this.maxDate == that.maxDate && this.masterKeyId == that.masterKeyId && IsEqual
					(this.owner, that.owner) && IsEqual(this.renewer, that.renewer) && IsEqual(this.
					realUser, that.realUser);
			}
			return false;
		}

		public override int GetHashCode()
		{
			return this.sequenceNumber;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ReadFields(BinaryReader @in)
		{
			byte version = @in.ReadByte();
			if (version != Version)
			{
				throw new IOException("Unknown version of delegation token " + version);
			}
			owner.ReadFields(@in, Text.DefaultMaxLen);
			renewer.ReadFields(@in, Text.DefaultMaxLen);
			realUser.ReadFields(@in, Text.DefaultMaxLen);
			issueDate = WritableUtils.ReadVLong(@in);
			maxDate = WritableUtils.ReadVLong(@in);
			sequenceNumber = WritableUtils.ReadVInt(@in);
			masterKeyId = WritableUtils.ReadVInt(@in);
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual void WriteImpl(BinaryWriter @out)
		{
			@out.WriteByte(Version);
			owner.Write(@out);
			renewer.Write(@out);
			realUser.Write(@out);
			WritableUtils.WriteVLong(@out, issueDate);
			WritableUtils.WriteVLong(@out, maxDate);
			WritableUtils.WriteVInt(@out, sequenceNumber);
			WritableUtils.WriteVInt(@out, masterKeyId);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(BinaryWriter @out)
		{
			if (owner.Length> Text.DefaultMaxLen)
			{
				throw new IOException("owner is too long to be serialized!");
			}
			if (renewer.Length> Text.DefaultMaxLen)
			{
				throw new IOException("renewer is too long to be serialized!");
			}
			if (realUser.Length> Text.DefaultMaxLen)
			{
				throw new IOException("realuser is too long to be serialized!");
			}
			WriteImpl(@out);
		}

		public override string ToString()
		{
			StringBuilder buffer = new StringBuilder();
			buffer.Append("owner=" + owner + ", renewer=" + renewer + ", realUser=" + realUser
				 + ", issueDate=" + issueDate + ", maxDate=" + maxDate + ", sequenceNumber=" + sequenceNumber
				 + ", masterKeyId=" + masterKeyId);
			return buffer.ToString();
		}
	}
}
