using Sharpen;

namespace org.apache.hadoop.security.token.delegation
{
	public abstract class AbstractDelegationTokenIdentifier : org.apache.hadoop.security.token.TokenIdentifier
	{
		private const byte VERSION = 0;

		private org.apache.hadoop.io.Text owner;

		private org.apache.hadoop.io.Text renewer;

		private org.apache.hadoop.io.Text realUser;

		private long issueDate;

		private long maxDate;

		private int sequenceNumber;

		private int masterKeyId = 0;

		public AbstractDelegationTokenIdentifier()
			: this(new org.apache.hadoop.io.Text(), new org.apache.hadoop.io.Text(), new org.apache.hadoop.io.Text
				())
		{
		}

		public AbstractDelegationTokenIdentifier(org.apache.hadoop.io.Text owner, org.apache.hadoop.io.Text
			 renewer, org.apache.hadoop.io.Text realUser)
		{
			setOwner(owner);
			setRenewer(renewer);
			setRealUser(realUser);
			issueDate = 0;
			maxDate = 0;
		}

		public abstract override org.apache.hadoop.io.Text getKind();

		/// <summary>Get the username encoded in the token identifier</summary>
		/// <returns>the username or owner</returns>
		public override org.apache.hadoop.security.UserGroupInformation getUser()
		{
			if ((owner == null) || (owner.ToString().isEmpty()))
			{
				return null;
			}
			org.apache.hadoop.security.UserGroupInformation realUgi;
			org.apache.hadoop.security.UserGroupInformation ugi;
			if ((realUser == null) || (realUser.ToString().isEmpty()) || realUser.Equals(owner
				))
			{
				ugi = realUgi = org.apache.hadoop.security.UserGroupInformation.createRemoteUser(
					owner.ToString());
			}
			else
			{
				realUgi = org.apache.hadoop.security.UserGroupInformation.createRemoteUser(realUser
					.ToString());
				ugi = org.apache.hadoop.security.UserGroupInformation.createProxyUser(owner.ToString
					(), realUgi);
			}
			realUgi.setAuthenticationMethod(org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
				.TOKEN);
			return ugi;
		}

		public virtual org.apache.hadoop.io.Text getOwner()
		{
			return owner;
		}

		public virtual void setOwner(org.apache.hadoop.io.Text owner)
		{
			if (owner == null)
			{
				this.owner = new org.apache.hadoop.io.Text();
			}
			else
			{
				this.owner = owner;
			}
		}

		public virtual org.apache.hadoop.io.Text getRenewer()
		{
			return renewer;
		}

		public virtual void setRenewer(org.apache.hadoop.io.Text renewer)
		{
			if (renewer == null)
			{
				this.renewer = new org.apache.hadoop.io.Text();
			}
			else
			{
				org.apache.hadoop.security.HadoopKerberosName renewerKrbName = new org.apache.hadoop.security.HadoopKerberosName
					(renewer.ToString());
				try
				{
					this.renewer = new org.apache.hadoop.io.Text(renewerKrbName.getShortName());
				}
				catch (System.IO.IOException e)
				{
					throw new System.Exception(e);
				}
			}
		}

		public virtual org.apache.hadoop.io.Text getRealUser()
		{
			return realUser;
		}

		public virtual void setRealUser(org.apache.hadoop.io.Text realUser)
		{
			if (realUser == null)
			{
				this.realUser = new org.apache.hadoop.io.Text();
			}
			else
			{
				this.realUser = realUser;
			}
		}

		public virtual void setIssueDate(long issueDate)
		{
			this.issueDate = issueDate;
		}

		public virtual long getIssueDate()
		{
			return issueDate;
		}

		public virtual void setMaxDate(long maxDate)
		{
			this.maxDate = maxDate;
		}

		public virtual long getMaxDate()
		{
			return maxDate;
		}

		public virtual void setSequenceNumber(int seqNum)
		{
			this.sequenceNumber = seqNum;
		}

		public virtual int getSequenceNumber()
		{
			return sequenceNumber;
		}

		public virtual void setMasterKeyId(int newId)
		{
			masterKeyId = newId;
		}

		public virtual int getMasterKeyId()
		{
			return masterKeyId;
		}

		protected internal static bool isEqual(object a, object b)
		{
			return a == null ? b == null : a.Equals(b);
		}

		public override bool Equals(object obj)
		{
			if (obj == this)
			{
				return true;
			}
			if (obj is org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier)
			{
				org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier that
					 = (org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
					)obj;
				return this.sequenceNumber == that.sequenceNumber && this.issueDate == that.issueDate
					 && this.maxDate == that.maxDate && this.masterKeyId == that.masterKeyId && isEqual
					(this.owner, that.owner) && isEqual(this.renewer, that.renewer) && isEqual(this.
					realUser, that.realUser);
			}
			return false;
		}

		public override int GetHashCode()
		{
			return this.sequenceNumber;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void readFields(java.io.DataInput @in)
		{
			byte version = @in.readByte();
			if (version != VERSION)
			{
				throw new System.IO.IOException("Unknown version of delegation token " + version);
			}
			owner.readFields(@in, org.apache.hadoop.io.Text.DEFAULT_MAX_LEN);
			renewer.readFields(@in, org.apache.hadoop.io.Text.DEFAULT_MAX_LEN);
			realUser.readFields(@in, org.apache.hadoop.io.Text.DEFAULT_MAX_LEN);
			issueDate = org.apache.hadoop.io.WritableUtils.readVLong(@in);
			maxDate = org.apache.hadoop.io.WritableUtils.readVLong(@in);
			sequenceNumber = org.apache.hadoop.io.WritableUtils.readVInt(@in);
			masterKeyId = org.apache.hadoop.io.WritableUtils.readVInt(@in);
		}

		/// <exception cref="System.IO.IOException"/>
		[com.google.common.annotations.VisibleForTesting]
		internal virtual void writeImpl(java.io.DataOutput @out)
		{
			@out.writeByte(VERSION);
			owner.write(@out);
			renewer.write(@out);
			realUser.write(@out);
			org.apache.hadoop.io.WritableUtils.writeVLong(@out, issueDate);
			org.apache.hadoop.io.WritableUtils.writeVLong(@out, maxDate);
			org.apache.hadoop.io.WritableUtils.writeVInt(@out, sequenceNumber);
			org.apache.hadoop.io.WritableUtils.writeVInt(@out, masterKeyId);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void write(java.io.DataOutput @out)
		{
			if (owner.getLength() > org.apache.hadoop.io.Text.DEFAULT_MAX_LEN)
			{
				throw new System.IO.IOException("owner is too long to be serialized!");
			}
			if (renewer.getLength() > org.apache.hadoop.io.Text.DEFAULT_MAX_LEN)
			{
				throw new System.IO.IOException("renewer is too long to be serialized!");
			}
			if (realUser.getLength() > org.apache.hadoop.io.Text.DEFAULT_MAX_LEN)
			{
				throw new System.IO.IOException("realuser is too long to be serialized!");
			}
			writeImpl(@out);
		}

		public override string ToString()
		{
			java.lang.StringBuilder buffer = new java.lang.StringBuilder();
			buffer.Append("owner=" + owner + ", renewer=" + renewer + ", realUser=" + realUser
				 + ", issueDate=" + issueDate + ", maxDate=" + maxDate + ", sequenceNumber=" + sequenceNumber
				 + ", masterKeyId=" + masterKeyId);
			return buffer.ToString();
		}
	}
}
