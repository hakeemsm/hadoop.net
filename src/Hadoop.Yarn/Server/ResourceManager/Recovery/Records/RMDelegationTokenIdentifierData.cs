using System.IO;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records
{
	public class RMDelegationTokenIdentifierData
	{
		internal YarnServerResourceManagerRecoveryProtos.RMDelegationTokenIdentifierDataProto.Builder
			 builder = YarnServerResourceManagerRecoveryProtos.RMDelegationTokenIdentifierDataProto
			.NewBuilder();

		public RMDelegationTokenIdentifierData()
		{
		}

		public RMDelegationTokenIdentifierData(YARNDelegationTokenIdentifier identifier, 
			long renewdate)
		{
			builder.SetTokenIdentifier(identifier.GetProto());
			builder.SetRenewDate(renewdate);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			builder.MergeFrom((DataInputStream)@in);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual byte[] ToByteArray()
		{
			return ((YarnServerResourceManagerRecoveryProtos.RMDelegationTokenIdentifierDataProto
				)builder.Build()).ToByteArray();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual RMDelegationTokenIdentifier GetTokenIdentifier()
		{
			ByteArrayInputStream @in = new ByteArrayInputStream(builder.GetTokenIdentifier().
				ToByteArray());
			RMDelegationTokenIdentifier identifer = new RMDelegationTokenIdentifier();
			identifer.ReadFields(new DataInputStream(@in));
			return identifer;
		}

		public virtual long GetRenewDate()
		{
			return builder.GetRenewDate();
		}
	}
}
