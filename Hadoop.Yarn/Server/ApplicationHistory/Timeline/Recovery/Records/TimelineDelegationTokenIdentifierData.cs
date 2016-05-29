using System.IO;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Timeline.Recovery.Records
{
	public class TimelineDelegationTokenIdentifierData
	{
		internal YarnServerTimelineServerRecoveryProtos.TimelineDelegationTokenIdentifierDataProto.Builder
			 builder = YarnServerTimelineServerRecoveryProtos.TimelineDelegationTokenIdentifierDataProto
			.NewBuilder();

		public TimelineDelegationTokenIdentifierData()
		{
		}

		public TimelineDelegationTokenIdentifierData(TimelineDelegationTokenIdentifier identifier
			, long renewdate)
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
			return ((YarnServerTimelineServerRecoveryProtos.TimelineDelegationTokenIdentifierDataProto
				)builder.Build()).ToByteArray();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual TimelineDelegationTokenIdentifier GetTokenIdentifier()
		{
			ByteArrayInputStream @in = new ByteArrayInputStream(builder.GetTokenIdentifier().
				ToByteArray());
			TimelineDelegationTokenIdentifier identifer = new TimelineDelegationTokenIdentifier
				();
			identifer.ReadFields(new DataInputStream(@in));
			return identifer;
		}

		public virtual long GetRenewDate()
		{
			return builder.GetRenewDate();
		}
	}
}
