using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records
{
	/// <summary>The epoch information of RM for work-preserving restart.</summary>
	/// <remarks>
	/// The epoch information of RM for work-preserving restart.
	/// Epoch is incremented each time RM restart. It's used for assuring
	/// uniqueness of <code>ContainerId</code>.
	/// </remarks>
	public abstract class Epoch
	{
		public static Epoch NewInstance(long sequenceNumber)
		{
			Epoch epoch = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<Epoch>();
			epoch.SetEpoch(sequenceNumber);
			return epoch;
		}

		public abstract long GetEpoch();

		public abstract void SetEpoch(long sequenceNumber);

		public abstract YarnServerResourceManagerRecoveryProtos.EpochProto GetProto();

		public override string ToString()
		{
			return GetEpoch().ToString();
		}

		public override int GetHashCode()
		{
			return (int)(GetEpoch() ^ ((long)(((ulong)GetEpoch()) >> 32)));
		}

		public override bool Equals(object obj)
		{
			if (this == obj)
			{
				return true;
			}
			if (obj == null)
			{
				return false;
			}
			if (GetType() != obj.GetType())
			{
				return false;
			}
			Epoch other = (Epoch)obj;
			if (this.GetEpoch() == other.GetEpoch())
			{
				return true;
			}
			else
			{
				return false;
			}
		}
	}
}
