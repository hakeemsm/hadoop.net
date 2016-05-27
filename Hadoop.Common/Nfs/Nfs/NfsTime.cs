using Org.Apache.Hadoop.Oncrpc;
using Sharpen;

namespace Org.Apache.Hadoop.Nfs
{
	/// <summary>Class that encapsulates time.</summary>
	public class NfsTime
	{
		internal const int MillisecondsInSecond = 1000;

		internal const int NanosecondsInMillisecond = 1000000;

		private readonly int seconds;

		private readonly int nseconds;

		public NfsTime(int seconds, int nseconds)
		{
			this.seconds = seconds;
			this.nseconds = nseconds;
		}

		public NfsTime(Org.Apache.Hadoop.Nfs.NfsTime other)
		{
			seconds = other.GetNseconds();
			nseconds = other.GetNseconds();
		}

		public NfsTime(long milliseconds)
		{
			seconds = (int)(milliseconds / MillisecondsInSecond);
			nseconds = (int)((milliseconds - this.seconds * MillisecondsInSecond) * NanosecondsInMillisecond
				);
		}

		public virtual int GetSeconds()
		{
			return seconds;
		}

		public virtual int GetNseconds()
		{
			return nseconds;
		}

		/// <summary>Get the total time in milliseconds</summary>
		public virtual long GetMilliSeconds()
		{
			return (long)(seconds) * 1000 + (long)(nseconds) / 1000000;
		}

		public virtual void Serialize(XDR xdr)
		{
			xdr.WriteInt(GetSeconds());
			xdr.WriteInt(GetNseconds());
		}

		public static Org.Apache.Hadoop.Nfs.NfsTime Deserialize(XDR xdr)
		{
			return new Org.Apache.Hadoop.Nfs.NfsTime(xdr.ReadInt(), xdr.ReadInt());
		}

		public override int GetHashCode()
		{
			return seconds ^ nseconds;
		}

		public override bool Equals(object o)
		{
			if (!(o is Org.Apache.Hadoop.Nfs.NfsTime))
			{
				return false;
			}
			return ((Org.Apache.Hadoop.Nfs.NfsTime)o).GetMilliSeconds() == this.GetMilliSeconds
				();
		}

		public override string ToString()
		{
			return "(NfsTime-" + seconds + "s, " + nseconds + "ns)";
		}
	}
}
