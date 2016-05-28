using System.IO;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Join
{
	public class IncomparableKey : WritableComparable
	{
		public virtual void Write(DataOutput @out)
		{
		}

		public virtual void ReadFields(DataInput @in)
		{
		}

		public virtual int CompareTo(object o)
		{
			throw new RuntimeException("Should never see this.");
		}
	}
}
