using Sharpen;

namespace Org.Apache.Hadoop.IO.Serializer.Avro
{
	public class Record
	{
		public int x = 7;

		public override int GetHashCode()
		{
			return x;
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
			Record other = (Record)obj;
			if (x != other.x)
			{
				return false;
			}
			return true;
		}
	}
}
