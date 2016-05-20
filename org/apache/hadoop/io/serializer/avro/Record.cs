using Sharpen;

namespace org.apache.hadoop.io.serializer.avro
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
			if (Sharpen.Runtime.getClassForObject(this) != Sharpen.Runtime.getClassForObject(
				obj))
			{
				return false;
			}
			org.apache.hadoop.io.serializer.avro.Record other = (org.apache.hadoop.io.serializer.avro.Record
				)obj;
			if (x != other.x)
			{
				return false;
			}
			return true;
		}
	}
}
