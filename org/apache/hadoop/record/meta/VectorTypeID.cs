using Sharpen;

namespace org.apache.hadoop.record.meta
{
	/// <summary>Represents typeID for vector.</summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class VectorTypeID : org.apache.hadoop.record.meta.TypeID
	{
		private org.apache.hadoop.record.meta.TypeID typeIDElement;

		public VectorTypeID(org.apache.hadoop.record.meta.TypeID typeIDElement)
			: base(org.apache.hadoop.record.meta.TypeID.RIOType.VECTOR)
		{
			this.typeIDElement = typeIDElement;
		}

		public virtual org.apache.hadoop.record.meta.TypeID getElementTypeID()
		{
			return this.typeIDElement;
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void write(org.apache.hadoop.record.RecordOutput rout, string tag
			)
		{
			rout.writeByte(typeVal, tag);
			typeIDElement.write(rout, tag);
		}

		/// <summary>
		/// Two vector typeIDs are equal if their constituent elements have the
		/// same type
		/// </summary>
		public override bool Equals(object o)
		{
			if (!base.Equals(o))
			{
				return false;
			}
			org.apache.hadoop.record.meta.VectorTypeID vti = (org.apache.hadoop.record.meta.VectorTypeID
				)o;
			return this.typeIDElement.Equals(vti.typeIDElement);
		}

		/// <summary>
		/// We use a basic hashcode implementation, since this class will likely not
		/// be used as a hashmap key
		/// </summary>
		public override int GetHashCode()
		{
			return 37 * 17 + typeIDElement.GetHashCode();
		}
	}
}
