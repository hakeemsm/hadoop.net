using Sharpen;

namespace org.apache.hadoop.record.meta
{
	/// <summary>Represents typeID for a Map</summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class MapTypeID : org.apache.hadoop.record.meta.TypeID
	{
		private org.apache.hadoop.record.meta.TypeID typeIDKey;

		private org.apache.hadoop.record.meta.TypeID typeIDValue;

		public MapTypeID(org.apache.hadoop.record.meta.TypeID typeIDKey, org.apache.hadoop.record.meta.TypeID
			 typeIDValue)
			: base(org.apache.hadoop.record.meta.TypeID.RIOType.MAP)
		{
			this.typeIDKey = typeIDKey;
			this.typeIDValue = typeIDValue;
		}

		/// <summary>get the TypeID of the map's key element</summary>
		public virtual org.apache.hadoop.record.meta.TypeID getKeyTypeID()
		{
			return this.typeIDKey;
		}

		/// <summary>get the TypeID of the map's value element</summary>
		public virtual org.apache.hadoop.record.meta.TypeID getValueTypeID()
		{
			return this.typeIDValue;
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void write(org.apache.hadoop.record.RecordOutput rout, string tag
			)
		{
			rout.writeByte(typeVal, tag);
			typeIDKey.write(rout, tag);
			typeIDValue.write(rout, tag);
		}

		/// <summary>
		/// Two map  typeIDs are equal if their constituent elements have the
		/// same type
		/// </summary>
		public override bool Equals(object o)
		{
			if (!base.Equals(o))
			{
				return false;
			}
			org.apache.hadoop.record.meta.MapTypeID mti = (org.apache.hadoop.record.meta.MapTypeID
				)o;
			return this.typeIDKey.Equals(mti.typeIDKey) && this.typeIDValue.Equals(mti.typeIDValue
				);
		}

		/// <summary>
		/// We use a basic hashcode implementation, since this class will likely not
		/// be used as a hashmap key
		/// </summary>
		public override int GetHashCode()
		{
			return 37 * 17 + typeIDKey.GetHashCode() + 37 * 17 + typeIDValue.GetHashCode();
		}
	}
}
