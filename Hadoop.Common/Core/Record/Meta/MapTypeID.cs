using Org.Apache.Hadoop.Record;
using Sharpen;

namespace Org.Apache.Hadoop.Record.Meta
{
	/// <summary>Represents typeID for a Map</summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class MapTypeID : TypeID
	{
		private TypeID typeIDKey;

		private TypeID typeIDValue;

		public MapTypeID(TypeID typeIDKey, TypeID typeIDValue)
			: base(TypeID.RIOType.Map)
		{
			this.typeIDKey = typeIDKey;
			this.typeIDValue = typeIDValue;
		}

		/// <summary>get the TypeID of the map's key element</summary>
		public virtual TypeID GetKeyTypeID()
		{
			return this.typeIDKey;
		}

		/// <summary>get the TypeID of the map's value element</summary>
		public virtual TypeID GetValueTypeID()
		{
			return this.typeIDValue;
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void Write(RecordOutput rout, string tag)
		{
			rout.WriteByte(typeVal, tag);
			typeIDKey.Write(rout, tag);
			typeIDValue.Write(rout, tag);
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
			Org.Apache.Hadoop.Record.Meta.MapTypeID mti = (Org.Apache.Hadoop.Record.Meta.MapTypeID
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
