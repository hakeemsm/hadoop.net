using Org.Apache.Hadoop.Record;


namespace Org.Apache.Hadoop.Record.Meta
{
	/// <summary>Represents typeID for vector.</summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class VectorTypeID : TypeID
	{
		private TypeID typeIDElement;

		public VectorTypeID(TypeID typeIDElement)
			: base(TypeID.RIOType.Vector)
		{
			this.typeIDElement = typeIDElement;
		}

		public virtual TypeID GetElementTypeID()
		{
			return this.typeIDElement;
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void Write(RecordOutput rout, string tag)
		{
			rout.WriteByte(typeVal, tag);
			typeIDElement.Write(rout, tag);
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
			Org.Apache.Hadoop.Record.Meta.VectorTypeID vti = (Org.Apache.Hadoop.Record.Meta.VectorTypeID
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
