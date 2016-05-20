using Sharpen;

namespace org.apache.hadoop.record.meta
{
	/// <summary>
	/// Represents a type information for a field, which is made up of its
	/// ID (name) and its type (a TypeID object).
	/// </summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class FieldTypeInfo
	{
		private string fieldID;

		private org.apache.hadoop.record.meta.TypeID typeID;

		/// <summary>Construct a FiledTypeInfo with the given field name and the type</summary>
		internal FieldTypeInfo(string fieldID, org.apache.hadoop.record.meta.TypeID typeID
			)
		{
			this.fieldID = fieldID;
			this.typeID = typeID;
		}

		/// <summary>get the field's TypeID object</summary>
		public virtual org.apache.hadoop.record.meta.TypeID getTypeID()
		{
			return typeID;
		}

		/// <summary>get the field's id (name)</summary>
		public virtual string getFieldID()
		{
			return fieldID;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void write(org.apache.hadoop.record.RecordOutput rout, string tag
			)
		{
			rout.writeString(fieldID, tag);
			typeID.write(rout, tag);
		}

		/// <summary>Two FieldTypeInfos are equal if ach of their fields matches</summary>
		public override bool Equals(object o)
		{
			if (this == o)
			{
				return true;
			}
			if (!(o is org.apache.hadoop.record.meta.FieldTypeInfo))
			{
				return false;
			}
			org.apache.hadoop.record.meta.FieldTypeInfo fti = (org.apache.hadoop.record.meta.FieldTypeInfo
				)o;
			// first check if fieldID matches
			if (!this.fieldID.Equals(fti.fieldID))
			{
				return false;
			}
			// now see if typeID matches
			return (this.typeID.Equals(fti.typeID));
		}

		/// <summary>
		/// We use a basic hashcode implementation, since this class will likely not
		/// be used as a hashmap key
		/// </summary>
		public override int GetHashCode()
		{
			return 37 * 17 + typeID.GetHashCode() + 37 * 17 + fieldID.GetHashCode();
		}

		public virtual bool equals(org.apache.hadoop.record.meta.FieldTypeInfo ti)
		{
			// first check if fieldID matches
			if (!this.fieldID.Equals(ti.fieldID))
			{
				return false;
			}
			// now see if typeID matches
			return (this.typeID.Equals(ti.typeID));
		}
	}
}
