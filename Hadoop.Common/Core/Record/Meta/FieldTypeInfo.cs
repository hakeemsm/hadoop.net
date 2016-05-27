using Org.Apache.Hadoop.Record;
using Sharpen;

namespace Org.Apache.Hadoop.Record.Meta
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

		private TypeID typeID;

		/// <summary>Construct a FiledTypeInfo with the given field name and the type</summary>
		internal FieldTypeInfo(string fieldID, TypeID typeID)
		{
			this.fieldID = fieldID;
			this.typeID = typeID;
		}

		/// <summary>get the field's TypeID object</summary>
		public virtual TypeID GetTypeID()
		{
			return typeID;
		}

		/// <summary>get the field's id (name)</summary>
		public virtual string GetFieldID()
		{
			return fieldID;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void Write(RecordOutput rout, string tag)
		{
			rout.WriteString(fieldID, tag);
			typeID.Write(rout, tag);
		}

		/// <summary>Two FieldTypeInfos are equal if ach of their fields matches</summary>
		public override bool Equals(object o)
		{
			if (this == o)
			{
				return true;
			}
			if (!(o is Org.Apache.Hadoop.Record.Meta.FieldTypeInfo))
			{
				return false;
			}
			Org.Apache.Hadoop.Record.Meta.FieldTypeInfo fti = (Org.Apache.Hadoop.Record.Meta.FieldTypeInfo
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

		public virtual bool Equals(Org.Apache.Hadoop.Record.Meta.FieldTypeInfo ti)
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
