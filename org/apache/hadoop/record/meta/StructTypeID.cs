using Sharpen;

namespace org.apache.hadoop.record.meta
{
	/// <summary>Represents typeID for a struct</summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class StructTypeID : org.apache.hadoop.record.meta.TypeID
	{
		private System.Collections.Generic.List<org.apache.hadoop.record.meta.FieldTypeInfo
			> typeInfos = new System.Collections.Generic.List<org.apache.hadoop.record.meta.FieldTypeInfo
			>();

		internal StructTypeID()
			: base(org.apache.hadoop.record.meta.TypeID.RIOType.STRUCT)
		{
		}

		/// <summary>Create a StructTypeID based on the RecordTypeInfo of some record</summary>
		public StructTypeID(org.apache.hadoop.record.meta.RecordTypeInfo rti)
			: base(org.apache.hadoop.record.meta.TypeID.RIOType.STRUCT)
		{
			Sharpen.Collections.AddAll(typeInfos, rti.getFieldTypeInfos());
		}

		internal virtual void add(org.apache.hadoop.record.meta.FieldTypeInfo ti)
		{
			typeInfos.add(ti);
		}

		public virtual System.Collections.Generic.ICollection<org.apache.hadoop.record.meta.FieldTypeInfo
			> getFieldTypeInfos()
		{
			return typeInfos;
		}

		/*
		* return the StructTypeiD, if any, of the given field
		*/
		internal virtual org.apache.hadoop.record.meta.StructTypeID findStruct(string name
			)
		{
			// walk through the list, searching. Not the most efficient way, but this
			// in intended to be used rarely, so we keep it simple. 
			// As an optimization, we can keep a hashmap of record name to its RTI, for later.
			foreach (org.apache.hadoop.record.meta.FieldTypeInfo ti in typeInfos)
			{
				if ((0 == string.CompareOrdinal(ti.getFieldID(), name)) && (ti.getTypeID().getTypeVal
					() == org.apache.hadoop.record.meta.TypeID.RIOType.STRUCT))
				{
					return (org.apache.hadoop.record.meta.StructTypeID)ti.getTypeID();
				}
			}
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void write(org.apache.hadoop.record.RecordOutput rout, string tag
			)
		{
			rout.writeByte(typeVal, tag);
			writeRest(rout, tag);
		}

		/*
		* Writes rest of the struct (excluding type value).
		* As an optimization, this method is directly called by RTI
		* for the top level record so that we don't write out the byte
		* indicating that this is a struct (since top level records are
		* always structs).
		*/
		/// <exception cref="System.IO.IOException"/>
		internal virtual void writeRest(org.apache.hadoop.record.RecordOutput rout, string
			 tag)
		{
			rout.writeInt(typeInfos.Count, tag);
			foreach (org.apache.hadoop.record.meta.FieldTypeInfo ti in typeInfos)
			{
				ti.write(rout, tag);
			}
		}

		/*
		* deserialize ourselves. Called by RTI.
		*/
		/// <exception cref="System.IO.IOException"/>
		internal virtual void read(org.apache.hadoop.record.RecordInput rin, string tag)
		{
			// number of elements
			int numElems = rin.readInt(tag);
			for (int i = 0; i < numElems; i++)
			{
				typeInfos.add(genericReadTypeInfo(rin, tag));
			}
		}

		// generic reader: reads the next TypeInfo object from stream and returns it
		/// <exception cref="System.IO.IOException"/>
		private org.apache.hadoop.record.meta.FieldTypeInfo genericReadTypeInfo(org.apache.hadoop.record.RecordInput
			 rin, string tag)
		{
			string fieldName = rin.readString(tag);
			org.apache.hadoop.record.meta.TypeID id = genericReadTypeID(rin, tag);
			return new org.apache.hadoop.record.meta.FieldTypeInfo(fieldName, id);
		}

		// generic reader: reads the next TypeID object from stream and returns it
		/// <exception cref="System.IO.IOException"/>
		private org.apache.hadoop.record.meta.TypeID genericReadTypeID(org.apache.hadoop.record.RecordInput
			 rin, string tag)
		{
			byte typeVal = rin.readByte(tag);
			switch (typeVal)
			{
				case org.apache.hadoop.record.meta.TypeID.RIOType.BOOL:
				{
					return org.apache.hadoop.record.meta.TypeID.BoolTypeID;
				}

				case org.apache.hadoop.record.meta.TypeID.RIOType.BUFFER:
				{
					return org.apache.hadoop.record.meta.TypeID.BufferTypeID;
				}

				case org.apache.hadoop.record.meta.TypeID.RIOType.BYTE:
				{
					return org.apache.hadoop.record.meta.TypeID.ByteTypeID;
				}

				case org.apache.hadoop.record.meta.TypeID.RIOType.DOUBLE:
				{
					return org.apache.hadoop.record.meta.TypeID.DoubleTypeID;
				}

				case org.apache.hadoop.record.meta.TypeID.RIOType.FLOAT:
				{
					return org.apache.hadoop.record.meta.TypeID.FloatTypeID;
				}

				case org.apache.hadoop.record.meta.TypeID.RIOType.INT:
				{
					return org.apache.hadoop.record.meta.TypeID.IntTypeID;
				}

				case org.apache.hadoop.record.meta.TypeID.RIOType.LONG:
				{
					return org.apache.hadoop.record.meta.TypeID.LongTypeID;
				}

				case org.apache.hadoop.record.meta.TypeID.RIOType.MAP:
				{
					org.apache.hadoop.record.meta.TypeID tIDKey = genericReadTypeID(rin, tag);
					org.apache.hadoop.record.meta.TypeID tIDValue = genericReadTypeID(rin, tag);
					return new org.apache.hadoop.record.meta.MapTypeID(tIDKey, tIDValue);
				}

				case org.apache.hadoop.record.meta.TypeID.RIOType.STRING:
				{
					return org.apache.hadoop.record.meta.TypeID.StringTypeID;
				}

				case org.apache.hadoop.record.meta.TypeID.RIOType.STRUCT:
				{
					org.apache.hadoop.record.meta.StructTypeID stID = new org.apache.hadoop.record.meta.StructTypeID
						();
					int numElems = rin.readInt(tag);
					for (int i = 0; i < numElems; i++)
					{
						stID.add(genericReadTypeInfo(rin, tag));
					}
					return stID;
				}

				case org.apache.hadoop.record.meta.TypeID.RIOType.VECTOR:
				{
					org.apache.hadoop.record.meta.TypeID tID = genericReadTypeID(rin, tag);
					return new org.apache.hadoop.record.meta.VectorTypeID(tID);
				}

				default:
				{
					// shouldn't be here
					throw new System.IO.IOException("Unknown type read");
				}
			}
		}

		public override bool Equals(object o)
		{
			return base.Equals(o);
		}

		public override int GetHashCode()
		{
			return base.GetHashCode();
		}
	}
}
