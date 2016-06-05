using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Record;


namespace Org.Apache.Hadoop.Record.Meta
{
	/// <summary>Represents typeID for a struct</summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class StructTypeID : TypeID
	{
		private AList<FieldTypeInfo> typeInfos = new AList<FieldTypeInfo>();

		internal StructTypeID()
			: base(TypeID.RIOType.Struct)
		{
		}

		/// <summary>Create a StructTypeID based on the RecordTypeInfo of some record</summary>
		public StructTypeID(RecordTypeInfo rti)
			: base(TypeID.RIOType.Struct)
		{
			Collections.AddAll(typeInfos, rti.GetFieldTypeInfos());
		}

		internal virtual void Add(FieldTypeInfo ti)
		{
			typeInfos.AddItem(ti);
		}

		public virtual ICollection<FieldTypeInfo> GetFieldTypeInfos()
		{
			return typeInfos;
		}

		/*
		* return the StructTypeiD, if any, of the given field
		*/
		internal virtual Org.Apache.Hadoop.Record.Meta.StructTypeID FindStruct(string name
			)
		{
			// walk through the list, searching. Not the most efficient way, but this
			// in intended to be used rarely, so we keep it simple. 
			// As an optimization, we can keep a hashmap of record name to its RTI, for later.
			foreach (FieldTypeInfo ti in typeInfos)
			{
				if ((0 == string.CompareOrdinal(ti.GetFieldID(), name)) && (ti.GetTypeID().GetTypeVal
					() == TypeID.RIOType.Struct))
				{
					return (Org.Apache.Hadoop.Record.Meta.StructTypeID)ti.GetTypeID();
				}
			}
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void Write(RecordOutput rout, string tag)
		{
			rout.WriteByte(typeVal, tag);
			WriteRest(rout, tag);
		}

		/*
		* Writes rest of the struct (excluding type value).
		* As an optimization, this method is directly called by RTI
		* for the top level record so that we don't write out the byte
		* indicating that this is a struct (since top level records are
		* always structs).
		*/
		/// <exception cref="System.IO.IOException"/>
		internal virtual void WriteRest(RecordOutput rout, string tag)
		{
			rout.WriteInt(typeInfos.Count, tag);
			foreach (FieldTypeInfo ti in typeInfos)
			{
				ti.Write(rout, tag);
			}
		}

		/*
		* deserialize ourselves. Called by RTI.
		*/
		/// <exception cref="System.IO.IOException"/>
		internal virtual void Read(RecordInput rin, string tag)
		{
			// number of elements
			int numElems = rin.ReadInt(tag);
			for (int i = 0; i < numElems; i++)
			{
				typeInfos.AddItem(GenericReadTypeInfo(rin, tag));
			}
		}

		// generic reader: reads the next TypeInfo object from stream and returns it
		/// <exception cref="System.IO.IOException"/>
		private FieldTypeInfo GenericReadTypeInfo(RecordInput rin, string tag)
		{
			string fieldName = rin.ReadString(tag);
			TypeID id = GenericReadTypeID(rin, tag);
			return new FieldTypeInfo(fieldName, id);
		}

		// generic reader: reads the next TypeID object from stream and returns it
		/// <exception cref="System.IO.IOException"/>
		private TypeID GenericReadTypeID(RecordInput rin, string tag)
		{
			byte typeVal = rin.ReadByte(tag);
			switch (typeVal)
			{
				case TypeID.RIOType.Bool:
				{
					return TypeID.BoolTypeID;
				}

				case TypeID.RIOType.Buffer:
				{
					return TypeID.BufferTypeID;
				}

				case TypeID.RIOType.Byte:
				{
					return TypeID.ByteTypeID;
				}

				case TypeID.RIOType.Double:
				{
					return TypeID.DoubleTypeID;
				}

				case TypeID.RIOType.Float:
				{
					return TypeID.FloatTypeID;
				}

				case TypeID.RIOType.Int:
				{
					return TypeID.IntTypeID;
				}

				case TypeID.RIOType.Long:
				{
					return TypeID.LongTypeID;
				}

				case TypeID.RIOType.Map:
				{
					TypeID tIDKey = GenericReadTypeID(rin, tag);
					TypeID tIDValue = GenericReadTypeID(rin, tag);
					return new MapTypeID(tIDKey, tIDValue);
				}

				case TypeID.RIOType.String:
				{
					return TypeID.StringTypeID;
				}

				case TypeID.RIOType.Struct:
				{
					Org.Apache.Hadoop.Record.Meta.StructTypeID stID = new Org.Apache.Hadoop.Record.Meta.StructTypeID
						();
					int numElems = rin.ReadInt(tag);
					for (int i = 0; i < numElems; i++)
					{
						stID.Add(GenericReadTypeInfo(rin, tag));
					}
					return stID;
				}

				case TypeID.RIOType.Vector:
				{
					TypeID tID = GenericReadTypeID(rin, tag);
					return new VectorTypeID(tID);
				}

				default:
				{
					// shouldn't be here
					throw new IOException("Unknown type read");
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
