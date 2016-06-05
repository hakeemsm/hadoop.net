using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Record;


namespace Org.Apache.Hadoop.Record.Meta
{
	/// <summary>Various utility functions for Hadooop record I/O platform.</summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class Utils
	{
		/// <summary>Cannot create a new instance of Utils</summary>
		private Utils()
		{
		}

		/// <summary>read/skip bytes from stream based on a type</summary>
		/// <exception cref="System.IO.IOException"/>
		public static void Skip(RecordInput rin, string tag, TypeID typeID)
		{
			switch (typeID.typeVal)
			{
				case TypeID.RIOType.Bool:
				{
					rin.ReadBool(tag);
					break;
				}

				case TypeID.RIOType.Buffer:
				{
					rin.ReadBuffer(tag);
					break;
				}

				case TypeID.RIOType.Byte:
				{
					rin.ReadByte(tag);
					break;
				}

				case TypeID.RIOType.Double:
				{
					rin.ReadDouble(tag);
					break;
				}

				case TypeID.RIOType.Float:
				{
					rin.ReadFloat(tag);
					break;
				}

				case TypeID.RIOType.Int:
				{
					rin.ReadInt(tag);
					break;
				}

				case TypeID.RIOType.Long:
				{
					rin.ReadLong(tag);
					break;
				}

				case TypeID.RIOType.Map:
				{
					Index midx1 = rin.StartMap(tag);
					MapTypeID mtID = (MapTypeID)typeID;
					for (; !midx1.Done(); midx1.Incr())
					{
						Skip(rin, tag, mtID.GetKeyTypeID());
						Skip(rin, tag, mtID.GetValueTypeID());
					}
					rin.EndMap(tag);
					break;
				}

				case TypeID.RIOType.String:
				{
					rin.ReadString(tag);
					break;
				}

				case TypeID.RIOType.Struct:
				{
					rin.StartRecord(tag);
					// read past each field in the struct
					StructTypeID stID = (StructTypeID)typeID;
					IEnumerator<FieldTypeInfo> it = stID.GetFieldTypeInfos().GetEnumerator();
					while (it.HasNext())
					{
						FieldTypeInfo tInfo = it.Next();
						Skip(rin, tag, tInfo.GetTypeID());
					}
					rin.EndRecord(tag);
					break;
				}

				case TypeID.RIOType.Vector:
				{
					Index vidx1 = rin.StartVector(tag);
					VectorTypeID vtID = (VectorTypeID)typeID;
					for (; !vidx1.Done(); vidx1.Incr())
					{
						Skip(rin, tag, vtID.GetElementTypeID());
					}
					rin.EndVector(tag);
					break;
				}

				default:
				{
					// shouldn't be here
					throw new IOException("Unknown typeID when skipping bytes");
				}
			}
		}
	}
}
