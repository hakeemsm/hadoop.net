using Sharpen;

namespace org.apache.hadoop.record.meta
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
		public static void skip(org.apache.hadoop.record.RecordInput rin, string tag, org.apache.hadoop.record.meta.TypeID
			 typeID)
		{
			switch (typeID.typeVal)
			{
				case org.apache.hadoop.record.meta.TypeID.RIOType.BOOL:
				{
					rin.readBool(tag);
					break;
				}

				case org.apache.hadoop.record.meta.TypeID.RIOType.BUFFER:
				{
					rin.readBuffer(tag);
					break;
				}

				case org.apache.hadoop.record.meta.TypeID.RIOType.BYTE:
				{
					rin.readByte(tag);
					break;
				}

				case org.apache.hadoop.record.meta.TypeID.RIOType.DOUBLE:
				{
					rin.readDouble(tag);
					break;
				}

				case org.apache.hadoop.record.meta.TypeID.RIOType.FLOAT:
				{
					rin.readFloat(tag);
					break;
				}

				case org.apache.hadoop.record.meta.TypeID.RIOType.INT:
				{
					rin.readInt(tag);
					break;
				}

				case org.apache.hadoop.record.meta.TypeID.RIOType.LONG:
				{
					rin.readLong(tag);
					break;
				}

				case org.apache.hadoop.record.meta.TypeID.RIOType.MAP:
				{
					org.apache.hadoop.record.Index midx1 = rin.startMap(tag);
					org.apache.hadoop.record.meta.MapTypeID mtID = (org.apache.hadoop.record.meta.MapTypeID
						)typeID;
					for (; !midx1.done(); midx1.incr())
					{
						skip(rin, tag, mtID.getKeyTypeID());
						skip(rin, tag, mtID.getValueTypeID());
					}
					rin.endMap(tag);
					break;
				}

				case org.apache.hadoop.record.meta.TypeID.RIOType.STRING:
				{
					rin.readString(tag);
					break;
				}

				case org.apache.hadoop.record.meta.TypeID.RIOType.STRUCT:
				{
					rin.startRecord(tag);
					// read past each field in the struct
					org.apache.hadoop.record.meta.StructTypeID stID = (org.apache.hadoop.record.meta.StructTypeID
						)typeID;
					System.Collections.Generic.IEnumerator<org.apache.hadoop.record.meta.FieldTypeInfo
						> it = stID.getFieldTypeInfos().GetEnumerator();
					while (it.MoveNext())
					{
						org.apache.hadoop.record.meta.FieldTypeInfo tInfo = it.Current;
						skip(rin, tag, tInfo.getTypeID());
					}
					rin.endRecord(tag);
					break;
				}

				case org.apache.hadoop.record.meta.TypeID.RIOType.VECTOR:
				{
					org.apache.hadoop.record.Index vidx1 = rin.startVector(tag);
					org.apache.hadoop.record.meta.VectorTypeID vtID = (org.apache.hadoop.record.meta.VectorTypeID
						)typeID;
					for (; !vidx1.done(); vidx1.incr())
					{
						skip(rin, tag, vtID.getElementTypeID());
					}
					rin.endVector(tag);
					break;
				}

				default:
				{
					// shouldn't be here
					throw new System.IO.IOException("Unknown typeID when skipping bytes");
				}
			}
		}
	}
}
