using Sharpen;

namespace org.apache.hadoop.record.meta
{
	/// <summary>Represents typeID for basic types.</summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class TypeID
	{
		/// <summary>constants representing the IDL types we support</summary>
		public sealed class RIOType
		{
			public const byte BOOL = 1;

			public const byte BUFFER = 2;

			public const byte BYTE = 3;

			public const byte DOUBLE = 4;

			public const byte FLOAT = 5;

			public const byte INT = 6;

			public const byte LONG = 7;

			public const byte MAP = 8;

			public const byte STRING = 9;

			public const byte STRUCT = 10;

			public const byte VECTOR = 11;
		}

		/// <summary>Constant classes for the basic types, so we can share them.</summary>
		public static readonly org.apache.hadoop.record.meta.TypeID BoolTypeID = new org.apache.hadoop.record.meta.TypeID
			(org.apache.hadoop.record.meta.TypeID.RIOType.BOOL);

		public static readonly org.apache.hadoop.record.meta.TypeID BufferTypeID = new org.apache.hadoop.record.meta.TypeID
			(org.apache.hadoop.record.meta.TypeID.RIOType.BUFFER);

		public static readonly org.apache.hadoop.record.meta.TypeID ByteTypeID = new org.apache.hadoop.record.meta.TypeID
			(org.apache.hadoop.record.meta.TypeID.RIOType.BYTE);

		public static readonly org.apache.hadoop.record.meta.TypeID DoubleTypeID = new org.apache.hadoop.record.meta.TypeID
			(org.apache.hadoop.record.meta.TypeID.RIOType.DOUBLE);

		public static readonly org.apache.hadoop.record.meta.TypeID FloatTypeID = new org.apache.hadoop.record.meta.TypeID
			(org.apache.hadoop.record.meta.TypeID.RIOType.FLOAT);

		public static readonly org.apache.hadoop.record.meta.TypeID IntTypeID = new org.apache.hadoop.record.meta.TypeID
			(org.apache.hadoop.record.meta.TypeID.RIOType.INT);

		public static readonly org.apache.hadoop.record.meta.TypeID LongTypeID = new org.apache.hadoop.record.meta.TypeID
			(org.apache.hadoop.record.meta.TypeID.RIOType.LONG);

		public static readonly org.apache.hadoop.record.meta.TypeID StringTypeID = new org.apache.hadoop.record.meta.TypeID
			(org.apache.hadoop.record.meta.TypeID.RIOType.STRING);

		protected internal byte typeVal;

		/// <summary>Create a TypeID object</summary>
		internal TypeID(byte typeVal)
		{
			this.typeVal = typeVal;
		}

		/// <summary>Get the type value.</summary>
		/// <remarks>Get the type value. One of the constants in RIOType.</remarks>
		public virtual byte getTypeVal()
		{
			return typeVal;
		}

		/// <summary>Serialize the TypeID object</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void write(org.apache.hadoop.record.RecordOutput rout, string tag
			)
		{
			rout.writeByte(typeVal, tag);
		}

		/// <summary>Two base typeIDs are equal if they refer to the same type</summary>
		public override bool Equals(object o)
		{
			if (this == o)
			{
				return true;
			}
			if (o == null)
			{
				return false;
			}
			if (Sharpen.Runtime.getClassForObject(this) != Sharpen.Runtime.getClassForObject(
				o))
			{
				return false;
			}
			org.apache.hadoop.record.meta.TypeID oTypeID = (org.apache.hadoop.record.meta.TypeID
				)o;
			return (this.typeVal == oTypeID.typeVal);
		}

		/// <summary>
		/// We use a basic hashcode implementation, since this class will likely not
		/// be used as a hashmap key
		/// </summary>
		public override int GetHashCode()
		{
			// See 'Effectve Java' by Joshua Bloch
			return 37 * 17 + (int)typeVal;
		}
	}
}
