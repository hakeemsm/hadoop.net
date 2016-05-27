using Org.Apache.Hadoop.Record;
using Sharpen;

namespace Org.Apache.Hadoop.Record.Meta
{
	/// <summary>Represents typeID for basic types.</summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class TypeID
	{
		/// <summary>constants representing the IDL types we support</summary>
		public sealed class RIOType
		{
			public const byte Bool = 1;

			public const byte Buffer = 2;

			public const byte Byte = 3;

			public const byte Double = 4;

			public const byte Float = 5;

			public const byte Int = 6;

			public const byte Long = 7;

			public const byte Map = 8;

			public const byte String = 9;

			public const byte Struct = 10;

			public const byte Vector = 11;
		}

		/// <summary>Constant classes for the basic types, so we can share them.</summary>
		public static readonly TypeID BoolTypeID = new TypeID(TypeID.RIOType.Bool);

		public static readonly TypeID BufferTypeID = new TypeID(TypeID.RIOType.Buffer);

		public static readonly TypeID ByteTypeID = new TypeID(TypeID.RIOType.Byte);

		public static readonly TypeID DoubleTypeID = new TypeID(TypeID.RIOType.Double);

		public static readonly TypeID FloatTypeID = new TypeID(TypeID.RIOType.Float);

		public static readonly TypeID IntTypeID = new TypeID(TypeID.RIOType.Int);

		public static readonly TypeID LongTypeID = new TypeID(TypeID.RIOType.Long);

		public static readonly TypeID StringTypeID = new TypeID(TypeID.RIOType.String);

		protected internal byte typeVal;

		/// <summary>Create a TypeID object</summary>
		internal TypeID(byte typeVal)
		{
			this.typeVal = typeVal;
		}

		/// <summary>Get the type value.</summary>
		/// <remarks>Get the type value. One of the constants in RIOType.</remarks>
		public virtual byte GetTypeVal()
		{
			return typeVal;
		}

		/// <summary>Serialize the TypeID object</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void Write(RecordOutput rout, string tag)
		{
			rout.WriteByte(typeVal, tag);
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
			if (this.GetType() != o.GetType())
			{
				return false;
			}
			TypeID oTypeID = (TypeID)o;
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
