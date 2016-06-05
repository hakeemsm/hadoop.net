using System;
using Org.Apache.Avro;
using Org.Apache.Avro.Data;
using Org.Apache.Avro.Specific;


namespace Org.Apache.Hadoop.IO.Serializer.Avro
{
	public class AvroRecord : SpecificRecordBase, SpecificRecord
	{
		public static readonly Schema Schema$ = new Schema.Parser().Parse("{\"type\":\"record\",\"name\":\"AvroRecord\",\"namespace\":\"org.apache.hadoop.io.serializer.avro\",\"fields\":[{\"name\":\"intField\",\"type\":\"int\"}]}"
			);

		public static Schema GetClassSchema()
		{
			return Schema$;
		}

		[Obsolete]
		public int intField;

		/// <summary>Default constructor.</summary>
		public AvroRecord()
		{
		}

		/// <summary>All-args constructor.</summary>
		public AvroRecord(int intField)
		{
			this.intField = intField;
		}

		public override Schema GetSchema()
		{
			return Schema$;
		}

		// Used by DatumWriter.  Applications should not call. 
		public override object Get(int field$)
		{
			switch (field$)
			{
				case 0:
				{
					return intField;
				}

				default:
				{
					throw new AvroRuntimeException("Bad index");
				}
			}
		}

		// Used by DatumReader.  Applications should not call. 
		public override void Put(int field$, object value$)
		{
			switch (field$)
			{
				case 0:
				{
					intField = (int)value$;
					break;
				}

				default:
				{
					throw new AvroRuntimeException("Bad index");
				}
			}
		}

		/// <summary>Gets the value of the 'intField' field.</summary>
		public virtual int GetIntField()
		{
			return intField;
		}

		/// <summary>Sets the value of the 'intField' field.</summary>
		/// <param name="value">the value to set.</param>
		public virtual void SetIntField(int value)
		{
			this.intField = value;
		}

		/// <summary>Creates a new AvroRecord RecordBuilder</summary>
		public static AvroRecord.Builder NewBuilder()
		{
			return new AvroRecord.Builder();
		}

		/// <summary>Creates a new AvroRecord RecordBuilder by copying an existing Builder</summary>
		public static AvroRecord.Builder NewBuilder(AvroRecord.Builder other)
		{
			return new AvroRecord.Builder(other);
		}

		/// <summary>Creates a new AvroRecord RecordBuilder by copying an existing AvroRecord instance
		/// 	</summary>
		public static AvroRecord.Builder NewBuilder(Org.Apache.Hadoop.IO.Serializer.Avro.AvroRecord
			 other)
		{
			return new AvroRecord.Builder(other);
		}

		/// <summary>RecordBuilder for AvroRecord instances.</summary>
		public class Builder : SpecificRecordBuilderBase<AvroRecord>, RecordBuilder<AvroRecord
			>
		{
			private int intField;

			/// <summary>Creates a new Builder</summary>
			private Builder()
				: base(AvroRecord.Schema$)
			{
			}

			/// <summary>Creates a Builder by copying an existing Builder</summary>
			private Builder(AvroRecord.Builder other)
				: base(other)
			{
			}

			/// <summary>Creates a Builder by copying an existing AvroRecord instance</summary>
			private Builder(AvroRecord other)
				: base(AvroRecord.Schema$)
			{
				if (IsValidValue(Fields()[0], other.intField))
				{
					this.intField = Data().DeepCopy(Fields()[0].Schema(), other.intField);
					FieldSetFlags()[0] = true;
				}
			}

			/// <summary>Gets the value of the 'intField' field</summary>
			public virtual int GetIntField()
			{
				return intField;
			}

			/// <summary>Sets the value of the 'intField' field</summary>
			public virtual AvroRecord.Builder SetIntField(int value)
			{
				Validate(Fields()[0], value);
				this.intField = value;
				FieldSetFlags()[0] = true;
				return this;
			}

			/// <summary>Checks whether the 'intField' field has been set</summary>
			public virtual bool HasIntField()
			{
				return FieldSetFlags()[0];
			}

			/// <summary>Clears the value of the 'intField' field</summary>
			public virtual AvroRecord.Builder ClearIntField()
			{
				FieldSetFlags()[0] = false;
				return this;
			}

			public override AvroRecord Build()
			{
				try
				{
					AvroRecord record = new AvroRecord();
					record.intField = FieldSetFlags()[0] ? this.intField : (int)DefaultValue(Fields()
						[0]);
					return record;
				}
				catch (Exception e)
				{
					throw new AvroRuntimeException(e);
				}
			}
		}
	}
}
