using Sharpen;

namespace org.apache.hadoop.io.serializer.avro
{
	public class AvroRecord : org.apache.avro.specific.SpecificRecordBase, org.apache.avro.specific.SpecificRecord
	{
		public static readonly org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser
			().parse("{\"type\":\"record\",\"name\":\"AvroRecord\",\"namespace\":\"org.apache.hadoop.io.serializer.avro\",\"fields\":[{\"name\":\"intField\",\"type\":\"int\"}]}"
			);

		public static org.apache.avro.Schema getClassSchema()
		{
			return SCHEMA$;
		}

		[System.Obsolete]
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

		public override org.apache.avro.Schema getSchema()
		{
			return SCHEMA$;
		}

		// Used by DatumWriter.  Applications should not call. 
		public override object get(int field$)
		{
			switch (field$)
			{
				case 0:
				{
					return intField;
				}

				default:
				{
					throw new org.apache.avro.AvroRuntimeException("Bad index");
				}
			}
		}

		// Used by DatumReader.  Applications should not call. 
		public override void put(int field$, object value$)
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
					throw new org.apache.avro.AvroRuntimeException("Bad index");
				}
			}
		}

		/// <summary>Gets the value of the 'intField' field.</summary>
		public virtual int getIntField()
		{
			return intField;
		}

		/// <summary>Sets the value of the 'intField' field.</summary>
		/// <param name="value">the value to set.</param>
		public virtual void setIntField(int value)
		{
			this.intField = value;
		}

		/// <summary>Creates a new AvroRecord RecordBuilder</summary>
		public static org.apache.hadoop.io.serializer.avro.AvroRecord.Builder newBuilder(
			)
		{
			return new org.apache.hadoop.io.serializer.avro.AvroRecord.Builder();
		}

		/// <summary>Creates a new AvroRecord RecordBuilder by copying an existing Builder</summary>
		public static org.apache.hadoop.io.serializer.avro.AvroRecord.Builder newBuilder(
			org.apache.hadoop.io.serializer.avro.AvroRecord.Builder other)
		{
			return new org.apache.hadoop.io.serializer.avro.AvroRecord.Builder(other);
		}

		/// <summary>Creates a new AvroRecord RecordBuilder by copying an existing AvroRecord instance
		/// 	</summary>
		public static org.apache.hadoop.io.serializer.avro.AvroRecord.Builder newBuilder(
			org.apache.hadoop.io.serializer.avro.AvroRecord other)
		{
			return new org.apache.hadoop.io.serializer.avro.AvroRecord.Builder(other);
		}

		/// <summary>RecordBuilder for AvroRecord instances.</summary>
		public class Builder : org.apache.avro.specific.SpecificRecordBuilderBase<org.apache.hadoop.io.serializer.avro.AvroRecord
			>, org.apache.avro.data.RecordBuilder<org.apache.hadoop.io.serializer.avro.AvroRecord
			>
		{
			private int intField;

			/// <summary>Creates a new Builder</summary>
			private Builder()
				: base(org.apache.hadoop.io.serializer.avro.AvroRecord.SCHEMA$)
			{
			}

			/// <summary>Creates a Builder by copying an existing Builder</summary>
			private Builder(org.apache.hadoop.io.serializer.avro.AvroRecord.Builder other)
				: base(other)
			{
			}

			/// <summary>Creates a Builder by copying an existing AvroRecord instance</summary>
			private Builder(org.apache.hadoop.io.serializer.avro.AvroRecord other)
				: base(org.apache.hadoop.io.serializer.avro.AvroRecord.SCHEMA$)
			{
				if (isValidValue(fields()[0], other.intField))
				{
					this.intField = data().deepCopy(fields()[0].schema(), other.intField);
					fieldSetFlags()[0] = true;
				}
			}

			/// <summary>Gets the value of the 'intField' field</summary>
			public virtual int getIntField()
			{
				return intField;
			}

			/// <summary>Sets the value of the 'intField' field</summary>
			public virtual org.apache.hadoop.io.serializer.avro.AvroRecord.Builder setIntField
				(int value)
			{
				validate(fields()[0], value);
				this.intField = value;
				fieldSetFlags()[0] = true;
				return this;
			}

			/// <summary>Checks whether the 'intField' field has been set</summary>
			public virtual bool hasIntField()
			{
				return fieldSetFlags()[0];
			}

			/// <summary>Clears the value of the 'intField' field</summary>
			public virtual org.apache.hadoop.io.serializer.avro.AvroRecord.Builder clearIntField
				()
			{
				fieldSetFlags()[0] = false;
				return this;
			}

			public override org.apache.hadoop.io.serializer.avro.AvroRecord build()
			{
				try
				{
					org.apache.hadoop.io.serializer.avro.AvroRecord record = new org.apache.hadoop.io.serializer.avro.AvroRecord
						();
					record.intField = fieldSetFlags()[0] ? this.intField : (int)defaultValue(fields()
						[0]);
					return record;
				}
				catch (System.Exception e)
				{
					throw new org.apache.avro.AvroRuntimeException(e);
				}
			}
		}
	}
}
