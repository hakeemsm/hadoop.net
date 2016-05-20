using Sharpen;

namespace org.apache.hadoop.record
{
	/// <summary>Abstract class that is extended by generated classes.</summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public abstract class Record : org.apache.hadoop.io.WritableComparable, System.ICloneable
	{
		/// <summary>Serialize a record with tag (ususally field name)</summary>
		/// <param name="rout">Record output destination</param>
		/// <param name="tag">record tag (Used only in tagged serialization e.g. XML)</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void serialize(org.apache.hadoop.record.RecordOutput rout, string
			 tag);

		/// <summary>Deserialize a record with a tag (usually field name)</summary>
		/// <param name="rin">Record input source</param>
		/// <param name="tag">Record tag (Used only in tagged serialization e.g. XML)</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void deserialize(org.apache.hadoop.record.RecordInput rin, string
			 tag);

		// inheric javadoc
		/// <exception cref="System.InvalidCastException"/>
		public abstract int compareTo(object peer);

		/// <summary>Serialize a record without a tag</summary>
		/// <param name="rout">Record output destination</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void serialize(org.apache.hadoop.record.RecordOutput rout)
		{
			this.serialize(rout, string.Empty);
		}

		/// <summary>Deserialize a record without a tag</summary>
		/// <param name="rin">Record input source</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void deserialize(org.apache.hadoop.record.RecordInput rin)
		{
			this.deserialize(rin, string.Empty);
		}

		// inherit javadoc
		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out)
		{
			org.apache.hadoop.record.BinaryRecordOutput bout = org.apache.hadoop.record.BinaryRecordOutput
				.get(@out);
			this.serialize(bout);
		}

		// inherit javadoc
		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput din)
		{
			org.apache.hadoop.record.BinaryRecordInput rin = org.apache.hadoop.record.BinaryRecordInput
				.get(din);
			this.deserialize(rin);
		}

		// inherit javadoc
		public override string ToString()
		{
			try
			{
				java.io.ByteArrayOutputStream s = new java.io.ByteArrayOutputStream();
				org.apache.hadoop.record.CsvRecordOutput a = new org.apache.hadoop.record.CsvRecordOutput
					(s);
				this.serialize(a);
				return Sharpen.Runtime.getStringForBytes(s.toByteArray(), "UTF-8");
			}
			catch (System.Exception ex)
			{
				throw new System.Exception(ex);
			}
		}

		object System.ICloneable.Clone()
		{
			return MemberwiseClone();
		}
	}
}
