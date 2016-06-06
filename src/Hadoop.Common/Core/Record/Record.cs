using System;
using System.IO;
using Org.Apache.Hadoop.IO;


namespace Org.Apache.Hadoop.Record
{
	/// <summary>Abstract class that is extended by generated classes.</summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public abstract class Record : WritableComparable, ICloneable
	{
		/// <summary>Serialize a record with tag (ususally field name)</summary>
		/// <param name="rout">Record output destination</param>
		/// <param name="tag">record tag (Used only in tagged serialization e.g. XML)</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void Serialize(RecordOutput rout, string tag);

		/// <summary>Deserialize a record with a tag (usually field name)</summary>
		/// <param name="rin">Record input source</param>
		/// <param name="tag">Record tag (Used only in tagged serialization e.g. XML)</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void Deserialize(RecordInput rin, string tag);

		// inheric javadoc
		/// <exception cref="System.InvalidCastException"/>
		public abstract int CompareTo(object peer);

		/// <summary>Serialize a record without a tag</summary>
		/// <param name="rout">Record output destination</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Serialize(RecordOutput rout)
		{
			this.Serialize(rout, string.Empty);
		}

		/// <summary>Deserialize a record without a tag</summary>
		/// <param name="rin">Record input source</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Deserialize(RecordInput rin)
		{
			this.Deserialize(rin, string.Empty);
		}

		// inherit javadoc
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(BinaryWriter writer)
		{
			BinaryRecordOutput bout = BinaryRecordOutput.Get(@out);
			this.Serialize(bout);
		}

		// inherit javadoc
		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader din)
		{
			BinaryRecordInput rin = BinaryRecordInput.Get(din);
			this.Deserialize(rin);
		}

		// inherit javadoc
		public override string ToString()
		{
			try
			{
				ByteArrayOutputStream s = new ByteArrayOutputStream();
				CsvRecordOutput a = new CsvRecordOutput(s);
				this.Serialize(a);
				return Runtime.GetStringForBytes(s.ToByteArray(), "UTF-8");
			}
			catch (Exception ex)
			{
				throw new RuntimeException(ex);
			}
		}
	}
}
