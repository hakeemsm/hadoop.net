using Sharpen;

namespace org.apache.hadoop.record.meta
{
	/// <summary>A record's Type Information object which can read/write itself.</summary>
	/// <remarks>
	/// A record's Type Information object which can read/write itself.
	/// Type information for a record comprises metadata about the record,
	/// as well as a collection of type information for each field in the record.
	/// </remarks>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class RecordTypeInfo : org.apache.hadoop.record.Record
	{
		private string name;

		internal org.apache.hadoop.record.meta.StructTypeID sTid;

		/// <summary>Create an empty RecordTypeInfo object.</summary>
		public RecordTypeInfo()
		{
			// A RecordTypeInfo is really just a wrapper around StructTypeID
			// A RecordTypeInfo object is just a collection of TypeInfo objects for each of its fields.  
			//private ArrayList<FieldTypeInfo> typeInfos = new ArrayList<FieldTypeInfo>();
			// we keep a hashmap of struct/record names and their type information, as we need it to 
			// set filters when reading nested structs. This map is used during deserialization.
			//private Map<String, RecordTypeInfo> structRTIs = new HashMap<String, RecordTypeInfo>();
			sTid = new org.apache.hadoop.record.meta.StructTypeID();
		}

		/// <summary>Create a RecordTypeInfo object representing a record with the given name
		/// 	</summary>
		/// <param name="name">Name of the record</param>
		public RecordTypeInfo(string name)
		{
			this.name = name;
			sTid = new org.apache.hadoop.record.meta.StructTypeID();
		}

		private RecordTypeInfo(string name, org.apache.hadoop.record.meta.StructTypeID stid
			)
		{
			/*
			* private constructor
			*/
			this.sTid = stid;
			this.name = name;
		}

		/// <summary>return the name of the record</summary>
		public virtual string getName()
		{
			return name;
		}

		/// <summary>set the name of the record</summary>
		public virtual void setName(string name)
		{
			this.name = name;
		}

		/// <summary>Add a field.</summary>
		/// <param name="fieldName">Name of the field</param>
		/// <param name="tid">Type ID of the field</param>
		public virtual void addField(string fieldName, org.apache.hadoop.record.meta.TypeID
			 tid)
		{
			sTid.getFieldTypeInfos().add(new org.apache.hadoop.record.meta.FieldTypeInfo(fieldName
				, tid));
		}

		private void addAll(System.Collections.Generic.ICollection<org.apache.hadoop.record.meta.FieldTypeInfo
			> tis)
		{
			Sharpen.Collections.AddAll(sTid.getFieldTypeInfos(), tis);
		}

		/// <summary>Return a collection of field type infos</summary>
		public virtual System.Collections.Generic.ICollection<org.apache.hadoop.record.meta.FieldTypeInfo
			> getFieldTypeInfos()
		{
			return sTid.getFieldTypeInfos();
		}

		/// <summary>Return the type info of a nested record.</summary>
		/// <remarks>
		/// Return the type info of a nested record. We only consider nesting
		/// to one level.
		/// </remarks>
		/// <param name="name">Name of the nested record</param>
		public virtual org.apache.hadoop.record.meta.RecordTypeInfo getNestedStructTypeInfo
			(string name)
		{
			org.apache.hadoop.record.meta.StructTypeID stid = sTid.findStruct(name);
			if (null == stid)
			{
				return null;
			}
			return new org.apache.hadoop.record.meta.RecordTypeInfo(name, stid);
		}

		/// <summary>Serialize the type information for a record</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void serialize(org.apache.hadoop.record.RecordOutput rout, string
			 tag)
		{
			// write out any header, version info, here
			rout.startRecord(this, tag);
			rout.writeString(name, tag);
			sTid.writeRest(rout, tag);
			rout.endRecord(this, tag);
		}

		/// <summary>Deserialize the type information for a record</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void deserialize(org.apache.hadoop.record.RecordInput rin, string
			 tag)
		{
			// read in any header, version info 
			rin.startRecord(tag);
			// name
			this.name = rin.readString(tag);
			sTid.read(rin, tag);
			rin.endRecord(tag);
		}

		/// <summary>
		/// This class doesn't implement Comparable as it's not meant to be used
		/// for anything besides de/serializing.
		/// </summary>
		/// <remarks>
		/// This class doesn't implement Comparable as it's not meant to be used
		/// for anything besides de/serializing.
		/// So we always throw an exception.
		/// Not implemented. Always returns 0 if another RecordTypeInfo is passed in.
		/// </remarks>
		/// <exception cref="System.InvalidCastException"/>
		public override int compareTo(object peer_)
		{
			if (!(peer_ is org.apache.hadoop.record.meta.RecordTypeInfo))
			{
				throw new System.InvalidCastException("Comparing different types of records.");
			}
			throw new System.NotSupportedException("compareTo() is not supported");
		}
	}
}
