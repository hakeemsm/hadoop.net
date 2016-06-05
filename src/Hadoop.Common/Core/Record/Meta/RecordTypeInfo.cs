using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Record;


namespace Org.Apache.Hadoop.Record.Meta
{
	/// <summary>A record's Type Information object which can read/write itself.</summary>
	/// <remarks>
	/// A record's Type Information object which can read/write itself.
	/// Type information for a record comprises metadata about the record,
	/// as well as a collection of type information for each field in the record.
	/// </remarks>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class RecordTypeInfo : Org.Apache.Hadoop.Record.Record
	{
		private string name;

		internal StructTypeID sTid;

		/// <summary>Create an empty RecordTypeInfo object.</summary>
		public RecordTypeInfo()
		{
			// A RecordTypeInfo is really just a wrapper around StructTypeID
			// A RecordTypeInfo object is just a collection of TypeInfo objects for each of its fields.  
			//private ArrayList<FieldTypeInfo> typeInfos = new ArrayList<FieldTypeInfo>();
			// we keep a hashmap of struct/record names and their type information, as we need it to 
			// set filters when reading nested structs. This map is used during deserialization.
			//private Map<String, RecordTypeInfo> structRTIs = new HashMap<String, RecordTypeInfo>();
			sTid = new StructTypeID();
		}

		/// <summary>Create a RecordTypeInfo object representing a record with the given name
		/// 	</summary>
		/// <param name="name">Name of the record</param>
		public RecordTypeInfo(string name)
		{
			this.name = name;
			sTid = new StructTypeID();
		}

		private RecordTypeInfo(string name, StructTypeID stid)
		{
			/*
			* private constructor
			*/
			this.sTid = stid;
			this.name = name;
		}

		/// <summary>return the name of the record</summary>
		public virtual string GetName()
		{
			return name;
		}

		/// <summary>set the name of the record</summary>
		public virtual void SetName(string name)
		{
			this.name = name;
		}

		/// <summary>Add a field.</summary>
		/// <param name="fieldName">Name of the field</param>
		/// <param name="tid">Type ID of the field</param>
		public virtual void AddField(string fieldName, TypeID tid)
		{
			sTid.GetFieldTypeInfos().AddItem(new FieldTypeInfo(fieldName, tid));
		}

		private void AddAll(ICollection<FieldTypeInfo> tis)
		{
			Collections.AddAll(sTid.GetFieldTypeInfos(), tis);
		}

		/// <summary>Return a collection of field type infos</summary>
		public virtual ICollection<FieldTypeInfo> GetFieldTypeInfos()
		{
			return sTid.GetFieldTypeInfos();
		}

		/// <summary>Return the type info of a nested record.</summary>
		/// <remarks>
		/// Return the type info of a nested record. We only consider nesting
		/// to one level.
		/// </remarks>
		/// <param name="name">Name of the nested record</param>
		public virtual Org.Apache.Hadoop.Record.Meta.RecordTypeInfo GetNestedStructTypeInfo
			(string name)
		{
			StructTypeID stid = sTid.FindStruct(name);
			if (null == stid)
			{
				return null;
			}
			return new Org.Apache.Hadoop.Record.Meta.RecordTypeInfo(name, stid);
		}

		/// <summary>Serialize the type information for a record</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void Serialize(RecordOutput rout, string tag)
		{
			// write out any header, version info, here
			rout.StartRecord(this, tag);
			rout.WriteString(name, tag);
			sTid.WriteRest(rout, tag);
			rout.EndRecord(this, tag);
		}

		/// <summary>Deserialize the type information for a record</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void Deserialize(RecordInput rin, string tag)
		{
			// read in any header, version info 
			rin.StartRecord(tag);
			// name
			this.name = rin.ReadString(tag);
			sTid.Read(rin, tag);
			rin.EndRecord(tag);
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
		public override int CompareTo(object peer_)
		{
			if (!(peer_ is Org.Apache.Hadoop.Record.Meta.RecordTypeInfo))
			{
				throw new InvalidCastException("Comparing different types of records.");
			}
			throw new NotSupportedException("compareTo() is not supported");
		}
	}
}
